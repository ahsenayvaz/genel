# --- Imports -----------------------------------------------------------
from dagster import asset, AssetExecutionContext, AssetIn, MetadataValue  # Dagster asset decorator, execution context, and metadata helpers
import os  # Access environment variables and build filesystem paths
import psycopg2  # PostgreSQL driver used to connect and run SQL statements
import csv  # Read the semicolon-separated source file with csv.DictReader
from datetime import datetime  # Used to capture "today" (currently only stored, not used further)
from dotenv import load_dotenv  # Load DB credentials from .env files into environment variables
from pathlib import Path  # Build the CSV file path in an OS-independent way

# Capture the run date. Currently not used elsewhere in the asset, but kept in case a "last processed" timestamp is
# needed later.
today = datetime.now()

# --- Load environment / configuration -----------------------------------
# Two .env files are loaded: a shared/global one and a local override.
# override=True means values from settings_local.env take precedence over
# settings_global.env when the same variable is defined in both.
load_dotenv(dotenv_path="settings_global.env", override=True)
load_dotenv(dotenv_path="settings_local.env", override=True)

# Database connection settings for the "dgh_central" PostgreSQL database.
# All values come from environment variables so no credentials are hardcoded.
db_host = os.getenv("DGH_CENTRAL_PQ_HOST")      # Hostname or IP of the Postgres server
db_port = os.getenv("DGH_CENTRAL_PQ_PORT")      # Port Postgres is listening on
db_name = os.getenv("DGH_CENTRAL_PQ_NAME")      # Name of the target database
db_user = os.getenv("DGH_CENTRAL_PQ_USER")      # Username for the connection
db_password = os.getenv("DGH_CENTRAL_PQ_PASSWORD")  # Password for the connection

# Names of the two target tables this asset writes into.
mapping_table_substances = "medication_cas_substances_alpha"
mapping_table_names = "medication_cas_names_alpha"

# Location of the source CSV file, relative to this script's own directory.
current_directory = os.path.dirname(__file__)  # Directory this .py file lives in
csv_directory_cas = "../misc/cas"              # Subfolder containing the source CSVs
csv_file_cas = "add_cas_use_ocas_quoted.csv"   #

from csv_to_dgh.CAS.cas_utils import (
    clean_html,
    split_names,
    normalize_name,
    extract_names,
)

select_substance = f"""
            SELECT
                cas_code,
                cas_display,
                ocas_code_primary,
                ocas_code_list,
                cas_code_source,
                ocas_code_source
            FROM {mapping_table_substances}
            WHERE ask_nummer = %s
        """

update_substance = f"""
            UPDATE {mapping_table_substances}
            SET
                cas_code = %s,
                cas_display = %s,
                ocas_code_primary = %s,
                ocas_code_list = %s,
                cas_code_source = %s,
                ocas_code_source = %s,
                datensatz_geaendert = now()
            WHERE ask_nummer = %s
        """

insert_name = f"""
    INSERT INTO {mapping_table_names} (
        ask_nummer,
        name,
        name_normalized,
        is_display,
        priority,
        source,
        datensatz_geaendert
    )
    SELECT
        %s, %s, %s, %s, %s, %s, now()
    WHERE NOT EXISTS (
        SELECT 1
        FROM {mapping_table_names}
        WHERE ask_nummer = %s
          AND name_normalized = %s
    )
"""

# ----------------------------------------------------------------------------------------------------------------------
# 🚀 DAGSTER ASSET -----------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------

@asset(
    key="medication_ingredient_cas_code_addition",
    description=f"Imports additional CAS and name mappings from '{csv_file_cas}' into '{mapping_table_substances}'.",
    deps=["medication_ingredient_cas_import_bezvo_csv"]
)

def medication_ingredient_cas_code_addition(context: AssetExecutionContext) -> None:
    """

    """

    context.log.info(
        f"***Configuration***\n"
        f"Writing into {db_name}: {mapping_table_substances}."
    )


    #...................................................................................................................
    # Build the full path to the source CSV file and fail fast if it's missing.
    csv_path = Path(current_directory) / csv_directory_cas / csv_file_cas
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    # Connect to the PostgreSQL database using credentials loaded from .env files.
    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        user=db_user,
        password=db_password,
        database=db_name
    )
    # Create a cursor object to interact with the database (execute SQL, fetch results).
    cursor = conn.cursor()

    # ------------------------------------------------------------------------------------------------------------------
    try:

        # ==============================================================
        # CSV einlesen
        # ==============================================================

        with csv_path.open(
                newline="",
                encoding="utf-8"
        ) as csvfile:

            reader = csv.DictReader(
                csvfile,
                delimiter=",",
                quotechar='"'
            )

            for row in reader:

                action_import = (
                    row.get("action_import") or ""
                ).strip()

                action_import_2 = (
                    row.get("action_import_2") or ""
                ).strip()

                ask = int(row["found_ask"])

                found_cas = row.get("found_cas") or None
                found_ocas = row.get("found_ocas") or None
                cas_display =row.get("cas_display") or None
                cas_source = row.get("cas_source") or None
                ocas_source = row.get("ocas_source") or None
                dwh_wirkstoff = (row.get("dwh_wirkstoff") or "").strip() or None

                # ------------------------------------------------------
                # Vorhandenen Datensatz laden
                # ------------------------------------------------------

                cursor.execute(select_substance, (ask,))
                existing = cursor.fetchone()

                if existing is None:
                    context.log.warning(
                        f"ASK {ask} not found in "
                        f"{mapping_table_substances}. Skipping row."
                    )
                    continue

                (
                    current_cas,
                    current_cas_display,
                    current_ocas_primary,
                    current_ocas_list,
                    current_cas_source,
                    current_ocas_source,
                ) = existing

                # ==============================================================
                # CAS-Code hinzufügen
                # ==============================================================

                if action_import == "add_cas_code":

                    cursor.execute(
                        update_substance,
                        (
                            found_cas,
                            cas_display,
                            current_ocas_primary,
                            current_ocas_list,
                            cas_source,
                            current_ocas_source,
                            ask,
                        )
                    )

                # ==============================================================
                # OCAS-Code hinzufügen
                # ==============================================================

                elif action_import == "add_ocas_code":

                    if not found_ocas:
                        context.log.warning(
                            f"ASK {ask}: action_import is add_ocas_code, "
                            f"but found_ocas_code is empty. Skipping."
                        )
                        continue

                    # ----------------------------------------------------------
                    # Bestehende OCAS-Liste aufbauen
                    # ----------------------------------------------------------

                    existing_ocas = []

                    if current_ocas_list:
                        existing_ocas = [
                            x.strip()
                            for x in current_ocas_list.split("|")
                            if x.strip()
                        ]

                    # ----------------------------------------------------------
                    # Primary OCAS muss immer auch in der Liste enthalten sein
                    # ----------------------------------------------------------

                    if (
                            current_ocas_primary
                            and current_ocas_primary not in existing_ocas
                    ):
                        existing_ocas.insert(0, current_ocas_primary)

                    # ----------------------------------------------------------
                    # Neuen OCAS nur ergänzen, wenn noch nicht vorhanden
                    # ----------------------------------------------------------

                    if found_ocas not in existing_ocas:
                        existing_ocas.append(found_ocas)

                    new_ocas_list = "|".join(existing_ocas)

                    # ----------------------------------------------------------
                    # Primary OCAS bestimmen
                    # ----------------------------------------------------------

                    if current_ocas_primary:
                        new_ocas_primary = current_ocas_primary
                    else:
                        new_ocas_primary = found_ocas

                    # ----------------------------------------------------------
                    # OCAS-Quellen zusammenführen
                    # ----------------------------------------------------------

                    existing_sources = []

                    if current_ocas_source:
                        existing_sources = [
                            x.strip()
                            for x in current_ocas_source.split("|")
                            if x.strip()
                        ]

                    if (
                            ocas_source
                            and ocas_source not in existing_sources
                    ):
                        existing_sources.append(ocas_source)

                    new_ocas_source = (
                        "|".join(existing_sources)
                        if existing_sources
                        else None
                    )

                    # ----------------------------------------------------------
                    # Update
                    # ----------------------------------------------------------

                    cursor.execute(
                        update_substance,
                        (
                            current_cas,
                            current_cas_display,
                            new_ocas_primary,
                            new_ocas_list,
                            current_cas_source,
                            new_ocas_source,
                            ask,
                        )
                    )
                # ==============================================================
                # Zusätzlichen Namen hinzufügen
                # ==============================================================

                if action_import_2 == "add_cas_name":

                    if not dwh_wirkstoff:
                        context.log.warning(
                            f"ASK {ask}: action_import_2 is add_cas_name, "
                            f"but dwh_wirkstoff is empty. Skipping."
                        )
                    else:
                        name_normalized = normalize_name(dwh_wirkstoff)
                        cas_name_source = "DWH"

                        cursor.execute(
                            insert_name,
                            (
                                ask,
                                dwh_wirkstoff,
                                name_normalized,
                                False,
                                4,
                                cas_name_source,
                                ask,
                                name_normalized,
                            )
                        )

                        context.log.info(
                            f"Added additional name "
                            f"'{dwh_wirkstoff}' for ASK {ask}."
                        )

        # --------------------------------------------------------------------------------------------------------------

        conn.commit()
        context.log.info(
            "CAS enrichment completed successfully."
        )

    # ------------------------------------------------------------------------------------------------------------------
    except Exception as e:
        # Log the error for visibility in the Dagster UI/logs, then re-raise so Dagster correctly marks this asset run as failed.
        context.log.error(f"An error occurred during CAS enrichment: {e}")
        raise e

    finally:
        # Always clean up the database connection, whether the run succeeded or failed.
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    return
