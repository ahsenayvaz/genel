# --- Imports -----------------------------------------------------------
from dagster import asset, AssetExecutionContext, AssetIn, MetadataValue  # Dagster asset decorator, execution context, and metadata helpers
import os  # Access environment variables and build filesystem paths
import psycopg2  # PostgreSQL driver used to connect and run SQL statements
import csv  # Read the comma-separated source file with csv.DictReader
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
csv_file_cas = "insert_substance_quoted.csv"   #

from csv_to_dgh.CAS.cas_utils import (
    clean_html,
    split_names,
    normalize_name,
    extract_names,
)

# Substances imported here often have no official BfArM ASK number
# (found_ask is empty in the source CSV). ask_nummer is nullable and
# UNIQUE, so we *could* just write NULL there - but that breaks the
# relational link to this substance's name rows: once more than one
# substance has ask_nummer = NULL, a JOIN on names.ask_nummer =
# substances.ask_nummer can never match them back to the *correct* one
# (NULL never equals NULL in SQL), so the name rows become effectively
# unlinkable/orphaned.
#
# To keep every substance individually addressable and its names properly
# joinable, we instead mint a synthetic, negative ask_nummer for rows
# without a found_ask. Real BfArM ASK numbers are always positive, so
# negative numbers can never collide with them. The next free value is
# looked up once per run from the current minimum negative ask_nummer
# already in the table. On reruns, an existing synthetic ASK is reused
# when the normalized DWH ingredient name is already present in the name table.
select_min_synthetic_ask = f"""
    SELECT MIN(ask_nummer)
    FROM {mapping_table_substances}
    WHERE ask_nummer < 0
"""
select_existing_synthetic_ask = f"""
    SELECT ask_nummer
    FROM {mapping_table_names}
    WHERE ask_nummer < 0
      AND name_normalized = %s
    LIMIT 1
"""
insert_substance = f"""
    INSERT INTO {mapping_table_substances} (
        ask_nummer,
        cas_code,
        cas_display,
        ocas_code_primary,
        ocas_code_list,
        cas_code_source,
        ocas_code_source
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (ask_nummer) DO UPDATE SET
        cas_code = EXCLUDED.cas_code,
        cas_display = EXCLUDED.cas_display,
        ocas_code_primary = EXCLUDED.ocas_code_primary,
        ocas_code_list = EXCLUDED.ocas_code_list,
        cas_code_source = EXCLUDED.cas_code_source,
        ocas_code_source = EXCLUDED.ocas_code_source,
        datensatz_geaendert = now();
"""

insert_name = f"""
    INSERT INTO {mapping_table_names} (
        ask_nummer,
        name,
        name_normalized,
        is_display,
        priority,
        source
    )
    SELECT
        %s, %s, %s, %s, %s, %s
    WHERE NOT EXISTS (
        SELECT 1
        FROM {mapping_table_names}
        WHERE ask_nummer = %s
          AND name_normalized = %s
    );
"""

# ----------------------------------------------------------------------------------------------------------------------
# 🚀 DAGSTER ASSET -----------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------

@asset(
    key="medication_ingredient_new_substance_import",
    description=(
            f"Imports wholly new substances from '{csv_file_cas}' into "
            f"'{mapping_table_substances}' and '{mapping_table_names}'.\n\n"
            f"Only rows with action_import == 'add_new_substance' are processed. "
            f"Rows that already carry a found_ask (e.g. legacy ASK numbers not "
            f"present in the current BEZVO source) reuse that ASK number. Rows "
            f"without a found_ask are assigned a synthetic, negative ask_nummer "
            f"(instead of NULL) so each new substance stays individually "
            f"addressable and its name rows stay reliably joinable via "
            f"ask_nummer - a NULL key would break that link as soon as more "
            f"than one substance lacked an ASK number.\n\n"
            f"Re-running this asset reuses existing synthetic ASK numbers "
            f"based on the normalized DWH ingredient name. Substance rows are "
            f"upserted (ON CONFLICT DO UPDATE) by ask_nummer. Name rows are "
            f"inserted only when the same ask_nummer and normalized name do not "
            f"already exist.\n\n"
            f"ATC data from the source CSV (atc_code/atc_display) is not "
            f"persisted - neither target table has columns for it."
    ),
    deps=["medication_ingredient_name_addition"]
)

def medication_ingredient_new_substance_import(context: AssetExecutionContext) -> None:
    """

    """

    context.log.info(
        f"***Configuration***\n"
        f"Writing into {db_name}: {mapping_table_substances}, {mapping_table_names}."
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
        # Startwert für synthetische ASK-Nummern ermitteln
        # ==============================================================

        cursor.execute(select_min_synthetic_ask)
        (current_min_synthetic,) = cursor.fetchone()

        # Next free synthetic ask_nummer to hand out (counts down from -1,
        # or continues below the lowest one already in use).
        next_synthetic_ask = (
            current_min_synthetic - 1 if current_min_synthetic is not None else -1
        )

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

                action_import = row.get("action_import") or ""

                if action_import != "add_new_substance":
                    continue

                dwh_wirkstoff = row.get("dwh_wirkstoff") or None
                found_ask = row.get("found_ask") or None
                found_cas = row.get("found_cas") or None
                found_ocas = row.get("found_ocas") or None
                cas_display = row.get("cas_display") or None
                cas_source = row.get("cas_source") or None
                ocas_source = row.get("ocas_source") or None

                if not dwh_wirkstoff:
                    context.log.warning(
                        "Row has action_import == 'add_new_substance' but "
                        "dwh_wirkstoff is empty. Skipping."
                    )
                    continue

                # ----------------------------------------------------------
                # ASK-Nummer bestimmen
                # ----------------------------------------------------------

                if found_ask:
                    # Reuse an existing (e.g. legacy) ASK number given in the CSV.
                    ask = int(found_ask)

                else:
                    # Reuse an already existing synthetic ASK on reruns.
                    dwh_name_normalized = normalize_name(dwh_wirkstoff)

                    cursor.execute(
                        select_existing_synthetic_ask,
                        (dwh_name_normalized,)
                    )

                    existing_row = cursor.fetchone()

                    if existing_row:
                        ask = existing_row[0]
                    else:
                        ask = next_synthetic_ask
                        next_synthetic_ask -= 1

                # ----------------------------------------------------------
                # OCAS-Liste aufbauen (aktuell max. ein Wert aus found_ocas)
                # ----------------------------------------------------------

                ocas_code_primary = found_ocas
                ocas_code_list = found_ocas

                # ----------------------------------------------------------
                # Substanz-Zeile schreiben
                # ----------------------------------------------------------

                context.log.info(
                    f"Adding new substance '{dwh_wirkstoff}' as ASK {ask}"
                )

                cursor.execute(
                    insert_substance,
                    (
                        ask,
                        found_cas,
                        cas_display,
                        ocas_code_primary,
                        ocas_code_list,
                        cas_source,
                        ocas_source,
                    )
                )

                # ----------------------------------------------------------
                # Namens-Zeilen schreiben
                # ----------------------------------------------------------

                # cas_display is the preferred/canonical name -> priority 1.
                if cas_display:
                    cursor.execute(
                        insert_name,
                        (
                            ask,
                            cas_display,
                            normalize_name(cas_display),
                            True,
                            1,
                            cas_source or ocas_source or "DWH",
                            ask,
                            normalize_name(cas_display),
                        )
                    )

                # The DWH ingredient name is added as a lower-priority
                # synonym (priority 4), unless it's identical to cas_display
                # (avoid inserting the same name twice).
                if dwh_wirkstoff != cas_display:
                    cursor.execute(
                        insert_name,
                        (
                            ask,
                            dwh_wirkstoff,
                            normalize_name(dwh_wirkstoff),
                            False,
                            4,
                            "DWH",
                            ask,
                            normalize_name(dwh_wirkstoff),
                        )
                    )

        # --------------------------------------------------------------------------------------------------------------

        conn.commit()
        context.log.info(
            "New substance import completed successfully."
        )

    # ------------------------------------------------------------------------------------------------------------------
    except Exception as e:
        # Log the error for visibility in the Dagster UI/logs, then re-raise so Dagster correctly marks this asset run as failed.
        context.log.error(f"An error occurred during new substance import: {e}")
        raise e

    finally:
        # Always clean up the database connection, whether the run succeeded or failed.
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    return
