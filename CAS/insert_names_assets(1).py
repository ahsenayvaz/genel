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
csv_file_cas = "insert_name_quoted.csv"   #

from csv_to_dgh.CAS.cas_utils import (
    clean_html,
    split_names,
    normalize_name,
    extract_names,
)

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
    key="medication_ingredient_name_addition",
    description=f"Imports additional ingredient names which are used in DWH (synonyms) from '{csv_file_cas}' into '{mapping_table_names}'.",
    deps=["medication_ingredient_cas_code_addition"]
)

def medication_ingredient_name_addition(context: AssetExecutionContext) -> None:
    """

    """

    context.log.info(
        f"***Configuration***\n"
        f"Writing into {db_name}: {mapping_table_names}."
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

                if row.get("action_import") != "add_cas_name":
                    continue

                ask = int(row["found_ask"])

                dwh_wirkstoff = row.get("dwh_wirkstoff") or None
                cas_name_source = "DWH"

                if not dwh_wirkstoff:
                    context.log.warning(
                        f"ASK {ask}: dwh_wirkstoff is empty. Skipping row."
                    )
                    continue

                name_normalized = normalize_name(dwh_wirkstoff)

                context.log.info(
                    f"Added name '{dwh_wirkstoff}' for ASK {ask}"
                )

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

        # --------------------------------------------------------------------------------------------------------------

        conn.commit()

        context.log.info(
            "Name enrichment completed successfully."
        )

    # ------------------------------------------------------------------------------------------------------------------
    except Exception as e:
        # Log the error for visibility in the Dagster UI/logs, then re-raise so Dagster correctly marks this asset run as failed.
        context.log.error(f"An error occurred during Name enrichment: {e}")
        raise e

    finally:
        # Always clean up the database connection, whether the run succeeded or failed.
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    return
