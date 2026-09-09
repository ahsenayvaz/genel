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
csv_file_cas = "bezvo.csv"                     # The BfArM BEZVO source file name

from csv_to_dgh.CAS.cas_utils import (
    clean_html,
    split_names,
    normalize_name,
    extract_names,
)

# Todo: is_primary is deprecated

# ----------------------------------------------------------------------------------------------------------------------
# 🚀 DAGSTER ASSET -----------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------

@asset(
    key="medication_ingredient_cas_import_bezvo_csv",
    description=(
            f"Imports BfArM substance/CAS-code mapping data from the source file "
            f"'{csv_file_cas}' (semicolon-separated, one row per BfArM ASK code) "
            f"into the PostgreSQL database '{db_name}'.\n\n"
            f"Writes into two tables:\n"
            f"- '{mapping_table_substances}': one row per substance, keyed by ASK code, "
            f"holding the main CAS registry number, preferred display name, molecular "
            f"weight, and any alternative CAS numbers (OCAS).\n"
            f"- '{mapping_table_names}': one row per name/synonym variant of a substance "
            f"(from HBEZ1-3, SYN*, and overflow columns), linked back to its substance "
            f"via the ASK code, with a normalized name for matching and a priority "
            f"ranking (display name > base name > salt-form name).\n\n"
            f"All rows are tagged with source = 'BfArM BEZVO' to distinguish them from "
            f"data added later from other sources. Re-running this asset is safe: "
            f"substance rows are upserted (ON CONFLICT DO UPDATE) by ASK code."
    )
)

def medication_ingredient_cas_import_bezvo_csv(context: AssetExecutionContext) -> None:
    """
    Reads the BfArM "bezvo.csv" source file and upserts its content into
    two PostgreSQL tables:
      - medication_cas_substances: one row per substance (keyed by ASK code)
      - medication_cas_names: one row per name/synonym, linked back to its substance

    All rows written by this asset are tagged with source = "BfArM BEZVO"
    so that later, when additional sources are merged in, it stays clear
    which source each row originated from.
    """

    context.log.info(
        f"***Configuration***\n"
        f"Writing into {db_name}: {mapping_table_substances}, {mapping_table_names}."
    )

    #...................................................................................................................
    # Define SQL query to create database tables
    # (CREATE TABLE IF NOT EXISTS: safe to run on every asset execution; does nothing if the tables already exist.)

    # (1) Main table: One dataset = one substance (CAS level), keyed by the BfArM ASK number.
    create_substances = f"""
    CREATE TABLE IF NOT EXISTS {mapping_table_substances} (
        id SERIAL PRIMARY KEY,              -- auto-incrementing internal row id
        ask_nummer INTEGER UNIQUE,          -- unique BfArM ASK number identifying the substance
        cas_code TEXT,                      -- primary/main CAS registry number (from the "CAS" column)
        cas_display TEXT,                   -- preferred display name for this substance
        molecular_weight NUMERIC,           -- molecular weight (from the "MOL" column)

        ocas_code_primary TEXT,             -- first alternative CAS listed in OCAS
        ocas_code_list TEXT,                -- all alternative CAS from OCAS, "|"-separated (raw list)

        cas_code_source TEXT,                        -- origin of CAS, e.g. "BfArM BEZVO"
        ocas_code_source TEXT,                       -- origin of OCAS, e.g. "BfArM BEZVO"

        datensatz_geaendert TIMESTAMP DEFAULT now()  -- timestamp of last insert/update
    );
    """

    # (2) Name table: One dataset = one name variant, linked to its substance via a foreign key on the ASK number.
    create_names = f"""
    CREATE TABLE IF NOT EXISTS {mapping_table_names} (
        id SERIAL PRIMARY KEY,                                   -- auto-incrementing internal row id
        ask_nummer INTEGER REFERENCES {mapping_table_substances}(ask_nummer),  -- FK to the substance's ASK number

        name TEXT,                          -- original (unmodified) name as found in the source
        name_normalized TEXT,               -- normalized version of the name

        is_display BOOLEAN,                 -- TRUE if this is the substance's main/display name
        priority INTEGER,                   -- ranking used for matching/preference (lower = better)

        source TEXT,                        -- origin of this row, e.g. "BfArM BEZVO"

        datensatz_geaendert TIMESTAMP DEFAULT now()  -- timestamp of last insert/update
    );
    """

    #...................................................................................................................
    # Define query for batch insert
    # Both statements use "ON CONFLICT" (upsert) where appropriate, so the asset can safely be re-run without creating
    # duplicate rows.

    # Upsert for the substances table: if a row with the same ASK number already exists, all its columns are refreshed
    # with the latest values from this run instead of inserting a duplicate row.
    insert_substances = f"""
    INSERT INTO {mapping_table_substances} (
        ask_nummer, cas_code, cas_display, molecular_weight,
        ocas_code_primary, ocas_code_list, cas_code_source, ocas_code_source
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (ask_nummer) DO UPDATE SET
        cas_code = EXCLUDED.cas_code,
        cas_display = EXCLUDED.cas_display,
        molecular_weight = EXCLUDED.molecular_weight,
        ocas_code_primary = EXCLUDED.ocas_code_primary,
        ocas_code_list = EXCLUDED.ocas_code_list,
        cas_code_source = EXCLUDED.cas_code_source,
        ocas_code_source = EXCLUDED.ocas_code_source,
        datensatz_geaendert = now();
    """

    # Plain insert for the names table (no ON CONFLICT / upsert logic here: each run currently appends new name rows
    # without checking for existing duplicates).
    insert_names = f"""
    INSERT INTO {mapping_table_names} (
        ask_nummer, name, name_normalized, is_display, priority, source
    )
    VALUES (%s, %s, %s, %s, %s, %s);
    """

    #...................................................................................................................
    # Transfer data from csv to PostgreSQL

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
        # Make sure both target tables exist before we try to insert anything.
        cursor.execute(create_substances)
        cursor.execute(create_names)
        conn.commit()

        # Accumulate all rows to insert here, then write them to the
        # database in two batched executemany() calls (faster than one
        # INSERT per row).
        substances_rows = []
        names_rows = []

        # Read CSV
        with csv_path.open(newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(
                csvfile,
                delimiter=";",       # The source file is semicolon-separated, not comma-separated
                restkey="EXTRA",     # Any extra columns beyond the header get collected into row["EXTRA"] as a list
            )

            for row in reader:
                # --- Basic data -------------------------------------------------
                ask = int(row.get("ASK"))                           # Unique BfArM ASK nummer (primary key)
                cas = row.get("CAS") or None                        # Main/primary CAS
                ocas = split_names(row.get("OCAS"))                 # Alternative CAS, parsed into a list
                ocas_code_primary = ocas[0] if ocas else None       # First alternative CAS, if any
                ocas_code_list = "|".join(ocas) if ocas else None   # All alternative CAS, rejoined with "|"

                # --- Names --------------------------------------------------------
                # Collect every usable name/synonym for this substance from the
                # various name-related columns (HBEZ1-3, SYN*, EXTRA).
                names = extract_names(context, row)

                # --- Determine the preferred display name -------------------------
                # Try HBEZ1 first, then HBEZ3, then HBEZ2 (in that priority order),
                # and use the first one that actually has a value.
                cas_display = None
                for field in ["HBEZ1", "HBEZ3", "HBEZ2"]:
                    val = clean_html(row.get(field))
                    if val and val != "-":
                        cas_display = val
                        break

                # --- Molecular weight ------------------------------------------------
                # Convert the "MOL" column to a float, or leave it as None if empty.
                mol = row.get("MOL")
                mol = float(mol) if mol else None

                # --- Fill main (substances) table ------------------------------------
                substances_rows.append((
                    ask,                          # ask_nummer: primary key
                    cas,                          # cas_code: main CAS
                    cas_display,                  # cas_display: preferred display name
                    mol,                          # molecular_weight
                    ocas_code_primary,            # ocas_code_primary: first alternative CAS
                    ocas_code_list,               # ocas_code_list: all alternative CAS, "|"-joined
                    "BfArM BEZVO",                # cas_code_source: fixed value identifying CAS origin
                    "BfArM BEZVO"                 # ocas_code_source: fixed value identifying OCAS origin
                ))

                # --- Fill names table, one row per collected name ---------------------
                for name in names:
                    norm = normalize_name(name)

                    # Default/standard priority for a name entry.
                    priority = 2

                    # The name that matches the chosen display name gets the
                    # best (lowest) priority, i.e. it's the "canonical" one.
                    if name == cas_display:
                        priority = 1

                    # Simple chemical heuristic: salt-form names (hydrochloride,
                    # sodium, potassium salts) are generally considered less
                    # "canonical" than the base substance name, so bump their
                    # priority value up (worse ranking) by 1.
                    if any(x in norm for x in ["hydrochlorid", "natrium", "kalium"]):
                        priority += 1

                    names_rows.append((
                        ask,                       # ask_nummer: foreign key back to the substance
                        name,                      # name: original (unmodified) name text
                        norm,                      # name_normalized: normalized name version setting priority
                        name == cas_display,       # is_display: True if this is the chosen display name
                        priority,                  # priority: ranking priority
                        "BfArM BEZVO"              # source: fixed value identifying this data's origin
                    ))

        # --- Write everything to the database in two batched calls -----------------
        context.log.info(f"Inserting {len(substances_rows)} substances")
        cursor.executemany(insert_substances, substances_rows)

        context.log.info(f"Inserting {len(names_rows)} names")
        cursor.executemany(insert_names, names_rows)

        # Commit both batches together as a single transaction.
        conn.commit()

    # ------------------------------------------------------------------------------------------------------------------
    except Exception as e:
        # Log the error for visibility in the Dagster UI/logs, then re-raise so Dagster correctly marks this asset run as failed.
        context.log.error(f"An error occurred during import: {e}")
        raise e

    finally:
        # Always clean up the database connection, whether the run
        # succeeded or failed.
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    return
