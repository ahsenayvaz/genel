from dagster import asset, AssetExecutionContext
import csv
import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv


load_dotenv(dotenv_path="settings_global.env", override=True)
load_dotenv(dotenv_path="settings_local.env", override=True)

db_host = os.getenv("DGH_CENTRAL_PQ_HOST")
db_port = os.getenv("DGH_CENTRAL_PQ_PORT")
db_name = os.getenv("DGH_CENTRAL_PQ_NAME")
db_user = os.getenv("DGH_CENTRAL_PQ_USER")
db_password = os.getenv("DGH_CENTRAL_PQ_PASSWORD")

mapping_table = "medikation_cas_overwrite"

csv_path = (
    Path(__file__).resolve().parent.parent
    / "misc"
    / "cas"
    / "clickhouse_sync"
    / "medikation_cas_overwrite.csv"
)

COLUMNS = [
    "medikation_id",
    "ingredient_item_coding_code_cas_1",
    "ingredient_item_coding_display_cas_1",
    "ingredient_item_coding_code_ask_1",
    "ingredient_item_coding_code_cas_2",
    "ingredient_item_coding_display_cas_2",
    "ingredient_item_coding_code_ask_2",
    "ingredient_item_coding_code_cas_3",
    "ingredient_item_coding_display_cas_3",
    "ingredient_item_coding_code_ask_3",
]


def empty_to_none(value):
    value = (value or "").strip()
    return value if value else None


def load_rows():
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    with csv_path.open(encoding="utf-8-sig", newline="") as csvfile:
        reader = csv.DictReader(csvfile)

        missing_columns = [
            column
            for column in COLUMNS
            if column not in (reader.fieldnames or [])
        ]

        if missing_columns:
            raise ValueError(
                f"Missing columns in CSV: {missing_columns}"
            )

        rows = list(reader)

    medikation_ids = [
        (row.get("medikation_id") or "").strip()
        for row in rows
    ]

    if any(not medikation_id for medikation_id in medikation_ids):
        raise ValueError("CSV contains an empty medikation_id.")

    if len(medikation_ids) != len(set(medikation_ids)):
        raise ValueError("CSV contains duplicate medikation_id values.")

    return rows


CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {mapping_table} (
    medikation_id TEXT PRIMARY KEY,

    ingredient_item_coding_code_cas_1 TEXT,
    ingredient_item_coding_display_cas_1 TEXT,
    ingredient_item_coding_code_ask_1 TEXT,

    ingredient_item_coding_code_cas_2 TEXT,
    ingredient_item_coding_display_cas_2 TEXT,
    ingredient_item_coding_code_ask_2 TEXT,

    ingredient_item_coding_code_cas_3 TEXT,
    ingredient_item_coding_display_cas_3 TEXT,
    ingredient_item_coding_code_ask_3 TEXT,

    datensatz_erstellt TIMESTAMP DEFAULT now(),
    datensatz_geaendert TIMESTAMP DEFAULT now()
);
"""

INSERT_SQL = f"""
INSERT INTO {mapping_table} (
    medikation_id,
    ingredient_item_coding_code_cas_1,
    ingredient_item_coding_display_cas_1,
    ingredient_item_coding_code_ask_1,
    ingredient_item_coding_code_cas_2,
    ingredient_item_coding_display_cas_2,
    ingredient_item_coding_code_ask_2,
    ingredient_item_coding_code_cas_3,
    ingredient_item_coding_display_cas_3,
    ingredient_item_coding_code_ask_3
)
VALUES (
    %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s
);
"""


@asset(
    key="medikation_cas_overwrite_import",
    description=(
        "Imports medication-id based CAS/ASK overwrite values "
        "into dgh_central."
    ),
)
def medikation_cas_overwrite_import(
    context: AssetExecutionContext,
) -> None:

    rows = load_rows()

    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        user=db_user,
        password=db_password,
        database=db_name,
    )

    cursor = conn.cursor()

    try:
        cursor.execute(CREATE_TABLE_SQL)

        # Tabelle soll exakt dem aktuellen CSV entsprechen.
        cursor.execute(f"DELETE FROM {mapping_table};")

        values = [
            tuple(
                empty_to_none(row.get(column))
                for column in COLUMNS
            )
            for row in rows
        ]

        cursor.executemany(INSERT_SQL, values)

        conn.commit()

        context.log.info(
            f"Imported {len(rows)} rows into {mapping_table}."
        )

    except Exception:
        conn.rollback()
        raise

    finally:
        cursor.close()
        conn.close()
