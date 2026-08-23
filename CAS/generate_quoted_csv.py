import pandas as pd
import csv
import glob
import os

EXCLUDED_FILES = {"bezvo.csv", "missing_cas.csv","wrong_split.csv","check_join.csv"}

input_folder = "."

for input_csv in glob.glob(os.path.join(input_folder, "*.csv")):
    filename = os.path.basename(input_csv)

    if filename in EXCLUDED_FILES:
        continue
    if filename.endswith("_quoted.csv"):
        continue

    output_csv = input_csv.replace(".csv", "_quoted.csv")

    df = pd.read_csv(
        input_csv,
        sep=",",
        dtype=str
    )
    df.to_csv(
        output_csv,
        sep=",",
        index=False,
        encoding="utf-8",
        quoting=csv.QUOTE_ALL
    )