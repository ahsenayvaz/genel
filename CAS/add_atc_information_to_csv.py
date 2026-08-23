"""
Ergänzt in insert_name.csv die Spalten atc_code und atc_display,
indem für jeden dwh_wirkstoff alle passenden Zeilen aus missing_cas.csv
(Match über ingredient_item_text == dwh_wirkstoff) gesucht werden.

Kommt ein dwh_wirkstoff in mehreren ATC-Codes vor, werden atc_code und
atc_display jeweils mit "|" getrennt zusammengeführt (Reihenfolge wie
zuerst in missing_cas.csv gefunden, Duplikate nach atc_code entfernt).

Bereits befüllte atc_code/atc_display-Zellen in insert_name.csv werden
NICHT überschrieben.
"""

import pandas as pd

INSERT_NAME_CSV = "insert_name.csv"
MISSING_CAS_CSV = "missing_cas.csv"
OUTPUT_CSV = "insert_name_filled.csv"

insert_df = pd.read_csv(INSERT_NAME_CSV, dtype=str).fillna("")
missing_df = pd.read_csv(MISSING_CAS_CSV, dtype=str).fillna("")

# Mapping: ingredient_item_text -> Liste von (atc_code, atc_display),
# in Reihenfolge des ersten Auftretens, dedupliziert nach atc_code.
atc_map: dict[str, list[tuple[str, str]]] = {}

for _, row in missing_df.iterrows():
    ingredient = row["ingredient_item_text"]
    code = row["atc_code"]
    display = row["atc_display"]

    if not ingredient or not code:
        continue

    entries = atc_map.setdefault(ingredient, [])
    if not any(existing_code == code for existing_code, _ in entries):
        entries.append((code, display))

filled_count = 0
unmatched = []

for idx, row in insert_df.iterrows():
    wirkstoff = row["dwh_wirkstoff"]

    # Bereits befüllte Zeilen nicht überschreiben
    if row["atc_code"].strip() or row["atc_display"].strip():
        continue

    entries = atc_map.get(wirkstoff)
    if not entries:
        unmatched.append(wirkstoff)
        continue

    codes = "|".join(code for code, _ in entries)
    displays = "|".join(display for _, display in entries)

    insert_df.at[idx, "atc_code"] = codes
    insert_df.at[idx, "atc_display"] = displays
    filled_count += 1

insert_df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")

print(f"Ergänzt: {filled_count} Zeilen")
print(f"Kein Match in missing_cas.csv gefunden: {len(unmatched)} Zeilen")
for w in unmatched:
    print(f"  - {w}")