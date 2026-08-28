from pathlib import Path
import csv

BASE_DIR = Path(__file__).parent

INPUT = BASE_DIR / "missing_cas_mitquelle.csv"
OUTPUT = BASE_DIR / "insert_name.csv"


def add_unique(values, value):
    value = (value or "").strip()
    if value and value not in values:
        values.append(value)


with INPUT.open(newline="", encoding="utf-8") as f:
    source_rows = list(csv.DictReader(f))

groups = {}

for row in source_rows:

    # Şimdilik SADECE add_name_mapping
    if (row.get("action") or "").strip() != "add_name_mapping":
        continue

    dwh_name = (row.get("ingredient_item_text") or "").strip()
    found_ask = (row.get("found_ask_id") or "").strip()

    if not dwh_name or not found_ask:
        continue

    key = (dwh_name, found_ask)

    if key not in groups:
        groups[key] = {
            "atc_pairs": [],
            "dwh_wirkstoff": dwh_name,
            "found_ask": found_ask,
            "cas_display": "",
            "sources": [],
            "comments": [],
        }

    group = groups[key]

    # ATC code + display birlikte tutulur
    atc_code = (row.get("atc_code") or "").strip()
    atc_display = (row.get("atc_display") or "").strip()

    pair = (atc_code, atc_display)

    if pair != ("", "") and pair not in group["atc_pairs"]:
        group["atc_pairs"].append(pair)

    # CAS display
    cas_display = (row.get("found_cas_display") or "").strip()

    if cas_display:
        if group["cas_display"] and group["cas_display"] != cas_display:
            raise ValueError(
                f"{key}: different found_cas_display values: "
                f"{group['cas_display']} / {cas_display}"
            )

        group["cas_display"] = cas_display

    # Güncel kaynak: missing_cas_mitquelle.csv
    add_unique(group["sources"], row.get("cas_quelle"))

    # Comment
    add_unique(group["comments"], row.get("comment"))


output_rows = []

for group in groups.values():

    atc_codes = [x[0] for x in group["atc_pairs"]]
    atc_displays = [x[1] for x in group["atc_pairs"]]

    output_rows.append({
        "atc_code": "|".join(atc_codes),
        "atc_display": "|".join(atc_displays),
        "dwh_wirkstoff": group["dwh_wirkstoff"],
        "found_ask": group["found_ask"],
        "cas_display": group["cas_display"],
        "cas_name_source": "|".join(group["sources"]),
        "action_import": "add_cas_name",
        "comment": "|".join(group["comments"]),
    })


with OUTPUT.open("w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=[
            "atc_code",
            "atc_display",
            "dwh_wirkstoff",
            "found_ask",
            "cas_display",
            "cas_name_source",
            "action_import",
            "comment",
        ],
    )

    writer.writeheader()
    writer.writerows(output_rows)


print(f"Created: {OUTPUT}")
print(f"Unique add_name_mapping entries: {len(output_rows)}")
