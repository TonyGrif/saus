import sqlite3
from pathlib import Path

import pandas as pd

XLS_PATH = Path.home() / "Downloads" / "12s0414.xls"
DB_PATH = Path("./datasets/414.db")
TABLE = "congress_measures"

raw = pd.read_excel(XLS_PATH, sheet_name=0, header=None)

header_row = raw.iloc[3]

congresses = []
for val in header_row[1:]:
    val = str(val).strip()
    if "Congress" in val:
        parts = val.split(",")
        congress = parts[0].replace("Congress", "").strip()
        years = parts[1].strip().replace(" to ", "-") if len(parts) > 1 else ""
    else:
        congress, years = val, ""
    congresses.append((congress, years))

data_rows = raw.iloc[4:].reset_index(drop=True)
footer_start = data_rows[
    data_rows.iloc[:, 0].astype(str).str.startswith("FOOTNOTES")
].index
data_rows = data_rows.iloc[: footer_start[0]]

section = "National"
records = []

section_labels = {
    "HOUSE OF REPRESENTATIVES": "House",
    "SENATE": "Senate",
}

for _, row in data_rows.iterrows():
    label = str(row.iloc[0]).strip()

    if not label or label == "nan":
        continue

    if label in section_labels:
        section = section_labels[label]
        continue

    item = label.lstrip(". ").strip()

    item = item.replace("\\1", "").replace("\\2", "").strip()

    for i, (congress, years) in enumerate(congresses):
        raw_val = row.iloc[i + 1]
        try:
            value = float(raw_val)
        except (ValueError, TypeError):
            value = None

        records.append(
            {
                "congress": congress,
                "years": years,
                "section": section,
                "item": item,
                "value": value,
            }
        )

df = pd.DataFrame(records)

with sqlite3.connect(DB_PATH) as con:
    df.to_sql(TABLE, con, if_exists="replace", index=False)
