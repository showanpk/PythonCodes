import pandas as pd
import json

CSV_FILE = r"C:\SQLImports\SaheliMigrations.csv"
OUTPUT_CSV = r"C:\SQLImports\SaheliMigrations_rawjson.csv"

df = pd.read_csv(
    CSV_FILE,
    dtype=str,          # keep everything as text
    encoding="cp1252"   # 🔥 FIX HERE
)

df = df.fillna("")

json_rows = df.apply(
    lambda row: json.dumps(row.to_dict(), ensure_ascii=False),
    axis=1
)

out_df = pd.DataFrame({"RawJson": json_rows})

out_df.to_csv(
    OUTPUT_CSV,
    index=False,
    encoding="utf-8"
)

print("✅ JSON-per-row CSV created:", OUTPUT_CSV)
