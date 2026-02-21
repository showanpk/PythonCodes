import pyodbc
import json
import pandas as pd
import re

CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=MIGHTYSUPERMAN;"
    "DATABASE=SahelihubCRM;"
    "Trusted_Connection=yes;"
)

SOURCE_TABLE = "dbo.Staging_HealthAssessments_RawJson"

def clean_name(name):
    if not name:
        return None

    name = name.strip()

    # Standardise spacing & casing
    name = re.sub(r"\s+", " ", name)
    name = name.title()

    return name

conn = pyodbc.connect(CONN_STR)
df = pd.read_sql(f"SELECT RawJson FROM {SOURCE_TABLE}", conn)
conn.close()

staff_set = set()

for _, row in df.iterrows():
    try:
        j = json.loads(row["RawJson"])
    except:
        continue

    raw_staff = j.get(" Staff Member:")
    if not raw_staff:
        continue

    # Split combined names
    parts = re.split(r"\band\b|&|,", raw_staff, flags=re.IGNORECASE)

    for p in parts:
        name = clean_name(p)
        if name:
            staff_set.add(name)

staff_df = pd.DataFrame(sorted(staff_set), columns=["StaffName"])

staff_df.to_excel("Staff_Cleaned.xlsx", index=False)

print(f"✅ Distinct staff extracted: {len(staff_df)}")
print("📁 Review file: Staff_Cleaned.xlsx")
