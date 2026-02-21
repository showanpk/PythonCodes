import pyodbc
import pandas as pd
from datetime import datetime

# ===============================
# CONFIG
# ===============================
CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=MIGHTYSUPERMAN;"
    "DATABASE=SahelihubCRM;"
    "Trusted_Connection=yes;"
)

EXCEL_FILE = "Participants.xlsx"
FAILED_FILE = "Participants_Failed.xlsx"
TABLE = "dbo.Participants"

# ===============================
# HELPERS
# ===============================
def clean(v):
    """Trim strings, turn blanks/NaN into None."""
    if v is None:
        return None
    if isinstance(v, float) and pd.isna(v):
        return None
    if isinstance(v, str):
        s = v.strip()
        return s if s != "" else None
    # pandas Timestamp / datetime etc.
    if pd.isna(v):
        return None
    return v

def yes_no_to_bit(v):
    """Convert Yes/No into 1/0, keep None if blank."""
    v = clean(v)
    if v is None:
        return None
    s = str(v).strip().lower()
    if s in ("yes", "y", "true", "1"):
        return 1
    if s in ("no", "n", "false", "0"):
        return 0
    # unknown text -> store None so insert doesn't break on BIT columns
    return None

def normalize_date(v):
    """
    Excel dates may come as Timestamp/datetime or string like '19-Sep-24'.
    SQL column is DATE. pyodbc accepts datetime/date objects.
    """
    v = clean(v)
    if v is None:
        return None
    if isinstance(v, (datetime, pd.Timestamp)):
        return v.date()
    # try parse strings
    try:
        dt = pd.to_datetime(v, dayfirst=True, errors="coerce")
        if pd.isna(dt):
            return None
        return dt.date()
    except:
        return None

# ===============================
# LOAD EXCEL
# ===============================
df = pd.read_excel(EXCEL_FILE)

# Ensure required columns exist (ignore extras)
expected_cols = [
    "SaheliCardNumber","FullName","DateOfBirth","Age","Address","Postcode","Email",
    "MobileNumber","Gender","GenderSameAsBirth","Ethnicity","PreferredLanguage",
    "Religion","Sexuality","Occupation","LivingAlone","CaringResponsibilities",
    "ReferralReason","HeardAboutSaheli","GPSurgeryName"
]

missing = [c for c in expected_cols if c not in df.columns]
if missing:
    raise ValueError(f"Missing columns in Excel: {missing}")

# Only keep expected columns (in correct order)
df = df[expected_cols].copy()

# Clean + conversions
df["GenderSameAsBirth"] = df["GenderSameAsBirth"].apply(yes_no_to_bit)
df["LivingAlone"] = df["LivingAlone"].apply(yes_no_to_bit)
df["CaringResponsibilities"] = df["CaringResponsibilities"].apply(yes_no_to_bit)
df["DateOfBirth"] = df["DateOfBirth"].apply(normalize_date)

# Clean all other cells column-wise (no applymap)
for c in df.columns:
    if c not in ("GenderSameAsBirth", "LivingAlone", "CaringResponsibilities", "DateOfBirth"):
        df[c] = df[c].apply(clean)

# ===============================
# SQL INSERT (CreatedAt auto)
# ===============================
insert_sql = f"""
INSERT INTO {TABLE} (
    SaheliCardNumber, FullName, DateOfBirth, Age, Address, Postcode, Email,
    MobileNumber, Gender, GenderSameAsBirth, Ethnicity, PreferredLanguage, Religion,
    Sexuality, Occupation, LivingAlone, CaringResponsibilities, ReferralReason,
    HeardAboutSaheli, GPSurgeryName
)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
"""

# ===============================
# INSERT ROW BY ROW
# ===============================
conn = pyodbc.connect(CONN_STR)
cursor = conn.cursor()

success = 0
failed_rows = []

for idx, r in df.iterrows():
    values = (
        r["SaheliCardNumber"],
        r["FullName"],
        r["DateOfBirth"],
        r["Age"],
        r["Address"],
        r["Postcode"],
        r["Email"],
        r["MobileNumber"],
        r["Gender"],
        r["GenderSameAsBirth"],
        r["Ethnicity"],
        r["PreferredLanguage"],
        r["Religion"],
        r["Sexuality"],
        r["Occupation"],
        r["LivingAlone"],
        r["CaringResponsibilities"],
        r["ReferralReason"],
        r["HeardAboutSaheli"],
        r["GPSurgeryName"],
    )

    try:
        cursor.execute(insert_sql, values)
        conn.commit()
        success += 1
    except Exception as e:
        conn.rollback()
        row_dict = r.to_dict()
        row_dict["_RowIndex"] = int(idx)
        row_dict["_Error"] = str(e)
        failed_rows.append(row_dict)

cursor.close()
conn.close()

print(f"✅ Inserted successfully: {success}")
print(f"❌ Failed rows: {len(failed_rows)}")

# ===============================
# EXPORT FAILURES
# ===============================
if failed_rows:
    failed_df = pd.DataFrame(failed_rows)
    # write without openpyxl dependency using xlsxwriter if available, else fallback to csv
    try:
        failed_df.to_excel(FAILED_FILE, index=False)  # pandas will pick an engine automatically if installed
        print(f"📁 Failed rows exported to: {FAILED_FILE}")
    except Exception:
        csv_file = FAILED_FILE.replace(".xlsx", ".csv")
        failed_df.to_csv(csv_file, index=False)
        print(f"📁 Could not write xlsx (missing engine). Exported CSV instead: {csv_file}")
