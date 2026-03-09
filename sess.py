import os
import pyodbc
import pandas as pd
from pathlib import Path

# ============================================================
# CONFIG
# ============================================================
FILES_TO_IMPORT = [
    r"C:\Users\shonk\Desktop\ARCC Activity Register  2026.xlsx",
    r"C:\Users\shonk\Desktop\Handsworth Register 2026.xlsx",
    r"C:\Users\shonk\Desktop\Mens Sessions 2026.xlsx",
    r"C:\Users\shonk\Downloads\members-export-2026-03-09.csv",
]

SKIP_SHEETS = {"Template"}
TARGET_TABLE = "dbo.ActivityRegisterImport"

SQL_SERVER = r"20.68.160.100"   # e.g. localhost\SQLEXPRESS
SQL_DATABASE = "SahelihubCRM"
SQL_USERNAME = "saheli_app"                # None = Windows auth
SQL_PASSWORD = "309183"  # Replace with actual password
SQL_DRIVER = "ODBC Driver 17 for SQL Server"

SEARCH_DIRS = [
    Path.cwd(),
    Path.home() / "Downloads",
    Path.home() / "Desktop",
    Path.home() / "Documents",
    Path.home() / "OneDrive",
    Path.home() / "OneDrive" / "Desktop",
    Path.home() / "OneDrive" / "Documents",
    Path.home() / "OneDrive - Saheli Hub",
    Path.home() / "OneDrive - Saheli Hub" / "Documents",
]

# ============================================================
# DB CONNECTION
# ============================================================
if SQL_USERNAME and SQL_PASSWORD:
    conn_str = (
        f"DRIVER={{{SQL_DRIVER}}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        f"UID={SQL_USERNAME};"
        f"PWD={SQL_PASSWORD};"
        "TrustServerCertificate=yes;"
    )
else:
    conn_str = (
        f"DRIVER={{{SQL_DRIVER}}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        "Trusted_Connection=yes;"
        "TrustServerCertificate=yes;"
    )

conn = pyodbc.connect(conn_str)
cursor = conn.cursor()
cursor.fast_executemany = True

# ============================================================
# HELPERS
# ============================================================
def ensure_target_table(cursor, table_name):
    if table_name != "dbo.ActivityRegisterImport":
        return

    cursor.execute(
        """
IF OBJECT_ID(N'dbo.ActivityRegisterImport', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.ActivityRegisterImport
    (
        Id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        SourceFile NVARCHAR(255) NOT NULL,
        SourceType NVARCHAR(20) NOT NULL,
        SourceSheet NVARCHAR(150) NULL,
        SourceRowNumber INT NOT NULL,
        SessionName NVARCHAR(255) NULL,
        DayName NVARCHAR(50) NULL,
        SessionDate NVARCHAR(100) NULL,
        MonthName NVARCHAR(50) NULL,
        SessionTime NVARCHAR(100) NULL,
        SaheliCardNumber NVARCHAR(100) NULL,
        MemberName NVARCHAR(255) NULL,
        EmergencyContactName NVARCHAR(255) NULL,
        EmergencyNumber NVARCHAR(100) NULL,
        RiskStratification NVARCHAR(100) NULL,
        ImportedAt DATETIME2 NOT NULL
            CONSTRAINT DF_ActivityRegisterImport_ImportedAt DEFAULT SYSUTCDATETIME()
    );
END
"""
    )

def clean_value(v):
    if pd.isna(v):
        return None
    if isinstance(v, str):
        v = v.strip()
        return v if v else None
    return str(v)

def normalize_header(h):
    if h is None:
        return ""
    h = str(h).strip().lower()
    h = h.replace("\n", " ")
    h = h.replace("_", " ")
    h = " ".join(h.split())
    return h

def find_column(df, possible_names):
    normalized_map = {normalize_header(col): col for col in df.columns}
    for name in possible_names:
        if normalize_header(name) in normalized_map:
            return normalized_map[normalize_header(name)]
    return None

def resolve_input_file(file_path):
    original = Path(file_path).expanduser()
    candidates = [original]

    parent_name = original.parent.name
    if parent_name:
        for base_dir in SEARCH_DIRS:
            candidates.append(base_dir / original.name)
            candidates.append(base_dir / parent_name / original.name)
    else:
        for base_dir in SEARCH_DIRS:
            candidates.append(base_dir / original.name)

    seen = set()
    for candidate in candidates:
        normalized = str(candidate).lower()
        if normalized in seen:
            continue
        seen.add(normalized)
        if candidate.exists():
            return candidate

    searched = "\n".join(f" - {candidate}" for candidate in candidates[:12])
    raise FileNotFoundError(
        f"Could not find input file '{original.name}'. Checked:\n{searched}"
    )

def map_columns(df):
    return {
        "SessionName": find_column(df, [
            "Session", "Session Name", "Activity", "Activity Name", "Class", "Programme"
        ]),
        "DayName": find_column(df, [
            "Day", "Day Name"
        ]),
        "SessionDate": find_column(df, [
            "Date", "Session Date"
        ]),
        "MonthName": find_column(df, [
            "Month"
        ]),
        "SessionTime": find_column(df, [
            "Time", "Session Time"
        ]),
        "SaheliCardNumber": find_column(df, [
            "Saheli Card Number", "SaheliCardNumber", "Card Number", "Card No", "Saheli No"
        ]),
        "MemberName": find_column(df, [
            "Name", "Member Name", "Full Name", "Participant Name"
        ]),
        "EmergencyContactName": find_column(df, [
            "Emergency Contact Name", "Emergency contact Name", "Emergency Contact"
        ]),
        "EmergencyNumber": find_column(df, [
            "Emergency Number", "Emergency Contact Number", "Emergency Phone"
        ]),
        "RiskStratification": find_column(df, [
            "Risk Stratification", "Risk"
        ]),
    }

def prepare_rows(df, source_file, source_type, source_sheet):
    rows = []
    colmap = map_columns(df)

    for idx, row in df.iterrows():
        session_name = clean_value(row[colmap["SessionName"]]) if colmap["SessionName"] else None
        day_name = clean_value(row[colmap["DayName"]]) if colmap["DayName"] else None
        session_date = clean_value(row[colmap["SessionDate"]]) if colmap["SessionDate"] else None
        month_name = clean_value(row[colmap["MonthName"]]) if colmap["MonthName"] else None
        session_time = clean_value(row[colmap["SessionTime"]]) if colmap["SessionTime"] else None
        saheli_card_number = clean_value(row[colmap["SaheliCardNumber"]]) if colmap["SaheliCardNumber"] else None
        member_name = clean_value(row[colmap["MemberName"]]) if colmap["MemberName"] else None
        emergency_contact_name = clean_value(row[colmap["EmergencyContactName"]]) if colmap["EmergencyContactName"] else None
        emergency_number = clean_value(row[colmap["EmergencyNumber"]]) if colmap["EmergencyNumber"] else None
        risk_stratification = clean_value(row[colmap["RiskStratification"]]) if colmap["RiskStratification"] else None

        # skip fully empty rows
        if all(v is None for v in [
            session_name, day_name, session_date, month_name, session_time,
            saheli_card_number, member_name, emergency_contact_name,
            emergency_number, risk_stratification
        ]):
            continue

        rows.append((
            source_file,
            source_type,
            source_sheet,
            idx + 2,   # approximate Excel/CSV visible row number
            session_name,
            day_name,
            session_date,
            month_name,
            session_time,
            saheli_card_number,
            member_name,
            emergency_contact_name,
            emergency_number,
            risk_stratification,
        ))
    return rows

# ============================================================
# INSERT SQL
# ============================================================
insert_sql = f"""
INSERT INTO {TARGET_TABLE}
(
    SourceFile,
    SourceType,
    SourceSheet,
    SourceRowNumber,
    SessionName,
    DayName,
    SessionDate,
    MonthName,
    SessionTime,
    SaheliCardNumber,
    MemberName,
    EmergencyContactName,
    EmergencyNumber,
    RiskStratification
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

# ============================================================
# IMPORT FILES
# ============================================================
ensure_target_table(cursor, TARGET_TABLE)
conn.commit()

all_rows = []

for file_path in FILES_TO_IMPORT:
    path = resolve_input_file(file_path)
    ext = path.suffix.lower()
    source_file = path.name

    print(f"Reading: {source_file}")

    if ext == ".csv":
        df = pd.read_csv(path, dtype=str)
        df = df.dropna(how="all")
        rows = prepare_rows(df, source_file, "CSV", None)
        all_rows.extend(rows)

    elif ext in [".xlsx", ".xlsm", ".xls"]:
        excel_file = pd.ExcelFile(path, engine="openpyxl")
        for sheet_name in excel_file.sheet_names:
            if sheet_name in SKIP_SHEETS:
                continue

            df = pd.read_excel(path, sheet_name=sheet_name, dtype=str, engine="openpyxl")
            df = df.dropna(how="all")
            rows = prepare_rows(df, source_file, "XLSX", sheet_name)
            all_rows.extend(rows)

    else:
        print(f"Skipped unsupported file: {source_file}")

# ============================================================
# SAVE TO SQL
# ============================================================
if all_rows:
    cursor.executemany(insert_sql, all_rows)
    conn.commit()
    print(f"Inserted {len(all_rows)} rows into {TARGET_TABLE}")
else:
    print("No rows found to import.")

cursor.close()
conn.close()
