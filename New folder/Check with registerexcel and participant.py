import pandas as pd
import pyodbc
import re
import os

# =========================
# CONFIG
# =========================
EXCEL_FILE = r"C:\Users\shonk\Downloads\Full Registration for SAHELI (1).xlsx"
SHEET_NAME = "Full Register"  # or "Sheet1"

OUTPUT_FILE = r"C:\Users\shonk\Downloads\Missing_Saheli_Cards.xlsx"

SQL_SERVER = r"20.68.160.100,1433"
SQL_DATABASE = "SahelihubCRM"
USE_WINDOWS_AUTH = False

# If not using Windows auth, uncomment and fill these:
# For SQL auth use:
SQL_UID = "saheli_app"
SQL_PWD = "309183"


def env_bool(name, default):
    val = os.getenv(name)
    if val is None:
        return default
    return val.strip().lower() in {"1", "true", "yes", "y", "on"}


def resolve_db_config():
    return {
        "server": os.getenv("DB_SERVER", SQL_SERVER),
        "database": os.getenv("DB_DATABASE", SQL_DATABASE),
        "uid": os.getenv("DB_USERNAME", SQL_UID),
        "pwd": os.getenv("DB_PASSWORD", SQL_PWD),
        "use_windows_auth": env_bool("DB_TRUSTED_CONNECTION", USE_WINDOWS_AUTH),
    }



# =========================
# DB CONNECTION
# =========================
def get_connection():
    db = resolve_db_config()

    if db["use_windows_auth"]:
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={db['server']};"
            f"DATABASE={db['database']};"
            f"Trusted_Connection=yes;"
        )
    else:
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={db['server']};"
            f"DATABASE={db['database']};"
            f"UID={db['uid']};"
            f"PWD={db['pwd']};"
        )

    try:
        return pyodbc.connect(conn_str)
    except pyodbc.Error as ex:
        auth_mode = "Windows Integrated Authentication" if db["use_windows_auth"] else "SQL Authentication"
        raise RuntimeError(
            "Database connection failed using "
            f"{auth_mode}. "
            "Set DB_TRUSTED_CONNECTION=false to force SQL login, "
            "or set DB_USERNAME/DB_PASSWORD with valid credentials."
        ) from ex


# =========================
# HELPERS
# =========================
def clean_card_number(val):
    if pd.isna(val):
        return None

    s = str(val).strip()
    if not s:
        return None

    # remove trailing .0 from Excel numeric-looking values
    if s.endswith(".0"):
        s = s[:-2]

    # keep digits only if mixed text exists
    digits = re.sub(r"\D", "", s)

    return digits if digits else s


def find_saheli_column(df):
    def normalize_header(value):
        s = str(value).strip().lower()
        return re.sub(r"[^a-z0-9]", "", s)

    wanted = {
        "sahelicardnumber",
        "sahelicardno",
        "sahelicard",
    }

    for col in df.columns:
        col_clean = normalize_header(col)
        if col_clean in wanted:
            return col
    return None


# =========================
# MAIN
# =========================
def main():
    print("Reading Excel...")
    df = pd.read_excel(EXCEL_FILE, sheet_name=SHEET_NAME)

    saheli_col = find_saheli_column(df)
    if not saheli_col:
        print("Available columns in Excel:")
        for c in df.columns:
            print(f"  - {c}")
        raise Exception("Could not find a Saheli Card Number column in Excel.")

    print(f"Detected column: {saheli_col}")

    # Clean Excel card numbers
    excel_df = df.copy()
    excel_df["CleanSaheliCardNumber"] = excel_df[saheli_col].apply(clean_card_number)

    # Remove blank rows
    excel_df = excel_df[excel_df["CleanSaheliCardNumber"].notna()].copy()

    # Preserve first occurrence only
    excel_df = excel_df.drop_duplicates(subset=["CleanSaheliCardNumber"])

    print(f"Unique non-empty card numbers found in Excel: {len(excel_df)}")

    print("Reading Participants from SQL Server...")
    conn = get_connection()

    sql = """
        SELECT SaheliCardNumber
        FROM Participants
        WHERE SaheliCardNumber IS NOT NULL
    """

    db_df = pd.read_sql(sql, conn)
    conn.close()

    db_df["CleanSaheliCardNumber"] = db_df["SaheliCardNumber"].apply(clean_card_number)
    db_df = db_df[db_df["CleanSaheliCardNumber"].notna()].drop_duplicates(subset=["CleanSaheliCardNumber"])

    print(f"Unique card numbers found in DB: {len(db_df)}")

    # Left anti join: in Excel but not in DB
    missing_df = excel_df.merge(
        db_df[["CleanSaheliCardNumber"]],
        on="CleanSaheliCardNumber",
        how="left",
        indicator=True
    )

    missing_df = missing_df[missing_df["_merge"] == "left_only"].copy()

    # Keep useful output columns
    result_df = missing_df[[saheli_col, "CleanSaheliCardNumber"]].copy()
    result_df = result_df.rename(columns={
        saheli_col: "OriginalExcelValue",
        "CleanSaheliCardNumber": "MissingSaheliCardNumber"
    })

    result_df.to_excel(OUTPUT_FILE, index=False)

    print("\n============================")
    print("COMPARE COMPLETED")
    print("============================")
    print(f"Missing in system: {len(result_df)}")
    print(f"Output saved to: {OUTPUT_FILE}")

    if len(result_df) > 0:
        print("\nFirst few missing card numbers:")
        print(result_df.head(20).to_string(index=False))
    else:
        print("\nNo missing card numbers found.")


if __name__ == "__main__":
    main()