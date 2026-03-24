import pandas as pd
import pyodbc
from datetime import datetime
import math
import os

# =========================
# CONFIG
# =========================
EXCEL_FILE = r"C:\Users\shonk\source\PythonCodes\New folder\Registrations_Cleaned.xlsx"
SHEET_NAME = 0  # change if needed, e.g. "Sheet1"

SQL_SERVER = r"20.68.160.100,1433"  # update with your server and port if needed
SQL_DATABASE = "SahelihubCRM"

# For SQL auth use:
SQL_UID = "saheli_app"
SQL_PWD = "309183"

USE_WINDOWS_AUTH = False


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
def is_blank(val):
    if val is None:
        return True
    if isinstance(val, float) and math.isnan(val):
        return True
    if str(val).strip() == "":
        return True
    return False

def clean_str(val):
    if is_blank(val):
        return None
    s = str(val).strip()
    return s if s else None

def clean_yes_no(val):
    s = clean_str(val)
    if not s:
        return None
    s_low = s.lower()
    if s_low in {"yes", "y", "true", "1"}:
        return "Yes"
    if s_low in {"no", "n", "false", "0"}:
        return "No"
    return s

def clean_card_number(val):
    s = clean_str(val)
    if not s:
        return None

    # keep digits only if mixed values like SAH1001 / 1001.0
    s = s.replace(" ", "")
    if s.endswith(".0"):
        s = s[:-2]

    digits = "".join(ch for ch in s if ch.isdigit())
    return digits if digits else s

def parse_date(val):
    if is_blank(val):
        return None
    try:
        dt = pd.to_datetime(val, errors="coerce")
        if pd.isna(dt):
            return None
        return dt.date()
    except Exception:
        return None

def compute_age(dob):
    if not dob:
        return None
    today = datetime.today().date()
    age = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
    return age

def find_col(df, possible_names):
    df_cols_normalized = {str(c).strip().lower(): c for c in df.columns}
    for name in possible_names:
        key = name.strip().lower()
        if key in df_cols_normalized:
            return df_cols_normalized[key]
    return None

# =========================
# LOAD EXCEL
# =========================
def load_excel():
    df = pd.read_excel(EXCEL_FILE, sheet_name=SHEET_NAME)

    # Map raw excel headers to usable keys
    column_map = {
        "SaheliCardNumber": find_col(df, [" Saheli Card No: ", "Saheli Card No:", "Saheli Card No", "SaheliCardNumber"]),
        "FullName": find_col(df, [" Full Name:", "Full Name:", "Full Name"]),
        "DateOfBirth": find_col(df, [" Date of Birth:", "Date of Birth:", "Date of Birth"]),
        "Address": find_col(df, [" Address:", "Address:", "Address"]),
        "Postcode": find_col(df, [" Postcode:", "Postcode:", "Postcode"]),
        "Email": find_col(df, [" Email:", "Email:", "Email"]),
        "MobileNumber": find_col(df, [" Mobile/Home No:", "Mobile/Home No:", "Mobile/Home No", "Mobile Number", "MobileNumber"]),
        "EmergencyContactName": find_col(df, [" Emergency Contact Name:", "Emergency Contact Name:", "Emergency Contact Name"]),
        "EmergencyNo": find_col(df, [" Emergency No:", "Emergency No:", "Emergency No"]),
        "EmergencyRelationship": find_col(df, [" Emergency Relation To You:", "Emergency Relation To You:", "Emergency Relation To You"]),
        "Gender": find_col(df, [" Gender:", "Gender:", "Gender"]),
        "GenderSameAsBirth": find_col(df, [" Is your gender the same as assigned at birth?", "Is your gender the same as assigned at birth?"]),
        "HealthConditionsDisability": find_col(df, [" Health Conditions/Disability:", "Health Conditions/Disability:", "Health Conditions/Disability"]),
        "Ethnicity": find_col(df, [" Ethnicity:", "Ethnicity:", "Ethnicity"]),
        "PreferredLanguage": find_col(df, [" Preferred spoken language:", "Preferred spoken language:", "Preferred spoken language"]),
        "Religion": find_col(df, [" Religion:", "Religion:", "Religion"]),
        "Sexuality": find_col(df, [" Sexuality:", "Sexuality:", "Sexuality"]),
        "Occupation": find_col(df, [" Occupation:", "Occupation:", "Occupation"]),
        "LivingAlone": find_col(df, [" Living alone:", "Living alone:", "Living alone"]),
        "CaringResponsibilities": find_col(df, [" Caring responsibilities:", "Caring responsibilities:", "Caring responsibilities"]),
        "ReferralReason": find_col(df, ["Referral reason"]),
        "HeardAboutSaheli": find_col(df, [" How heard about Saheli Hub?", "How heard about Saheli Hub?"]),
        "GPSurgeryName": find_col(df, ["GP Surgery Name"]),
        "StaffMember": find_col(df, [" Staff Member:", "Staff Member:", "Staff Member"]),
        "Site": find_col(df, ["Site:", " Site:", "Site"]),
        "Notes": find_col(df, ["Notes:", " Notes:", "Notes"]),
        "RegistrationDate": find_col(df, [" Date:  ", " Date:", "Date:", "Date"]),
    }

    print("Detected columns:")
    for k, v in column_map.items():
        print(f"  {k}: {v}")

    return df, column_map

# =========================
# MAIN INSERT LOGIC
# =========================
def participant_exists(cursor, saheli_card_number):
    cursor.execute("""
        SELECT TOP 1 ParticipantID
        FROM Participants
        WHERE SaheliCardNumber = ?
    """, saheli_card_number)
    row = cursor.fetchone()
    return row[0] if row else None

def emergency_exists(cursor, participant_id, contact_name, contact_number, relationship):
    cursor.execute("""
        SELECT TOP 1 EmergencyContactID
        FROM ParticipantEmergencyContacts
        WHERE ParticipantID = ?
          AND ISNULL(ContactName, '') = ISNULL(?, '')
          AND ISNULL(ContactNumber, '') = ISNULL(?, '')
          AND ISNULL(Relationship, '') = ISNULL(?, '')
    """, participant_id, contact_name or "", contact_number or "", relationship or "")
    row = cursor.fetchone()
    return row is not None

def insert_participant(cursor, data):
    cursor.execute("""
        INSERT INTO Participants
        (
            SaheliCardNumber,
            FullName,
            DateOfBirth,
            Age,
            Address,
            Postcode,
            Email,
            MobileNumber,
            Gender,
            GenderSameAsBirth,
            Ethnicity,
            PreferredLanguage,
            Religion,
            Sexuality,
            Occupation,
            LivingAlone,
            CaringResponsibilities,
            ReferralReason,
            HeardAboutSaheli,
            GPSurgeryName,
            CreatedAt,
            HasHealthConditionOrDisability,
            HealthConditionDetails,
            StaffMember,
            Site,
            Notes,
            RegistrationDate
        )
        OUTPUT INSERTED.ParticipantID
        VALUES
        (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
    """,
        data["SaheliCardNumber"],
        data["FullName"],
        data["DateOfBirth"],
        data["Age"],
        data["Address"],
        data["Postcode"],
        data["Email"],
        data["MobileNumber"],
        data["Gender"],
        data["GenderSameAsBirth"],
        data["Ethnicity"],
        data["PreferredLanguage"],
        data["Religion"],
        data["Sexuality"],
        data["Occupation"],
        data["LivingAlone"],
        data["CaringResponsibilities"],
        data["ReferralReason"],
        data["HeardAboutSaheli"],
        data["GPSurgeryName"],
        data["CreatedAt"],
        data["HasHealthConditionOrDisability"],
        data["HealthConditionDetails"],
        data["StaffMember"],
        data["Site"],
        data["Notes"],
        data["RegistrationDate"]
    )
    row = cursor.fetchone()
    return row[0]

def insert_emergency_contact(cursor, data):
    cursor.execute("""
        INSERT INTO ParticipantEmergencyContacts
        (
            SaheliCardNumber,
            ContactName,
            ContactNumber,
            Relationship,
            ParticipantID
        )
        VALUES (?, ?, ?, ?, ?)
    """,
        data["SaheliCardNumber"],
        data["ContactName"],
        data["ContactNumber"],
        data["Relationship"],
        data["ParticipantID"]
    )

def build_row_data(row, colmap):
    saheli_card_number = clean_card_number(row[colmap["SaheliCardNumber"]]) if colmap["SaheliCardNumber"] else None
    dob = parse_date(row[colmap["DateOfBirth"]]) if colmap["DateOfBirth"] else None
    health_text = clean_str(row[colmap["HealthConditionsDisability"]]) if colmap["HealthConditionsDisability"] else None

    has_health_condition = None
    if health_text:
        if health_text.strip().lower() in {"no", "none", "nil", "n/a", "na"}:
            has_health_condition = "No"
        else:
            has_health_condition = "Yes"

    registration_date = parse_date(row[colmap["RegistrationDate"]]) if colmap["RegistrationDate"] else None

    participant_data = {
        "SaheliCardNumber": saheli_card_number,
        "FullName": clean_str(row[colmap["FullName"]]) if colmap["FullName"] else None,
        "DateOfBirth": dob,
        "Age": compute_age(dob),
        "Address": clean_str(row[colmap["Address"]]) if colmap["Address"] else None,
        "Postcode": clean_str(row[colmap["Postcode"]]) if colmap["Postcode"] else None,
        "Email": clean_str(row[colmap["Email"]]) if colmap["Email"] else None,
        "MobileNumber": clean_str(row[colmap["MobileNumber"]]) if colmap["MobileNumber"] else None,
        "Gender": clean_str(row[colmap["Gender"]]) if colmap["Gender"] else None,
        "GenderSameAsBirth": clean_yes_no(row[colmap["GenderSameAsBirth"]]) if colmap["GenderSameAsBirth"] else None,
        "Ethnicity": clean_str(row[colmap["Ethnicity"]]) if colmap["Ethnicity"] else None,
        "PreferredLanguage": clean_str(row[colmap["PreferredLanguage"]]) if colmap["PreferredLanguage"] else None,
        "Religion": clean_str(row[colmap["Religion"]]) if colmap["Religion"] else None,
        "Sexuality": clean_str(row[colmap["Sexuality"]]) if colmap["Sexuality"] else None,
        "Occupation": clean_str(row[colmap["Occupation"]]) if colmap["Occupation"] else None,
        "LivingAlone": clean_yes_no(row[colmap["LivingAlone"]]) if colmap["LivingAlone"] else None,
        "CaringResponsibilities": clean_str(row[colmap["CaringResponsibilities"]]) if colmap["CaringResponsibilities"] else None,
        "ReferralReason": clean_str(row[colmap["ReferralReason"]]) if colmap["ReferralReason"] else None,
        "HeardAboutSaheli": clean_str(row[colmap["HeardAboutSaheli"]]) if colmap["HeardAboutSaheli"] else None,
        "GPSurgeryName": clean_str(row[colmap["GPSurgeryName"]]) if colmap["GPSurgeryName"] else None,
        "CreatedAt": datetime.utcnow(),
        "HasHealthConditionOrDisability": has_health_condition,
        "HealthConditionDetails": health_text,
        "StaffMember": clean_str(row[colmap["StaffMember"]]) if colmap["StaffMember"] else None,
        "Site": clean_str(row[colmap["Site"]]) if colmap["Site"] else None,
        "Notes": clean_str(row[colmap["Notes"]]) if colmap["Notes"] else None,
        "RegistrationDate": registration_date,
    }

    emergency_data = {
        "SaheliCardNumber": saheli_card_number,
        "ContactName": clean_str(row[colmap["EmergencyContactName"]]) if colmap["EmergencyContactName"] else None,
        "ContactNumber": clean_str(row[colmap["EmergencyNo"]]) if colmap["EmergencyNo"] else None,
        "Relationship": clean_str(row[colmap["EmergencyRelationship"]]) if colmap["EmergencyRelationship"] else None,
    }

    return participant_data, emergency_data

def main():
    df, colmap = load_excel()

    if not colmap["SaheliCardNumber"]:
        raise Exception("Could not find the Saheli Card Number column in Excel.")

    conn = get_connection()
    conn.autocommit = False
    cursor = conn.cursor()

    inserted_participants = 0
    skipped_existing = 0
    inserted_emergency = 0
    skipped_no_card = 0
    failed_rows = 0

    try:
        for idx, row in df.iterrows():
            try:
                participant_data, emergency_data = build_row_data(row, colmap)
                card = participant_data["SaheliCardNumber"]

                if not card:
                    skipped_no_card += 1
                    print(f"[Row {idx+2}] Skipped - no SaheliCardNumber")
                    continue

                existing_participant_id = participant_exists(cursor, card)

                if existing_participant_id:
                    skipped_existing += 1
                    participant_id = existing_participant_id
                    print(f"[Row {idx+2}] Exists - card {card}, ParticipantID {participant_id}")
                else:
                    participant_id = insert_participant(cursor, participant_data)
                    inserted_participants += 1
                    print(f"[Row {idx+2}] Inserted participant - card {card}, ParticipantID {participant_id}")

                # Insert emergency contact only if there is something useful
                has_emergency = any([
                    emergency_data["ContactName"],
                    emergency_data["ContactNumber"],
                    emergency_data["Relationship"]
                ])

                if has_emergency:
                    if not emergency_exists(
                        cursor,
                        participant_id,
                        emergency_data["ContactName"],
                        emergency_data["ContactNumber"],
                        emergency_data["Relationship"]
                    ):
                        emergency_data["ParticipantID"] = participant_id
                        insert_emergency_contact(cursor, emergency_data)
                        inserted_emergency += 1
                        print(f"[Row {idx+2}] Inserted emergency contact for card {card}")
                    else:
                        print(f"[Row {idx+2}] Emergency contact already exists for card {card}")

            except Exception as row_ex:
                failed_rows += 1
                print(f"[Row {idx+2}] ERROR: {row_ex}")

        conn.commit()

        print("\n====================")
        print("IMPORT COMPLETED")
        print("====================")
        print(f"Inserted participants: {inserted_participants}")
        print(f"Skipped existing:      {skipped_existing}")
        print(f"Inserted emergency:    {inserted_emergency}")
        print(f"Skipped no card:       {skipped_no_card}")
        print(f"Failed rows:           {failed_rows}")

    except Exception as ex:
        conn.rollback()
        print(f"\nFATAL ERROR: {ex}")
        print("Transaction rolled back.")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()