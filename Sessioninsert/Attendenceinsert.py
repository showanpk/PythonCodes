import pandas as pd
import pyodbc
import os
from datetime import datetime

# =========================
# CONFIG
# =========================
EXCEL_FILE = r"C:\Users\shonk\Downloads\Calthorpe_SessionAttendance_Export.xlsx"
TABLE_NAME = "dbo.SessionAttendanceimports"
INVALID_ROWS_OUTPUT_FILE = r"C:\Users\shonk\Downloads\Calthorpe_SessionAttendanceimports_InvalidRows.xlsx"

SERVER = "20.68.160.100,1433"
DATABASE = "SahelihubCRM"

USE_TRUSTED_CONNECTION = False
USERNAME = "saheli_app"
PASSWORD = "309183"

ODBC_DRIVER = "ODBC Driver 18 for SQL Server"


# =========================
# DB CONNECTION
# =========================
def get_connection():
    server = os.getenv("DB_SERVER", SERVER)
    database = os.getenv("DB_DATABASE", DATABASE)
    username = os.getenv("DB_USERNAME", USERNAME)
    password = os.getenv("DB_PASSWORD", PASSWORD)
    use_trusted_env = os.getenv("DB_TRUSTED_CONNECTION")
    use_trusted = (
        USE_TRUSTED_CONNECTION
        if use_trusted_env is None
        else use_trusted_env.strip().lower() in {"1", "true", "yes", "y"}
    )

    def build_conn_str(trusted: bool):
        base = (
            f"DRIVER={{{ODBC_DRIVER}}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"TrustServerCertificate=yes;"
        )
        if trusted:
            return base + "Trusted_Connection=yes;"
        return base + f"UID={username};PWD={password};"

    try:
        return pyodbc.connect(build_conn_str(use_trusted))
    except pyodbc.InterfaceError as first_error:
        # If integrated auth fails in domain-trust scenarios, retry SQL auth once.
        if use_trusted:
            try:
                return pyodbc.connect(build_conn_str(False))
            except pyodbc.InterfaceError:
                pass
        raise RuntimeError(
            "Database connection failed. "
            "Check DB_USERNAME/DB_PASSWORD or set DB_TRUSTED_CONNECTION=false "
            "to force SQL authentication."
        ) from first_error


# =========================
# HELPERS
# =========================
def clean_value(v):
    if pd.isna(v):
        return None
    if isinstance(v, str):
        v = v.strip()
        if v == "":
            return None
    return v


def parse_date(v):
    v = clean_value(v)
    if v is None:
        return None
    dt = pd.to_datetime(v, errors="coerce")
    if pd.isna(dt):
        return None
    return dt.date()


def parse_datetime(v):
    v = clean_value(v)
    if v is None:
        return None
    dt = pd.to_datetime(v, errors="coerce")
    if pd.isna(dt):
        return None
    return dt.to_pydatetime()


def parse_time(v):
    v = clean_value(v)
    if v is None:
        return None

    text = str(v).strip()

    # Handles values like 09:30:00
    try:
        return datetime.strptime(text, "%H:%M:%S").time()
    except ValueError:
        pass

    # Handles values like 09:30
    try:
        return datetime.strptime(text, "%H:%M").time()
    except ValueError:
        pass

    # Handles Excel parsed time-like values
    dt = pd.to_datetime(text, errors="coerce")
    if pd.isna(dt):
        return None
    return dt.time()


def parse_bit(v, default=0):
    v = clean_value(v)
    if v is None:
        return default
    if isinstance(v, bool):
        return int(v)
    text = str(v).strip().lower()
    if text in {"1", "true", "yes", "y"}:
        return 1
    if text in {"0", "false", "no", "n"}:
        return 0
    try:
        return 1 if int(float(text)) != 0 else 0
    except Exception:
        return default


def parse_int(v):
    v = clean_value(v)
    if v is None:
        return None
    try:
        if isinstance(v, str):
            v = v.replace(",", "")
        return int(float(v))
    except Exception:
        return None


def read_excel_file(path):
    df = pd.read_excel(path)
    df.columns = [str(c).strip() for c in df.columns]
    return df


def to_date_key(v):
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.date().isoformat()
    try:
        return v.isoformat()
    except Exception:
        return None


def to_time_key(v):
    if v is None:
        return None
    try:
        return v.strftime("%H:%M:%S")
    except Exception:
        return None


def fetch_sessions_lookup(conn):
    sql = """
    SELECT
        SessionId,
        ActivityName,
        SessionDate,
        CONVERT(varchar(8), StartTime, 108) AS StartTimeStr,
        CONVERT(varchar(8), EndTime, 108) AS EndTimeStr
    FROM dbo.Sessions
    """
    df = pd.read_sql(sql, conn)

    lookup = {}
    for _, row in df.iterrows():
        key = (
            clean_value(row["ActivityName"]),
            pd.to_datetime(row["SessionDate"]).strftime("%Y-%m-%d") if pd.notna(row["SessionDate"]) else None,
            clean_value(row["StartTimeStr"]),
            clean_value(row["EndTimeStr"]),
        )
        lookup[key] = int(row["SessionId"])
    return lookup


# =========================
# MAIN INSERT
# =========================
def insert_attendance_imports():
    df = read_excel_file(EXCEL_FILE)

    required_columns = [
        "SessionId",
        "ParticipantId",
        "SessionName",
        "SessionDay",
        "SessionDate",
        "SessionMonth",
        "SessionStartTime",
        "SessionEndTime",
        "SaheliCardNumber",
        "RiskStratification",
        "Attended",
        "CheckInTime",
        "CheckOutTime",
        "Notes",
        "CreatedAtUtc",
        "UpdatedAtUtc",
        "AttendanceMemberKind",
        "LiteMemberId",
        "MemberDisplayId",
        "MemberName",
        "Phone",
        "EmergencyName",
        "EmergencyPhone",
    ]

    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in Excel: {missing}")

    conn = get_connection()
    cur = conn.cursor()
    cur.fast_executemany = True
    sessions_lookup = fetch_sessions_lookup(conn)

    insert_sql = f"""
    INSERT INTO {TABLE_NAME}
    (
        SessionId,
        ParticipantId,
        SessionName,
        SessionDay,
        SessionDate,
        SessionMonth,
        SessionStartTime,
        SessionEndTime,
        SaheliCardNumber,
        RiskStratification,
        Attended,
        CheckInTime,
        CheckOutTime,
        Notes,
        CreatedAtUtc,
        UpdatedAtUtc,
        AttendanceMemberKind,
        LiteMemberId,
        MemberDisplayId,
        MemberName,
        Phone,
        EmergencyName,
        EmergencyPhone
    )
    VALUES
    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    prepared_rows = []
    rejected_rows = []
    skipped_missing_session_id = 0
    resolved_session_id_from_lookup = 0
    skipped_unmatched_session_lookup = 0
    skipped_invalid_participant_id = 0
    skipped_sql_error = 0
    inserted = 0

    for _, row in df.iterrows():
        row_dict = row.to_dict()

        session_name = clean_value(row["SessionName"])
        session_date = parse_date(row["SessionDate"])
        session_start_time = parse_time(row["SessionStartTime"])
        session_end_time = parse_time(row["SessionEndTime"])

        session_id = parse_int(row["SessionId"])
        if session_id is None:
            skipped_missing_session_id += 1
            session_key = (
                session_name,
                to_date_key(session_date),
                to_time_key(session_start_time),
                to_time_key(session_end_time),
            )
            session_id = sessions_lookup.get(session_key)
            if session_id is None:
                row_dict["Reason"] = "Missing/invalid SessionId and no match found in dbo.Sessions"
                rejected_rows.append(row_dict)
                skipped_unmatched_session_lookup += 1
                continue
            resolved_session_id_from_lookup += 1

        participant_raw = clean_value(row["ParticipantId"])
        participant_id = parse_int(row["ParticipantId"])
        if participant_raw is not None and participant_id is None:
            row_dict["Reason"] = "Invalid ParticipantId"
            rejected_rows.append(row_dict)
            skipped_invalid_participant_id += 1
            continue

        prepared_rows.append((
            (
                session_id,
                participant_id,
                session_name,
                clean_value(row["SessionDay"]),
                session_date,
                clean_value(row["SessionMonth"]),
                session_start_time,
                session_end_time,
                clean_value(row["SaheliCardNumber"]),
                clean_value(row["RiskStratification"]),
                parse_bit(row["Attended"], default=0),
                parse_time(row["CheckInTime"]),
                parse_time(row["CheckOutTime"]),
                clean_value(row["Notes"]),
                parse_datetime(row["CreatedAtUtc"]),
                parse_datetime(row["UpdatedAtUtc"]),
                clean_value(row["AttendanceMemberKind"]) or "FULL",
                clean_value(row["LiteMemberId"]),
                clean_value(row["MemberDisplayId"]),
                clean_value(row["MemberName"]),
                clean_value(row["Phone"]),
                clean_value(row["EmergencyName"]),
                clean_value(row["EmergencyPhone"]),
            ),
            row_dict,
        ))

    try:
        if prepared_rows:
            cur.executemany(insert_sql, [values for values, _ in prepared_rows])
            conn.commit()
            inserted = len(prepared_rows)
    except pyodbc.Error:
        # Fall back to row-by-row insert to isolate bad records and continue.
        conn.rollback()
        for values, row_dict in prepared_rows:
            try:
                cur.execute(insert_sql, values)
                inserted += 1
            except pyodbc.Error as exc:
                bad = row_dict.copy()
                bad["Reason"] = str(exc)
                rejected_rows.append(bad)
                skipped_sql_error += 1
        conn.commit()
    finally:
        cur.close()
        conn.close()

    if rejected_rows:
        pd.DataFrame(rejected_rows).to_excel(INVALID_ROWS_OUTPUT_FILE, index=False)

    print(f"Inserted rows: {inserted}")
    print(f"Skipped rows - missing/invalid SessionId: {skipped_missing_session_id}")
    print(f"Resolved SessionId via lookup: {resolved_session_id_from_lookup}")
    print(f"Skipped rows - no session match for lookup: {skipped_unmatched_session_lookup}")
    print(f"Skipped rows - invalid ParticipantId: {skipped_invalid_participant_id}")
    print(f"Skipped rows - SQL insert errors: {skipped_sql_error}")
    if rejected_rows:
        print(f"Rejected rows exported: {INVALID_ROWS_OUTPUT_FILE}")


if __name__ == "__main__":
    insert_attendance_imports()