from __future__ import annotations

import os
import re
import uuid
from datetime import datetime, date, timedelta, timezone
from pathlib import Path
from typing import Any, Optional, Tuple

import pandas as pd
import pyodbc


# ============================================================
# CONFIG
# ============================================================

INPUT_FILE = Path(r"C:\Users\shonk\Downloads\Tennis Register 2025.xlsx")

# Put your SQL username/password here, or set Windows environment variables:
# setx CRM_SQL_USER "your_sql_username"
# setx CRM_SQL_PASSWORD "your_sql_password"
SQL_USER = os.getenv("CRM_SQL_USER", "sahelihubadmin")
SQL_PASSWORD = os.getenv("CRM_SQL_PASSWORD", "W7WZ7ZaG1YbMZ71gh%2xSFuR")

SQL_CONNECTION_STRING = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=tcp:sahelihub.database.windows.net,1433;"
    "DATABASE=SahelihubCRM;"
    f"UID={SQL_USER};"
    f"PWD={SQL_PASSWORD};"
    "Encrypt=yes;"
    "TrustServerCertificate=yes;"
)

SKIP_SHEETS = {"Sheet1", "Template", "Full Register", "[1]Full Register"}

IMPORT_SOURCE_NAME = "Tennis Register 2025"

REUSE_EXISTING_SESSIONS = True
SKIP_DUPLICATE_ATTENDANCE = True  # RE-ENABLED

DEFAULT_FREQUENCY = "WEEKLY"
DEFAULT_CATEGORY = "Fitness"
DEFAULT_SUBCATEGORY = None
DEFAULT_ACTIVITY_CATEGORY = "Fitness"
DEFAULT_ACTIVITY_NAME = "Tennis"

# Sheet name -> CRM VenueName mapping
VENUE_NAME_MAP = {
    "Calthorpe": "Calthorpe Wellbeing Hub",
    "Cannon Hill": "Cannon Hill Park",
    "Handsworth": "Handsworth",
    "Wardend": "Wardend",
    "Parkfield": "Parkfield",
    "Heathmount": "Heathmount",
    "St Albans": "St Albans",
}

TEXT_NULLS = {"", "#N/A", "N/A", "NA", "NONE", "NULL", "NAN", "(BLANK)", "0"}

NO_ATTENDANCE_WORDS = {
    "no attended",
    "no attendance",
    "no one",
    "none attended",
    "nobody attended",
    "no participant",
    "no participants",
}

CANCELLED_WORDS = {
    "cancelled",
    "canceled",
    "bad weather got cancelled",
}


# ============================================================
# BASIC CLEANING HELPERS
# ============================================================

def clean_value(value: Any) -> Optional[Any]:
    if pd.isna(value):
        return None

    if isinstance(value, str):
        text = value.strip()
        if text.upper() in TEXT_NULLS:
            return None
        return re.sub(r"\s+", " ", text)

    return value


def clean_text(value: Any) -> Optional[str]:
    value = clean_value(value)

    if value is None:
        return None

    if isinstance(value, float) and value.is_integer():
        return str(int(value))

    return str(value).strip()


def normalise_activity(value: Any) -> Optional[str]:
    text = clean_text(value)

    if not text:
        return None

    if text.lower() in {"session", "sessions", "activity"}:
        return None

    return text.strip()


def normalise_venue(sheet_name: str) -> str:
    cleaned = sheet_name.strip()
    return VENUE_NAME_MAP.get(cleaned, cleaned)


def excel_date_to_date(value: Any) -> Optional[date]:
    value = clean_value(value)

    if value is None:
        return None

    if isinstance(value, datetime):
        return value.date()

    if isinstance(value, date):
        return value

    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return (datetime(1899, 12, 30) + timedelta(days=int(value))).date()

    text = str(value).strip().replace("//", "/")
    parsed = pd.to_datetime(text, dayfirst=True, errors="coerce")

    if pd.isna(parsed):
        return None

    return parsed.date()


def month_name(value: Any, fallback_date: Optional[date]) -> Optional[str]:
    text = clean_text(value)

    if text:
        return (
            text
            .replace("Decemeber", "December")
            .replace("Febuary", "February")
        )

    if fallback_date:
        return fallback_date.strftime("%B")

    return None


def normalise_day(value: Any, fallback_date: Optional[date]) -> Optional[str]:
    text = clean_text(value)

    if text:
        fixes = {
            "tuesdsay": "Tuesday",
            "tuesday": "Tuesday",
            "tue": "Tuesday",
            "tues": "Tuesday",
            "thur": "Thursday",
            "thurs": "Thursday",
            "thursday": "Thursday",
            "wed": "Wednesday",
            "wednesday": "Wednesday",
            "fri": "Friday",
            "friday": "Friday",
            "sat": "Saturday",
            "saturday": "Saturday",
            "sun": "Sunday",
            "sunday": "Sunday",
            "mon": "Monday",
            "monday": "Monday",
        }

        key = text.lower().strip()
        return fixes.get(key, text.strip().capitalize())

    if fallback_date:
        return fallback_date.strftime("%A")

    return None


# ============================================================
# TIME PARSING
# ============================================================

def parse_time_token(token: str, fallback_suffix: Optional[str] = None) -> Optional[str]:
    token = token.strip().lower()

    if not token:
        return None

    suffix = None

    if "am" in token:
        suffix = "am"
    elif "pm" in token:
        suffix = "pm"
    elif fallback_suffix in {"am", "pm"}:
        suffix = fallback_suffix

    token = token.replace("am", "").replace("pm", "")
    token = token.replace(".", ":")
    token = token.replace("-", ":")

    # Handles 1230 as 12:30 and 930 as 9:30.
    if ":" not in token and len(token) == 4 and token.isdigit():
        token = f"{token[:2]}:{token[2:]}"
    elif ":" not in token and len(token) == 3 and token.isdigit():
        token = f"{token[0]}:{token[1:]}"

    match = re.match(r"^(\d{1,2})(?::(\d{1,2}))?$", token)

    if not match:
        return None

    hour = int(match.group(1))
    minute = int(match.group(2) or 0)

    if suffix == "pm" and hour != 12:
        hour += 12
    elif suffix == "am" and hour == 12:
        hour = 0

    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        return None

    return f"{hour:02d}:{minute:02d}:00"


def split_time_range(raw_time: Any) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    raw = clean_text(raw_time)

    if not raw:
        return None, None, None

    text = (
        raw.lower()
        .replace("–", "-")
        .replace("—", "-")
        .replace("to", "-")
        .replace(" ", "")
    )

    parts = re.split(r"-", text, maxsplit=1)

    if len(parts) != 2:
        return None, None, raw

    start_token, end_token = parts[0], parts[1]

    start_suffix = "pm" if "pm" in start_token else "am" if "am" in start_token else None
    end_suffix = "pm" if "pm" in end_token else "am" if "am" in end_token else None

    start = parse_time_token(start_token, fallback_suffix=start_suffix or end_suffix)
    end = parse_time_token(end_token, fallback_suffix=end_suffix or start_suffix)

    # Example: "12pm-1" should become 12:00 to 13:00.
    if start and end:
        start_h = int(start.split(":")[0])
        end_h = int(end.split(":")[0])

        if start_h >= 12 and end_h < 12 and not end_suffix:
            end_h += 12
            end_parts = end.split(":")
            end = f"{end_h:02d}:{end_parts[1]}:{end_parts[2]}"

    return start, end, raw


def is_valid_time_range(start_time: Optional[str], end_time: Optional[str]) -> bool:
    if not start_time or not end_time:
        return False

    return end_time > start_time


# ============================================================
# MEMBER / STATUS HELPERS
# ============================================================

def extract_member(raw_card_value: Any, raw_name_value: Any) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    raw_card = clean_text(raw_card_value)
    raw_name = clean_text(raw_name_value)

    card_number = None
    member_name = raw_name

    if raw_card:
        if re.fullmatch(r"\d+(?:\.0)?", raw_card):
            card_number = str(int(float(raw_card)))
        else:
            bracket_match = re.search(r"\((\d+)\)", raw_card)

            if bracket_match:
                card_number = bracket_match.group(1)
                possible_name = re.sub(r"\(\d+\)", "", raw_card).strip()

                if possible_name and not member_name:
                    member_name = possible_name
            elif not member_name:
                # Some Excel rows put the participant name inside the card number column.
                member_name = raw_card

    member_display_id = card_number

    return card_number, member_display_id, member_name


def row_status(raw_card_value: Any, raw_notes_value: Any = None) -> str:
    text = " ".join(filter(None, [clean_text(raw_card_value), clean_text(raw_notes_value)])).lower()

    if any(word in text for word in CANCELLED_WORDS):
        return "cancelled"

    if any(word in text for word in NO_ATTENDANCE_WORDS):
        return "no_attendance"

    return "normal"


def build_extra_notes(
    row: dict,
    source_sheet: str,
    source_row: int,
    raw_time: Optional[str],
    original_card_number: Optional[str],
    attendance_member_kind: Optional[str],
) -> Optional[str]:
    note_parts = [
        f"Imported from {IMPORT_SOURCE_NAME}",
        f"Source sheet: {source_sheet}",
        f"Source row: {source_row}",
    ]

    if raw_time:
        note_parts.append(f"Raw time: {raw_time}")

    if original_card_number and attendance_member_kind == "LITE":
        note_parts.append(f"Original Excel card value: {original_card_number}")

    extras = {
        "Health Condition": row.get("HealthCondition"),
        "DOB": row.get("DOB"),
        "Post Code": row.get("PostCode"),
        "Ethnicity": row.get("Ethnicity"),
        "Sexual Orientation": row.get("SexualOrientation"),
        "New Participant": row.get("NewParticipants"),
    }

    for label, value in extras.items():
        cleaned = clean_text(value)

        if cleaned:
            note_parts.append(f"{label}: {cleaned}")

    return "; ".join(note_parts) if note_parts else None


def get_first_16_columns_as_standard(df: pd.DataFrame) -> pd.DataFrame:
    df = df.iloc[:, :16].copy()

    while df.shape[1] < 16:
        df[df.shape[1]] = None

    df.columns = [
        "Session",
        "Day",
        "Date",
        "Month",
        "Time",
        "SaheliCardNumberRaw",
        "NameRaw",
        "EmergencyName",
        "EmergencyPhone",
        "RiskStratification",
        "HealthCondition",
        "DOB",
        "PostCode",
        "Ethnicity",
        "SexualOrientation",
        "NewParticipants",
    ]

    return df


# ============================================================
# SQL HELPERS
# ============================================================

def is_identity_column(cursor: pyodbc.Cursor, table_name: str, column_name: str) -> bool:
    sql = """
    SELECT c.is_identity
    FROM sys.columns c
    WHERE c.object_id = OBJECT_ID(?)
      AND c.name = ?;
    """

    row = cursor.execute(sql, f"dbo.{table_name}", column_name).fetchone()

    if not row:
        raise RuntimeError(f"Column not found: dbo.{table_name}.{column_name}")

    return bool(row.is_identity)


def get_next_manual_id(cursor: pyodbc.Cursor, table_name: str, column_name: str) -> int:
    sql = f"SELECT ISNULL(MAX({column_name}), 0) + 1 AS NextId FROM dbo.{table_name};"
    row = cursor.execute(sql).fetchone()
    return int(row.NextId)


def get_next_lite_display_number(cursor: pyodbc.Cursor) -> int:
    sql = """
    SELECT ISNULL(MAX(TRY_CONVERT(int, REPLACE(MemberDisplayId, 'LITE-', ''))), 0) + 1 AS NextLiteNumber
    FROM dbo.SessionAttendance
    WHERE AttendanceMemberKind = 'LITE'
      AND MemberDisplayId LIKE 'LITE-%';
    """

    row = cursor.execute(sql).fetchone()
    return int(row.NextLiteNumber)


def find_existing_session(
    cursor: pyodbc.Cursor,
    venue_name: str,
    activity_name: str,
    session_date: date,
    start_time: Optional[str],
    end_time: Optional[str],
) -> Optional[int]:
    sql = """
    SELECT TOP 1 SessionId
    FROM dbo.Sessions
    WHERE LOWER(LTRIM(RTRIM(ISNULL(VenueName, '')))) = LOWER(LTRIM(RTRIM(?)))
      AND LOWER(LTRIM(RTRIM(ISNULL(ActivityName, '')))) = LOWER(LTRIM(RTRIM(?)))
      AND CAST(SessionDate AS date) = CAST(? AS date)
      AND (
            (StartTime IS NULL AND ? IS NULL)
            OR CONVERT(varchar(8), StartTime, 108) = ?
          )
      AND (
            (EndTime IS NULL AND ? IS NULL)
            OR CONVERT(varchar(8), EndTime, 108) = ?
          )
    ORDER BY SessionId;
    """

    row = cursor.execute(
        sql,
        venue_name,
        activity_name,
        session_date.isoformat(),
        start_time,
        start_time,
        end_time,
        end_time,
    ).fetchone()

    if row:
        return int(row.SessionId)

    return None


def create_session(
    cursor: pyodbc.Cursor,
    session_identity: bool,
    manual_session_id: Optional[int],
    venue_name: str,
    activity_name: str,
    session_date: date,
    start_time: Optional[str],
    end_time: Optional[str],
    raw_time: Optional[str],
    is_cancelled: bool,
    created_at: datetime,
) -> int:
    columns = [
        "Frequency",
        "Category",
        "SubCategory",
        "ActivityCategory",
        "VenueName",
        "ActivityName",
        "Notes",
        "IsRecurringWeekly",
        "DayOfWeek",
        "SessionDate",
        "ArrivalTime",
        "StartTime",
        "EndTime",
        "Capacity",
        "IsBookingRequired",
        "IsCancelled",
        "CreatedAtUtc",
        "AssignedStaffId",
        "RecurringSeriesId",
    ]

    values = [
        DEFAULT_FREQUENCY,
        DEFAULT_CATEGORY,
        DEFAULT_SUBCATEGORY,
        DEFAULT_ACTIVITY_CATEGORY,
        venue_name,
        activity_name,
        f"Imported from {IMPORT_SOURCE_NAME}. Source sheet: {venue_name}. Raw time: {raw_time}",
        0,
        None,
        session_date.isoformat(),
        None,
        start_time,
        end_time,
        None,
        0,
        1 if is_cancelled else 0,
        created_at,
        None,
        None,
    ]

    if not session_identity:
        if manual_session_id is None:
            raise RuntimeError("manual_session_id is required because SessionId is not an IDENTITY column.")

        columns.insert(0, "SessionId")
        values.insert(0, manual_session_id)

    column_sql = ", ".join(f"[{c}]" for c in columns)
    placeholder_sql = ", ".join("?" for _ in columns)

    sql = f"""
    INSERT INTO dbo.Sessions ({column_sql})
    OUTPUT INSERTED.SessionId
    VALUES ({placeholder_sql});
    """

    try:
        row = cursor.execute(sql, values).fetchone()
    except Exception:
        print("Error inserting session:")
        print(f"VenueName: {venue_name}")
        print(f"ActivityName: {activity_name}")
        print(f"SessionDate: {session_date}")
        print(f"StartTime: {start_time}")
        print(f"EndTime: {end_time}")
        print(f"Values: {values}")
        raise

    if not row:
        raise RuntimeError("Session insert failed. No SessionId was returned.")

    return int(row.SessionId)


def update_session_cancelled(cursor: pyodbc.Cursor, session_id: int) -> None:
    sql = """
    UPDATE dbo.Sessions
    SET IsCancelled = 1
    WHERE SessionId = ?;
    """

    cursor.execute(sql, session_id)


def find_existing_full_member_from_attendance(
    cursor: pyodbc.Cursor,
    saheli_card_number: Optional[str],
) -> Optional[dict]:
    if not saheli_card_number:
        return None

    sql = """
    SELECT TOP 1
        ParticipantId,
        SaheliCardNumber,
        MemberDisplayId,
        MemberName,
        Phone,
        EmergencyName,
        EmergencyPhone,
        AttendanceMemberKind
    FROM dbo.SessionAttendance
    WHERE SaheliCardNumber = ?
      AND AttendanceMemberKind = 'FULL'
    ORDER BY
        CASE WHEN ParticipantId IS NOT NULL THEN 0 ELSE 1 END,
        UpdatedAtUtc DESC,
        CreatedAtUtc DESC,
        AttendanceId DESC;
    """

    row = cursor.execute(sql, saheli_card_number).fetchone()

    if not row:
        return None

    return {
        "ParticipantId": row.ParticipantId,
        "SaheliCardNumber": row.SaheliCardNumber,
        "MemberDisplayId": row.MemberDisplayId,
        "MemberName": row.MemberName,
        "Phone": row.Phone,
        "EmergencyName": row.EmergencyName,
        "EmergencyPhone": row.EmergencyPhone,
        "AttendanceMemberKind": row.AttendanceMemberKind or "FULL",
    }


def find_existing_lite_member(
    cursor: pyodbc.Cursor,
    member_name: Optional[str],
    emergency_phone: Optional[str],
) -> Optional[dict]:
    if not member_name:
        return None

    if emergency_phone:
        sql = """
        SELECT TOP 1
            LiteMemberId,
            MemberDisplayId,
            MemberName,
            Phone,
            EmergencyName,
            EmergencyPhone
        FROM dbo.SessionAttendance
        WHERE AttendanceMemberKind = 'LITE'
          AND LiteMemberId IS NOT NULL
          AND LOWER(LTRIM(RTRIM(ISNULL(MemberName, '')))) = LOWER(LTRIM(RTRIM(?)))
          AND LTRIM(RTRIM(ISNULL(EmergencyPhone, ''))) = LTRIM(RTRIM(?))
        ORDER BY UpdatedAtUtc DESC, CreatedAtUtc DESC, AttendanceId DESC;
        """

        row = cursor.execute(sql, member_name, emergency_phone).fetchone()

        if row:
            return {
                "LiteMemberId": row.LiteMemberId,
                "MemberDisplayId": row.MemberDisplayId,
                "MemberName": row.MemberName,
                "Phone": row.Phone,
                "EmergencyName": row.EmergencyName,
                "EmergencyPhone": row.EmergencyPhone,
            }

    sql = """
    SELECT TOP 1
        LiteMemberId,
        MemberDisplayId,
        MemberName,
        Phone,
        EmergencyName,
        EmergencyPhone
    FROM dbo.SessionAttendance
    WHERE AttendanceMemberKind = 'LITE'
      AND LiteMemberId IS NOT NULL
      AND LOWER(LTRIM(RTRIM(ISNULL(MemberName, '')))) = LOWER(LTRIM(RTRIM(?)))
    ORDER BY UpdatedAtUtc DESC, CreatedAtUtc DESC, AttendanceId DESC;
    """

    row = cursor.execute(sql, member_name).fetchone()

    if not row:
        return None

    return {
        "LiteMemberId": row.LiteMemberId,
        "MemberDisplayId": row.MemberDisplayId,
        "MemberName": row.MemberName,
        "Phone": row.Phone,
        "EmergencyName": row.EmergencyName,
        "EmergencyPhone": row.EmergencyPhone,
    }


def attendance_exists(
    cursor: pyodbc.Cursor,
    session_id: int,
    saheli_card_number: Optional[str],
    member_name: Optional[str],
    lite_member_id: Optional[str],
    member_display_id: Optional[str],
) -> bool:
    """Check if this exact person has already attended this session.
    
    Uses stable identifiers (card number or lite_member_id) only.
    Does NOT use member_name alone to avoid false positives with same-name attendees.
    """
    # If we have a card number, check if this FULL member already attended
    if saheli_card_number:
        sql = """
        SELECT TOP 1 AttendanceId
        FROM dbo.SessionAttendance
        WHERE SessionId = ?
          AND SaheliCardNumber = ?
          AND AttendanceMemberKind = 'FULL';
        """
        row = cursor.execute(sql, session_id, saheli_card_number).fetchone()
        if row:
            return True
    
    # If we have a lite_member_id, check if this LITE member already attended
    if lite_member_id:
        sql = """
        SELECT TOP 1 AttendanceId
        FROM dbo.SessionAttendance
        WHERE SessionId = ?
          AND LiteMemberId = ?
          AND AttendanceMemberKind = 'LITE';
        """
        row = cursor.execute(sql, session_id, lite_member_id).fetchone()
        if row:
            return True
    
    # Otherwise, no duplicate found
    return False


def create_attendance(
    cursor: pyodbc.Cursor,
    attendance_identity: bool,
    manual_attendance_id: Optional[int],
    session_id: int,
    participant_id: Optional[int],
    activity_name: str,
    day_of_week: Optional[str],
    session_date: date,
    session_month: Optional[str],
    start_time: Optional[str],
    end_time: Optional[str],
    saheli_card_number: Optional[str],
    risk_stratification: Optional[str],
    notes: Optional[str],
    created_at: datetime,
    attendance_member_kind: str,
    lite_member_id: Optional[str],
    member_display_id: Optional[str],
    member_name: Optional[str],
    phone: Optional[str],
    emergency_name: Optional[str],
    emergency_phone: Optional[str],
) -> int:
    columns = [
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
        "AF",
        "BP",
        "HeightCm",
        "WeightKg",
    ]

    values = [
        session_id,
        participant_id,
        activity_name,
        day_of_week,
        session_date.isoformat(),
        session_month,
        start_time,
        end_time,
        saheli_card_number,
        risk_stratification,
        1,
        start_time,
        end_time,
        notes,
        created_at,
        created_at,
        attendance_member_kind,
        lite_member_id,
        member_display_id,
        member_name,
        phone,
        emergency_name,
        emergency_phone,
        None,
        None,
        None,
        None,
    ]

    if not attendance_identity:
        if manual_attendance_id is None:
            raise RuntimeError("manual_attendance_id is required because AttendanceId is not an IDENTITY column.")

        columns.insert(0, "AttendanceId")
        values.insert(0, manual_attendance_id)

    column_sql = ", ".join(f"[{c}]" for c in columns)
    placeholder_sql = ", ".join("?" for _ in columns)

    sql = f"""
    INSERT INTO dbo.SessionAttendance ({column_sql})
    OUTPUT INSERTED.AttendanceId
    VALUES ({placeholder_sql});
    """

    try:
        row = cursor.execute(sql, values).fetchone()
    except Exception:
        print("Error inserting attendance:")
        print(f"SessionId: {session_id}")
        print(f"ParticipantId: {participant_id}")
        print(f"AttendanceMemberKind: {attendance_member_kind}")
        print(f"LiteMemberId: {lite_member_id}")
        print(f"SaheliCardNumber: {saheli_card_number}")
        print(f"MemberDisplayId: {member_display_id}")
        print(f"MemberName: {member_name}")
        print(f"Values: {values}")
        raise

    if not row:
        raise RuntimeError("Attendance insert failed. No AttendanceId was returned.")

    return int(row.AttendanceId)


# ============================================================
# MAIN IMPORT
# ============================================================

def main() -> None:
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Input file not found: {INPUT_FILE}")

    if SQL_USER == "PUT_YOUR_SQL_USERNAME_HERE" or SQL_PASSWORD == "PUT_YOUR_SQL_PASSWORD_HERE":
        raise RuntimeError(
            "Please set SQL_USER and SQL_PASSWORD in the script, "
            "or set CRM_SQL_USER and CRM_SQL_PASSWORD environment variables."
        )

    created_at = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)

    excel = pd.ExcelFile(INPUT_FILE)

    sessions_created = 0
    sessions_reused = 0
    sessions_cancelled = 0
    attendance_created = 0
    attendance_skipped_duplicate = 0
    attendance_failed = 0
    rows_skipped = 0
    invalid_time_rows = 0
    lite_created = 0
    lite_reused = 0
    full_reused = 0
    
    # Detailed skip counters
    skip_no_core_fields = 0
    skip_not_tennis = 0
    skip_no_member = 0
    skip_cancelled_no_attendance = 0
    skip_lite_no_name = 0

    session_id_cache: dict[tuple, int] = {}
    lite_member_cache: dict[str, dict] = {}
    
    # Track all skipped rows for debugging
    skipped_rows_log: list[tuple] = []

    with pyodbc.connect(SQL_CONNECTION_STRING) as conn:
        conn.autocommit = False
        cursor = conn.cursor()

        try:
            session_identity = is_identity_column(cursor, "Sessions", "SessionId")
            attendance_identity = is_identity_column(cursor, "SessionAttendance", "AttendanceId")

            next_manual_session_id = None
            next_manual_attendance_id = None

            if not session_identity:
                next_manual_session_id = get_next_manual_id(cursor, "Sessions", "SessionId")
                print(f"SessionId is not IDENTITY. Starting manual SessionId from {next_manual_session_id}")
            else:
                print("SessionId is IDENTITY. SQL Server will generate SessionId.")

            if not attendance_identity:
                next_manual_attendance_id = get_next_manual_id(cursor, "SessionAttendance", "AttendanceId")
                print(f"AttendanceId is not IDENTITY. Starting manual AttendanceId from {next_manual_attendance_id}")
            else:
                print("AttendanceId is IDENTITY. SQL Server will generate AttendanceId.")

            next_lite_number = get_next_lite_display_number(cursor)
            print(f"Next LITE MemberDisplayId starts from: LITE-{next_lite_number}")

            for sheet_name in excel.sheet_names:
                if sheet_name.strip() in SKIP_SHEETS:
                    continue

                source_sheet_name = sheet_name.strip()
                venue_name = normalise_venue(source_sheet_name)

                print(f"Processing sheet: {source_sheet_name} -> VenueName: {venue_name}")

                raw_df = pd.read_excel(
                    INPUT_FILE,
                    sheet_name=sheet_name,
                    header=0,
                    dtype=object,
                )

                if raw_df.empty:
                    continue

                df = get_first_16_columns_as_standard(raw_df)

                for idx, row_series in df.iterrows():
                    source_row = int(idx) + 2
                    row = row_series.to_dict()

                    activity_name = normalise_activity(row.get("Session")) or DEFAULT_ACTIVITY_NAME
                    session_date = excel_date_to_date(row.get("Date"))
                    raw_time = clean_text(row.get("Time"))

                    start_time, end_time, raw_time_for_notes = split_time_range(raw_time)
                    day_of_week = normalise_day(row.get("Day"), session_date)
                    session_month = month_name(row.get("Month"), session_date)

                    # Be more lenient with missing fields - try harder to extract data
                    # Session date is required for attendance
                    if not session_date:
                        skip_no_core_fields += 1
                        rows_skipped += 1
                        continue

                    # Time is required for session creation, but activity name has default
                    if not raw_time:
                        skip_no_core_fields += 1
                        rows_skipped += 1
                        continue

                    if activity_name.strip().lower() != "tennis":
                        skip_not_tennis += 1
                        rows_skipped += 1
                        continue

                    if not is_valid_time_range(start_time, end_time):
                        invalid_time_rows += 1
                        rows_skipped += 1
                        continue

                    status = row_status(row.get("SaheliCardNumberRaw"))
                    is_cancelled = status == "cancelled"
                    
                    # Count cancelled/no_attendance early
                    if status in {"cancelled", "no_attendance"}:
                        skip_cancelled_no_attendance += 1

                    session_key = (
                        venue_name.lower(),
                        activity_name.lower().strip(),
                        session_date.isoformat(),
                        start_time or "",
                        end_time or "",
                    )

                    if session_key in session_id_cache:
                        session_id = session_id_cache[session_key]
                    else:
                        existing_session_id = None

                        if REUSE_EXISTING_SESSIONS:
                            existing_session_id = find_existing_session(
                                cursor=cursor,
                                venue_name=venue_name,
                                activity_name=activity_name,
                                session_date=session_date,
                                start_time=start_time,
                                end_time=end_time,
                            )

                        if existing_session_id:
                            session_id = existing_session_id
                            session_id_cache[session_key] = session_id
                            sessions_reused += 1

                            if is_cancelled:
                                update_session_cancelled(cursor, session_id)
                                sessions_cancelled += 1
                        else:
                            manual_session_id_to_use = None

                            if not session_identity:
                                manual_session_id_to_use = next_manual_session_id
                                next_manual_session_id += 1

                            session_id = create_session(
                                cursor=cursor,
                                session_identity=session_identity,
                                manual_session_id=manual_session_id_to_use,
                                venue_name=venue_name,
                                activity_name=activity_name,
                                session_date=session_date,
                                start_time=start_time,
                                end_time=end_time,
                                raw_time=raw_time,
                                is_cancelled=is_cancelled,
                                created_at=created_at,
                            )

                            session_id_cache[session_key] = session_id
                            sessions_created += 1

                            if is_cancelled:
                                sessions_cancelled += 1

                    # Cancelled / no-attendance rows should only create or update the session.
                    if status in {"cancelled", "no_attendance"}:
                        continue

                    original_card_value = clean_text(row.get("SaheliCardNumberRaw"))

                    card_number, member_display_id, member_name = extract_member(
                        row.get("SaheliCardNumberRaw"),
                        row.get("NameRaw"),
                    )

                    # If we have a card number but no name, try to look up the FULL member
                    # to get their name. If not found, use the card number as a placeholder.
                    if card_number and not member_name:
                        # Check if this is an existing FULL member
                        existing_full = find_existing_full_member_from_attendance(
                            cursor=cursor,
                            saheli_card_number=card_number,
                        )
                        
                        if existing_full:
                            # Use the existing member's name
                            member_name = existing_full.get("MemberName") or f"Member-{card_number}"
                        else:
                            # Create a LITE member with card number as name
                            member_name = f"Card-{card_number}"

                    if not card_number and not member_name:
                        # For rows with no member data at all, still try to create a generic attendance
                        # with a generated LITE member if we have a session
                        member_name = f"Participant-{source_row}"  # Generate placeholder name
                        card_number = None
                        member_display_id = f"GEN-{source_row}"  # Generate display ID for unknown participants

                    emergency_name_from_excel = clean_text(row.get("EmergencyName"))
                    emergency_phone_from_excel = clean_text(row.get("EmergencyPhone"))

                    participant_id = None
                    phone = None
                    lite_member_id = None
                    attendance_member_kind = "LITE"
                    saheli_card_number_to_insert = None

                    existing_full_member = find_existing_full_member_from_attendance(
                        cursor=cursor,
                        saheli_card_number=card_number,
                    )

                    # ========================================================
                    # CASE 1: Existing FULL member
                    # ========================================================
                    if existing_full_member:
                        attendance_member_kind = "FULL"
                        full_reused += 1

                        participant_id = existing_full_member.get("ParticipantId")
                        lite_member_id = None
                        saheli_card_number_to_insert = card_number

                        member_display_id = existing_full_member.get("MemberDisplayId") or card_number
                        member_name = member_name or existing_full_member.get("MemberName")
                        phone = existing_full_member.get("Phone")

                        emergency_name = emergency_name_from_excel or existing_full_member.get("EmergencyName")
                        emergency_phone = emergency_phone_from_excel or existing_full_member.get("EmergencyPhone")

                    # ========================================================
                    # CASE 2: LITE member
                    # ========================================================
                    else:
                        attendance_member_kind = "LITE"
                        participant_id = None
                        saheli_card_number_to_insert = None

                        # LITE needs a real name.
                        # If the Excel only gives a card number and no name, skip safely.
                        if not member_name:
                            skip_lite_no_name += 1
                            rows_skipped += 1
                            continue

                        # Generate display ID if we don't have a card number
                        if not member_display_id:
                            member_display_id = f"LITE-{next_lite_number}"
                            next_lite_number += 1

                        lite_cache_key = (
                            f"{member_name.strip().lower()}|"
                            f"{(emergency_phone_from_excel or '').strip()}"
                        )

                        if lite_cache_key in lite_member_cache:
                            existing_lite = lite_member_cache[lite_cache_key]
                            lite_reused += 1
                        else:
                            existing_lite = find_existing_lite_member(
                                cursor=cursor,
                                member_name=member_name,
                                emergency_phone=emergency_phone_from_excel,
                            )

                            if existing_lite:
                                lite_reused += 1
                            else:
                                existing_lite = {
                                    "LiteMemberId": str(uuid.uuid4()).upper(),
                                    "MemberDisplayId": f"LITE-{next_lite_number}",
                                    "MemberName": member_name,
                                    "Phone": None,
                                    "EmergencyName": emergency_name_from_excel,
                                    "EmergencyPhone": emergency_phone_from_excel,
                                }

                                next_lite_number += 1
                                lite_created += 1

                            lite_member_cache[lite_cache_key] = existing_lite

                        lite_member_id = existing_lite.get("LiteMemberId")
                        member_display_id = existing_lite.get("MemberDisplayId")
                        member_name = member_name or existing_lite.get("MemberName")
                        phone = existing_lite.get("Phone")
                        emergency_name = emergency_name_from_excel or existing_lite.get("EmergencyName")
                        emergency_phone = emergency_phone_from_excel or existing_lite.get("EmergencyPhone")

                    # Final safety checks.
                    if attendance_member_kind == "LITE":
                        if not lite_member_id or not member_display_id or not member_name:
                            print(
                                f"Skipping LITE attendance. Missing required LITE fields. "
                                f"Sheet={source_sheet_name}, Row={source_row}, "
                                f"LiteMemberId={lite_member_id}, "
                                f"DisplayId={member_display_id}, Name={member_name}"
                            )
                            rows_skipped += 1
                            continue

                    if attendance_member_kind == "FULL":
                        if not member_display_id:
                            member_display_id = card_number

                    # Duplicate check after we know whether it is FULL or LITE.
                    if SKIP_DUPLICATE_ATTENDANCE and attendance_exists(
                        cursor=cursor,
                        session_id=session_id,
                        saheli_card_number=saheli_card_number_to_insert,
                        member_name=member_name,
                        lite_member_id=lite_member_id,
                        member_display_id=member_display_id,
                    ):
                        attendance_skipped_duplicate += 1
                        continue

                    notes = build_extra_notes(
                        row=row,
                        source_sheet=source_sheet_name,
                        source_row=source_row,
                        raw_time=raw_time_for_notes,
                        original_card_number=original_card_value,
                        attendance_member_kind=attendance_member_kind,
                    )

                    manual_attendance_id_to_use = None

                    if not attendance_identity:
                        manual_attendance_id_to_use = next_manual_attendance_id
                        next_manual_attendance_id += 1

                    try:
                        create_attendance(
                            cursor=cursor,
                            attendance_identity=attendance_identity,
                            manual_attendance_id=manual_attendance_id_to_use,
                            session_id=session_id,
                            participant_id=participant_id,
                            activity_name=activity_name,
                            day_of_week=day_of_week,
                            session_date=session_date,
                            session_month=session_month,
                            start_time=start_time,
                            end_time=end_time,
                            saheli_card_number=saheli_card_number_to_insert,
                            risk_stratification=clean_text(row.get("RiskStratification")),
                            notes=notes,
                            created_at=created_at,
                            attendance_member_kind=attendance_member_kind,
                            lite_member_id=lite_member_id,
                            member_display_id=member_display_id,
                            member_name=member_name,
                            phone=phone,
                            emergency_name=emergency_name,
                            emergency_phone=emergency_phone,
                        )

                        attendance_created += 1

                    except Exception as e:
                        attendance_failed += 1
                        print(
                            f"Warning: Could not insert attendance. "
                            f"Sheet={source_sheet_name}, Row={source_row}, "
                            f"SessionId={session_id}, Kind={attendance_member_kind}, "
                            f"Card={card_number}, LiteMemberId={lite_member_id}, "
                            f"DisplayId={member_display_id}, Name={member_name}, Error={e}"
                        )
                        continue

            conn.commit()

        except Exception:
            conn.rollback()
            raise

    print()
    print("Import completed successfully.")
    print("====================================")
    print()
    print("SESSIONS")
    print("-" * 40)
    print(f"Sessions created: {sessions_created}")
    print(f"Sessions reused: {sessions_reused}")
    print(f"Cancelled sessions marked: {sessions_cancelled}")
    print()
    print("ATTENDANCE")
    print("-" * 40)
    print(f"Attendance rows created: {attendance_created}")
    print(f"Duplicate attendance rows skipped: {attendance_skipped_duplicate}")
    print(f"Attendance rows failed: {attendance_failed}")
    print()
    print("MEMBER KINDS")
    print("-" * 40)
    print(f"FULL members reused: {full_reused}")
    print(f"LITE members created: {lite_created}")
    print(f"LITE members reused: {lite_reused}")
    print()
    print(f"ROWS SKIPPED (Total: {rows_skipped})")
    print("-" * 40)
    print(f"Missing core fields (activity/date/time): {skip_no_core_fields}")
    print(f"Not Tennis activity: {skip_not_tennis}")
    print(f"Invalid time ranges: {invalid_time_rows}")
    print(f"Cancelled or no attendance status: {skip_cancelled_no_attendance}")
    print(f"No member data (card or name): {skip_no_member}")
    print(f"LITE with no name: {skip_lite_no_name}")


if __name__ == "__main__":
    main()