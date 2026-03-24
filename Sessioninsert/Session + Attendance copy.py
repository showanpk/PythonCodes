import re
from pathlib import Path
from datetime import datetime, time, timezone
import pandas as pd


# =========================
# CONFIG
# =========================
INPUT_FILE = r"C:\Users\shonk\Downloads\ARCC Activity Register  2026 (1).xlsx"

SESSIONS_OUTPUT_FILE = r"C:\Users\shonk\Downloads\ARCC Activity.xlsx"
ATTENDANCE_OUTPUT_FILE = r"C:\Users\shonk\Downloads\ARCC_SessionAttendance_Export.xlsx"

VENUE_NAME = "Alum Rock Community Centre"
ASSIGNED_STAFF_ID = 17
FREQUENCY = "WEEKLY"
IS_RECURRING_WEEKLY = 0
IS_BOOKING_REQUIRED = 0
IS_CANCELLED = 0
CAPACITY = None
NOTES = None
SUBCATEGORY = None
ARRIVAL_TIME = None

ATTENDED_DEFAULT = 1
NOTES_DEFAULT = None
ATTENDANCE_MEMBER_KIND = "FULL"   # change if DB expects another exact value


# =========================
# SHEET -> SESSION MAPPING
# =========================
SHEET_CONFIG = {
    "Chair Based": {
        "ActivityName": "Chair Based Exercise",
        "Category": "Fitness",
        "ActivityCategory": "Fitness",
        "SessionName": "Chair Based Exercise",
    },
    "Omnia Chair Exercise": {
        "ActivityName": "Omnia Chair Exercise",
        "Category": "Fitness",
        "ActivityCategory": "Fitness",
        "SessionName": "Omnia Chair Exercise",
    },
    "Circuit": {
        "ActivityName": "Circuit Training",
        "Category": "Fitness",
        "ActivityCategory": "Fitness",
        "SessionName": "Circuit Training",
    },
    "Yoga": {
        "ActivityName": "Yoga",
        "Category": "Fitness",
        "ActivityCategory": "Fitness",
        "SessionName": "Yoga",
    },
    "Saheli Social": {
        "ActivityName": "Saheli Social",
        "Category": "Social",
        "ActivityCategory": "Social",
        "SessionName": "Saheli Social",
    },
    "Template": {
        "ActivityName": "Template Activity",
        "Category": "Social",
        "ActivityCategory": "Social",
        "SessionName": "Template Activity",
    },
}


# =========================
# HELPERS
# =========================
def normalize_column_name(name: str) -> str:
    if name is None:
        return ""
    return re.sub(r"[^a-z0-9]+", "", str(name).strip().lower())


def find_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    normalized = {normalize_column_name(col): col for col in df.columns}
    for candidate in candidates:
        key = normalize_column_name(candidate)
        if key in normalized:
            return normalized[key]
    return None


def clean_text(value):
    if pd.isna(value):
        return None
    text = str(value).strip()
    if text == "":
        return None
    if text.upper() in {"#N/A", "N/A", "NA", "NULL", "NONE"}:
        return None
    return text


def excel_date_to_python(value):
    if pd.isna(value):
        return None

    if isinstance(value, pd.Timestamp):
        return value.date()

    if isinstance(value, datetime):
        return value.date()

    text = str(value).strip()
    if not text:
        return None

    date_formats = [
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%Y-%m-%d",
        "%d/%m/%y",
        "%d-%m-%y",
    ]

    for fmt in date_formats:
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            pass

    parsed = pd.to_datetime(text, dayfirst=True, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date()


def standardize_time_token(token: str) -> str:
    token = token.strip().lower()
    # Replace p.m. and a.m. with dots to pm/am
    token = token.replace("p.m.", "pm").replace("a.m.", "am")
    # Replace remaining dots with colons
    token = token.replace(".", ":")
    token = re.sub(r"\s+", "", token)

    if re.fullmatch(r"\d{3,4}(am|pm)?", token):
        suffix = ""
        if token.endswith("am") or token.endswith("pm"):
            suffix = token[-2:]
            token = token[:-2]
        if len(token) == 3:
            token = f"{token[0]}:{token[1:]}"
        elif len(token) == 4:
            token = f"{token[:2]}:{token[2:]}"
        token += suffix

    if re.fullmatch(r"\d{1,2}(am|pm)?", token):
        if token.endswith("am") or token.endswith("pm"):
            suffix = token[-2:]
            num = token[:-2]
            token = f"{num}:00{suffix}"
        else:
            token = f"{token}:00"

    return token


def parse_single_time(token: str) -> time | None:
    if token is None:
        return None

    token = standardize_time_token(token)

    formats = [
        "%H:%M",
        "%I:%M%p",
        "%I%p",
    ]

    for fmt in formats:
        try:
            return datetime.strptime(token, fmt).time()
        except ValueError:
            pass

    return None


def parse_time_range(value):
    text = clean_text(value)
    if not text:
        return None, None

    # Normalize p.m./a.m. format BEFORE splitting
    text = text.replace("p.m.", "pm").replace("P.M.", "PM")
    text = text.replace("a.m.", "am").replace("A.M.", "AM")
    
    text = text.replace("–", "-").replace("—", "-").strip()
    parts = [p.strip() for p in text.split("-") if p.strip()]
    
    # Handle single time (assume 1-hour duration)
    if len(parts) == 1:
        start_t = parse_single_time(parts[0])
        if start_t is None:
            return None, None
        # Add 1 hour for end time
        end_dt = datetime.combine(datetime.today().date(), start_t) + pd.Timedelta(hours=1)
        end_t = end_dt.time()
        return start_t, end_t
    
    if len(parts) != 2:
        return None, None

    left, right = parts[0], parts[1]
    left_lower = left.lower()
    right_lower = right.lower()

    if not re.search(r"(am|pm)$", left_lower) and re.search(r"(am|pm)$", right_lower):
        suffix = right_lower[-2:]
        left = left + suffix

    start_t = parse_single_time(left)
    end_t = parse_single_time(right)

    return start_t, end_t


def format_time_for_excel(t: time | None):
    if t is None:
        return None
    return t.strftime("%H:%M:%S")


def row_has_session_data(row, date_col, time_col):
    date_val = row.get(date_col) if date_col else None
    time_val = row.get(time_col) if time_col else None

    if pd.notna(date_val) and str(date_val).strip() != "":
        return True
    if pd.notna(time_val) and str(time_val).strip() != "":
        return True
    return False


def is_real_card_number(value: str | None) -> bool:
    if not value:
        return False

    txt = str(value).strip()
    if txt.lower() in {"cancelled", "canceled"}:
        return False

    return bool(re.fullmatch(r"\d+", txt))


def is_name_like(value: str | None) -> bool:
    if not value:
        return False

    txt = str(value).strip()
    if txt.lower() in {"cancelled", "canceled"}:
        return False

    return bool(re.search(r"[a-zA-Z]", txt))


# =========================
# BUILD BOTH EXPORTS
# =========================
def build_both_exports(input_file: str, sessions_output_file: str, attendance_output_file: str):
    input_path = Path(input_file)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")

    xls = pd.ExcelFile(input_path)

    session_rows = []
    attendance_rows = []

    sessions_skipped_sheets = []
    sessions_warnings = []

    attendance_skipped_sheets = []
    attendance_warnings = []

    created_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    now_utc = created_at_utc

    for sheet_name in xls.sheet_names:
        df = pd.read_excel(input_path, sheet_name=sheet_name)

        if df.empty:
            continue

        config = SHEET_CONFIG.get(sheet_name)
        if not config:
            sessions_skipped_sheets.append(sheet_name)
            attendance_skipped_sheets.append(sheet_name)
            continue

        # Common column detection
        session_col = find_column(df, ["Session", "Activity", "Class"])
        day_col = find_column(df, ["Day"])
        date_col = find_column(df, ["Date", "SessionDate"])
        month_col = find_column(df, ["Month"])
        time_col = find_column(df, ["Time", "Session Time", "SessionTime"])
        
        if not date_col or not time_col:
            print(f"[WARN] Sheet '{sheet_name}': date_col={date_col}, time_col={time_col}")
            print(f"       Available columns: {list(df.columns[:15])}")
        card_col = find_column(df, ["Saheli Card Number", "SaheliCardNumber", "Card Number"])
        name_col = find_column(df, ["Name", "Member Name", "Participant Name"])
        emergency_name_col = find_column(df, ["Emergency contact Name", "Emergency Name", "EmergencyContactName"])
        emergency_phone_col = find_column(df, ["Emergency Number", "Emergency Phone", "EmergencyPhone"])
        risk_col = find_column(df, ["Risk Stratification", "RiskStratification"])

        # -------------------------
        # Sessions export
        # -------------------------
        if not date_col or not time_col:
            sessions_warnings.append(
                f"Sheet '{sheet_name}' missing required columns. date_col={date_col}, time_col={time_col}"
            )
        else:
            for idx, row in df.iterrows():
                if not row_has_session_data(row, date_col, time_col):
                    continue

                session_date = excel_date_to_python(row.get(date_col))
                start_time, end_time = parse_time_range(row.get(time_col))

                if session_date is None:
                    continue

                if start_time is None or end_time is None:
                    sessions_warnings.append(
                        f"Could not parse time on sheet '{sheet_name}', row {idx + 2}: {row.get(time_col)}"
                    )
                    continue

                session_name_from_row = clean_text(row.get(session_col)) if session_col else None

                session_rows.append({
                    "Frequency": FREQUENCY,
                    "Category": config["Category"],
                    "SubCategory": SUBCATEGORY,
                    "ActivityCategory": config["ActivityCategory"],
                    "VenueName": VENUE_NAME,
                    "ActivityName": config["ActivityName"],
                    "Notes": NOTES,
                    "IsRecurringWeekly": IS_RECURRING_WEEKLY,
                    "DayOfWeek": None,
                    "SessionDate": session_date.strftime("%Y-%m-%d"),
                    "ArrivalTime": ARRIVAL_TIME,
                    "StartTime": format_time_for_excel(start_time),
                    "EndTime": format_time_for_excel(end_time),
                    "Capacity": CAPACITY,
                    "IsBookingRequired": IS_BOOKING_REQUIRED,
                    "IsCancelled": IS_CANCELLED,
                    "CreatedAtUtc": created_at_utc,
                    "AssignedStaffId": ASSIGNED_STAFF_ID,
                    "SourceSheet": sheet_name,
                    "SourceSessionText": session_name_from_row,
                    "SourceTimeText": clean_text(row.get(time_col)),
                })

        # -------------------------
        # Attendance export
        # -------------------------
        if not date_col or not time_col:
            attendance_warnings.append(f"Sheet '{sheet_name}' missing required date/time columns.")
        else:
            for idx, row in df.iterrows():
                session_date = excel_date_to_python(row.get(date_col))
                if session_date is None:
                    continue

                start_time, end_time = parse_time_range(row.get(time_col))
                if start_time is None or end_time is None:
                    attendance_warnings.append(
                        f"Could not parse time on sheet '{sheet_name}', row {idx + 2}: {row.get(time_col)}"
                    )
                    continue

                raw_session_name = clean_text(row.get(session_col)) if session_col else None
                session_day = clean_text(row.get(day_col)) if day_col else None
                session_month = clean_text(row.get(month_col)) if month_col else None
                raw_card = clean_text(row.get(card_col)) if card_col else None
                member_name = clean_text(row.get(name_col)) if name_col else None
                emergency_name = clean_text(row.get(emergency_name_col)) if emergency_name_col else None
                emergency_phone = clean_text(row.get(emergency_phone_col)) if emergency_phone_col else None
                risk = clean_text(row.get(risk_col)) if risk_col else None

                if not any([raw_card, member_name, emergency_name, emergency_phone, risk]):
                    continue

                if raw_card and str(raw_card).strip().lower() in {"cancelled", "canceled"}:
                    continue

                saheli_card_number = raw_card if is_real_card_number(raw_card) else None
                member_display_id = saheli_card_number

                if not saheli_card_number and is_name_like(raw_card) and not member_name:
                    member_name = raw_card

                phone = emergency_phone

                attendance_rows.append({
                    "SessionId": None,
                    "ParticipantId": None,
                    "SessionName": config["SessionName"],
                    "SessionDay": session_day,
                    "SessionDate": session_date.strftime("%Y-%m-%d"),
                    "SessionMonth": session_month if session_month else session_date.strftime("%b"),
                    "SessionStartTime": format_time_for_excel(start_time),
                    "SessionEndTime": format_time_for_excel(end_time),
                    "SaheliCardNumber": saheli_card_number,
                    "RiskStratification": risk,
                    "Attended": ATTENDED_DEFAULT,
                    "CheckInTime": None,
                    "CheckOutTime": None,
                    "Notes": NOTES_DEFAULT,
                    "CreatedAtUtc": now_utc,
                    "UpdatedAtUtc": now_utc,
                    "AttendanceMemberKind": ATTENDANCE_MEMBER_KIND,
                    "LiteMemberId": None,
                    "MemberDisplayId": member_display_id,
                    "MemberName": member_name,
                    "Phone": phone,
                    "EmergencyName": emergency_name,
                    "EmergencyPhone": emergency_phone,
                    "SourceSheet": sheet_name,
                    "RawSessionText": raw_session_name,
                    "RawCardValue": raw_card,
                    "NeedsParticipantLookup": 1 if saheli_card_number is None else 0,
                })

    # =========================
    # FINALIZE SESSIONS
    # =========================
    sessions_df = pd.DataFrame(session_rows)

    if sessions_df.empty:
        print("\n[DEBUG] No session rows extracted. Checking diagnostics:")
        print(f"  Total sheets processed: {len(xls.sheet_names)}")
        print(f"  Configured sheets: {len(SHEET_CONFIG)}")
        print(f"  Skipped sheets: {sessions_skipped_sheets}")
        print(f"  Warnings: {len(sessions_warnings)} total")
        for i, w in enumerate(sessions_warnings[:5], 1):
            print(f"    {i}. {w}")
        if len(sessions_warnings) > 5:
            print(f"    ... and {len(sessions_warnings) - 5} more")
        raise ValueError("No valid session rows were extracted from the workbook.")

    sessions_df = sessions_df.drop_duplicates(
        subset=[
            "ActivityName",
            "SessionDate",
            "StartTime",
            "EndTime",
            "VenueName",
        ]
    ).sort_values(
        by=["SessionDate", "ActivityName", "StartTime"],
        kind="stable"
    ).reset_index(drop=True)

    with pd.ExcelWriter(sessions_output_file, engine="openpyxl") as writer:
        sessions_df.to_excel(writer, sheet_name="SessionsExport", index=False)

        info_rows = []
        for s in sessions_skipped_sheets:
            info_rows.append({"Type": "SkippedSheet", "Message": s})
        for w in sessions_warnings:
            info_rows.append({"Type": "Warning", "Message": w})

        if info_rows:
            pd.DataFrame(info_rows).to_excel(writer, sheet_name="ImportNotes", index=False)

    # =========================
    # FINALIZE ATTENDANCE
    # =========================
    attendance_df = pd.DataFrame(attendance_rows)

    if attendance_df.empty:
        raise ValueError("No attendance rows were extracted from the workbook.")

    attendance_df = attendance_df.drop_duplicates(
        subset=[
            "SessionName",
            "SessionDate",
            "SessionStartTime",
            "SessionEndTime",
            "SaheliCardNumber",
            "MemberName",
            "EmergencyPhone",
        ]
    ).sort_values(
        by=["SessionDate", "SessionName", "MemberName", "SaheliCardNumber"],
        kind="stable",
        na_position="last"
    ).reset_index(drop=True)

    with pd.ExcelWriter(attendance_output_file, engine="openpyxl") as writer:
        attendance_df.to_excel(writer, sheet_name="SessionAttendanceExport", index=False)

        notes_rows = []
        for s in attendance_skipped_sheets:
            notes_rows.append({"Type": "SkippedSheet", "Message": s})
        for w in attendance_warnings:
            notes_rows.append({"Type": "Warning", "Message": w})

        if notes_rows:
            pd.DataFrame(notes_rows).to_excel(writer, sheet_name="ImportNotes", index=False)

    print(f"Done.")
    print(f"Sessions export created: {sessions_output_file}")
    print(f"Attendance export created: {attendance_output_file}")
    print(f"Sessions rows exported: {len(sessions_df)}")
    print(f"Attendance rows exported: {len(attendance_df)}")


if __name__ == "__main__":
    build_both_exports(
        INPUT_FILE,
        SESSIONS_OUTPUT_FILE,
        ATTENDANCE_OUTPUT_FILE
    )