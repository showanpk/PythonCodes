import re
from pathlib import Path
from datetime import datetime
import pandas as pd


# =========================
# CONFIG
# =========================
INPUT_FILE = r"C:\Users\shonk\Downloads\Calthorpe Register 2026 (2).xlsx"
OUTPUT_FILE = r"C:\Users\shonk\Downloads\Calthorpe_SessionAttendance_Export.xlsx"

ATTENDED_DEFAULT = 1
NOTES_DEFAULT = None
ATTENDANCE_MEMBER_KIND = "Participant"   # change only if your DB expects another value


# =========================
# SHEET -> SESSION MAPPING
# =========================
SHEET_CONFIG = {
    "Mens Multisport": {"SessionName": "Mens Multisports"},
    "Zumba": {"SessionName": "Zumba"},
    "HIIT": {"SessionName": "HIIT"},
    "Chair Exercise": {"SessionName": "Chair Exercise"},
    "Social Knit and Crochet": {"SessionName": "Social Knit and Crochet"},
    "Strength and Stretch": {"SessionName": "Strenth and Stretch"},
    "ESOL": {"SessionName": "ESOL"},
    "Yoga": {"SessionName": "Yoga"},
    "Arts and Craft": {"SessionName": "Arts and Craft"},
    "Salsa": {"SessionName": "Salsa/Belly Dancing"},
    "Circuits Class": {"SessionName": "Circuits Class"},
    "Body Conditioning": {"SessionName": "Body Conditioning"},
    "Pilate Floor Base": {"SessionName": "Pilate Floor Base"},
    "Workshops": {"SessionName": "Workshops"},
    "Tennis": {"SessionName": "Tennis"},
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


def parse_single_time(token: str):
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

    text = text.replace("–", "-").replace("—", "-").strip()
    parts = [p.strip() for p in text.split("-") if p.strip()]
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


def format_time_for_excel(t):
    if t is None:
        return None
    return t.strftime("%H:%M:%S")


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
# MAIN
# =========================
def build_session_attendance_export(input_file: str, output_file: str):
    input_path = Path(input_file)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")

    xls = pd.ExcelFile(input_path)
    output_rows = []
    warnings = []
    skipped_sheets = []

    now_utc = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    for sheet_name in xls.sheet_names:
        df = pd.read_excel(input_file, sheet_name=sheet_name)

        if df.empty:
            continue

        config = SHEET_CONFIG.get(sheet_name)
        if not config:
            skipped_sheets.append(sheet_name)
            continue

        session_col = find_column(df, ["Session", "Activity", "Class"])
        day_col = find_column(df, ["Day"])
        date_col = find_column(df, ["Date", "SessionDate"])
        month_col = find_column(df, ["Month"])
        time_col = find_column(df, ["Time", "Session Time", "SessionTime"])
        card_col = find_column(df, ["Saheli Card Number", "SaheliCardNumber", "Card Number"])
        name_col = find_column(df, ["Name", "Member Name", "Participant Name"])
        emergency_name_col = find_column(df, ["Emergency contact Name", "Emergency Name", "EmergencyContactName"])
        emergency_phone_col = find_column(df, ["Emergency Number", "Emergency Phone", "EmergencyPhone"])
        risk_col = find_column(df, ["Risk Stratification", "RiskStratification"])

        if not date_col or not time_col:
            warnings.append(f"Sheet '{sheet_name}' missing required date/time columns.")
            continue

        for idx, row in df.iterrows():
            session_date = excel_date_to_python(row.get(date_col))
            if session_date is None:
                continue

            start_time, end_time = parse_time_range(row.get(time_col))
            if start_time is None or end_time is None:
                warnings.append(
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

            # Skip pure empty rows
            if not any([raw_card, member_name, emergency_name, emergency_phone, risk]):
                continue

            # Skip cancelled rows
            if raw_card and str(raw_card).strip().lower() in {"cancelled", "canceled"}:
                continue

            # If card column contains a real card number, use it
            saheli_card_number = raw_card if is_real_card_number(raw_card) else None
            member_display_id = saheli_card_number

            # If card column contains a name and Name column is blank, use that as member name
            if not saheli_card_number and is_name_like(raw_card) and not member_name:
                member_name = raw_card

            # Use emergency number as phone for now
            phone = emergency_phone

            output_rows.append({
                "SessionId": None,  # fill later by join
                "ParticipantId": None,  # fill later by participant lookup
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

                # helper columns for review
                "SourceSheet": sheet_name,
                "RawSessionText": raw_session_name,
                "RawCardValue": raw_card,
                "NeedsParticipantLookup": 1 if saheli_card_number is None else 0,
            })

    result_df = pd.DataFrame(output_rows)

    if result_df.empty:
        raise ValueError("No attendance rows were extracted from the workbook.")

    # Remove exact duplicate attendance rows
    result_df = result_df.drop_duplicates(
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

    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        result_df.to_excel(writer, sheet_name="SessionAttendanceExport", index=False)

        notes_rows = []
        for s in skipped_sheets:
            notes_rows.append({"Type": "SkippedSheet", "Message": s})
        for w in warnings:
            notes_rows.append({"Type": "Warning", "Message": w})

        if notes_rows:
            pd.DataFrame(notes_rows).to_excel(writer, sheet_name="ImportNotes", index=False)

    print(f"Done. Export created: {output_file}")
    print(f"Rows exported: {len(result_df)}")

    if skipped_sheets:
        print("\nSkipped sheets:")
        for s in skipped_sheets:
            print(f" - {s}")

    if warnings:
        print("\nWarnings:")
        for w in warnings[:20]:
            print(f" - {w}")
        if len(warnings) > 20:
            print(f" ... and {len(warnings) - 20} more")


if __name__ == "__main__":
    build_session_attendance_export(INPUT_FILE, OUTPUT_FILE)