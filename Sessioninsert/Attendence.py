import re
from pathlib import Path
from datetime import datetime
import pandas as pd


MONTH_NAME_TO_NUM = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}


# =========================
# CONFIG
# =========================
INPUT_FILE = r"C:\Users\shonk\Downloads\Innerva Booking Sheet.xlsx"
OUTPUT_FILE = r"C:\Users\shonk\Downloads\Innerva_SessionAttendance_Export.xlsx"

ATTENDED_DEFAULT = 1
NOTES_DEFAULT = None
ATTENDANCE_MEMBER_KIND = "FULL"   # change only if your DB expects another value
INNERVA_SESSION_NAME = "Innerva Sessions"
MIN_SESSION_DATE = datetime(2026, 1, 10).date()


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


def resolve_date_and_time_columns(df: pd.DataFrame) -> tuple[str | None, str | None]:
    date_col = find_column(df, ["Date", "date", "Session Date", "SessionDate"])
    time_col = find_column(df, ["Time", "Session Time", "SessionTime", "Session"])

    session_like_cols = [
        col for col in df.columns
        if normalize_column_name(col).startswith("session")
    ]

    if date_col is None and len(session_like_cols) >= 1:
        date_col = session_like_cols[0]

    if time_col is None and len(session_like_cols) >= 2:
        time_col = session_like_cols[1]

    if date_col is not None and time_col == date_col and len(session_like_cols) >= 2:
        if session_like_cols[0] == date_col:
            time_col = session_like_cols[1]
        else:
            time_col = session_like_cols[0]

    return date_col, time_col


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


def extract_year_from_text(value: str | None) -> int | None:
    if not value:
        return None

    text = str(value)

    # Prefer explicit 4-digit years when available.
    match_4 = re.search(r"\b(20\d{2})\b", text)
    if match_4:
        year = int(match_4.group(1))
        if 1900 <= year <= 2100:
            return year

    # Support sheet labels like "July 25" or "Sept '24".
    match_2 = re.search(r"(?:^|\D)(\d{2})(?:$|\D)", text)
    if match_2:
        yy = int(match_2.group(1))
        if 0 <= yy <= 79:
            return 2000 + yy
        return 1900 + yy

    return None


def extract_month_from_text(value: str | None) -> int | None:
    if not value:
        return None

    text = str(value).strip().lower()
    for month_name, month_num in MONTH_NAME_TO_NUM.items():
        if re.search(rf"\b{month_name}\b", text):
            return month_num
    return None


def infer_year(sheet_name: str, input_file: str) -> int:
    from_sheet = extract_year_from_text(sheet_name)
    if from_sheet:
        return from_sheet

    from_file = extract_year_from_text(Path(input_file).name)
    if from_file:
        return from_file

    return datetime.utcnow().year


def is_attendance_sheet(sheet_name: str) -> bool:
    name = (sheet_name or "").strip().lower()
    if not name:
        return False

    if name == "current":
        return True

    has_month = extract_month_from_text(name) is not None
    has_year = extract_year_from_text(name) is not None
    return has_month or has_year


def parse_session_date(raw_date, raw_month, sheet_name: str, input_file: str):
    date_text = clean_text(raw_date)
    month_text = clean_text(raw_month)

    # Build date explicitly from row day + row month + sheet year.
    day = None
    if date_text:
        day_match = re.search(r"\b(\d{1,2})(?:st|nd|rd|th)?\b", date_text, flags=re.IGNORECASE)
        if day_match:
            day = int(day_match.group(1))
        else:
            parsed_date = excel_date_to_python(raw_date)
            if parsed_date is not None:
                day = parsed_date.day

    month_from_sheet = extract_month_from_text(sheet_name)
    month_from_col = extract_month_from_text(month_text)
    month_from_date_text = extract_month_from_text(date_text)
    month = month_from_col or month_from_date_text or month_from_sheet

    sheet_year = extract_year_from_text(sheet_name)
    year = sheet_year if sheet_year is not None else infer_year(sheet_name, input_file)

    if day is not None and month is not None:
        try:
            return datetime(year, month, day).date()
        except ValueError:
            pass

    return None


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
        "%H:%M:%S",
        "%H:%M",
        "%I:%M:%S%p",
        "%I:%M%p",
        "%I%p",
    ]

    for fmt in formats:
        try:
            return datetime.strptime(token, fmt).time()
        except ValueError:
            pass

    return None


def excel_number_to_time(value: float):
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None

    if numeric < 0:
        return None

    # Excel time can be a pure fraction (0..1) or a serial date/time (>1).
    fraction = numeric % 1
    total_seconds = int(round(fraction * 24 * 60 * 60)) % (24 * 60 * 60)
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    return datetime.strptime(f"{hours:02d}:{minutes:02d}:{seconds:02d}", "%H:%M:%S").time()


def parse_single_time_value(value):
    if value is None or pd.isna(value):
        return None

    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime().time().replace(microsecond=0)

    if isinstance(value, datetime):
        return value.time().replace(microsecond=0)

    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return excel_number_to_time(float(value))

    text = clean_text(value)
    if not text:
        return None

    if re.fullmatch(r"\d+(?:\.\d+)?", text):
        parsed_num_time = excel_number_to_time(float(text))
        if parsed_num_time is not None:
            return parsed_num_time

    parsed_dt = pd.to_datetime(text, errors="coerce")
    if not pd.isna(parsed_dt):
        return parsed_dt.to_pydatetime().time().replace(microsecond=0)

    return parse_single_time(text)


def parse_time_range(value):
    if value is None or pd.isna(value):
        return None, None

    text = clean_text(value)
    if not text:
        return None, None

    lower_text = text.lower()

    # If it is a parseable single timestamp/time (including datetime text), keep it as a single-point time.
    is_explicit_time_range = bool(
        re.search(
            r"(?i)(?:\b\d{1,2}[:.]\d{2}(?::\d{2})?(?:\s*[ap]m)?\b|\b\d{1,2}\s*[ap]m\b)\s*[-–—]\s*(?:\b\d{1,2}[:.]\d{2}(?::\d{2})?(?:\s*[ap]m)?\b|\b\d{1,2}\s*[ap]m\b)",
            text,
        )
        or re.search(r"\s+to\s+", lower_text)
    )
    single_candidate = parse_single_time_value(value)
    if single_candidate is not None and not is_explicit_time_range:
        return single_candidate, single_candidate

    # Single time values (e.g., 11:00:00, 10:30, or Excel numeric time fraction).
    if "-" not in text and "–" not in text and "—" not in text and " to " not in lower_text:
        single_t = parse_single_time_value(value)
        if single_t is not None:
            return single_t, single_t
        return None, None

    text = text.replace("–", "-").replace("—", "-")
    text = re.sub(r"\s+to\s+", "-", text, flags=re.IGNORECASE).strip()
    parts = [p.strip() for p in text.split("-") if p.strip()]
    if len(parts) < 1:
        return None, None

    if len(parts) == 1:
        single_t = parse_single_time_value(parts[0])
        if single_t is not None:
            return single_t, single_t
        return None, None

    if len(parts) != 2:
        return None, None

    left, right = parts[0], parts[1]
    left_lower = left.lower()
    right_lower = right.lower()

    if not re.search(r"(am|pm)$", left_lower) and re.search(r"(am|pm)$", right_lower):
        suffix = right_lower[-2:]
        left = left + suffix

    start_t = parse_single_time_value(left)
    end_t = parse_single_time_value(right)

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

    return bool(re.fullmatch(r"\d+(?:\.0+)?", txt))


def normalize_card_number(value: str | None) -> str | None:
    if not value:
        return None

    txt = str(value).strip()
    if not re.fullmatch(r"\d+(?:\.0+)?", txt):
        return None

    if "." in txt:
        txt = txt.split(".", 1)[0]

    return txt


def is_excluded_induction(value: str | None) -> bool:
    text = clean_text(value)
    if not text:
        return False

    normalized = re.sub(r"[^a-z]", "", text.lower())
    return normalized in {
        "cancelled",
        "canceled",
        "inductioncancelled",
        "cancelledinduction",
        "noinduction",
    }


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
    now_utc = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    for sheet_name in xls.sheet_names:
        if not is_attendance_sheet(sheet_name):
            continue

        df = pd.read_excel(input_file, sheet_name=sheet_name)

        if df.empty:
            continue

        session_col = find_column(df, ["Activity", "Class", "Session Type", "SessionType"])
        day_col = find_column(df, ["Day"])
        date_col, time_col = resolve_date_and_time_columns(df)
        month_col = find_column(df, ["Month"])
        induction_col = find_column(df, ["Induction Time", "InductionTime", "Induction"])
        card_col = find_column(df, ["Saheli Card Number", "SaheliCardNumber", "Card Number", "Saheli Card No", "SaheliCardNo"])
        name_col = find_column(df, ["Name", "Member Name", "Participant Name"])
        emergency_name_col = find_column(df, ["Emergency contact Name", "Emergency Name", "EmergencyContactName"])
        emergency_phone_col = find_column(df, ["Emergency Number", "Emergency Phone", "EmergencyPhone"])
        risk_col = find_column(df, ["Risk Stratification", "RiskStratification"])

        if not date_col or not time_col:
            warnings.append(f"Sheet '{sheet_name}' missing required date/time columns.")
            continue

        # Fill merged-cell session context values down so row-level filtering/parsing stays accurate.
        context_cols = [col for col in [session_col, day_col, date_col, month_col, time_col, induction_col] if col]
        if context_cols:
            df[context_cols] = df[context_cols].ffill()

        for idx, row in df.iterrows():
            raw_date_value = row.get(date_col)
            raw_month_value = row.get(month_col) if month_col else None
            session_date = parse_session_date(raw_date_value, raw_month_value, sheet_name, input_file)
            if session_date is None:
                continue

            if session_date < MIN_SESSION_DATE:
                continue

            start_time, end_time = parse_time_range(row.get(time_col))
            if start_time is None or end_time is None:
                warnings.append(
                    f"Could not parse time on sheet '{sheet_name}', row {idx + 2}: {row.get(time_col)}"
                )
                continue

            if induction_col and is_excluded_induction(row.get(induction_col)):
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
            saheli_card_number = normalize_card_number(raw_card) if is_real_card_number(raw_card) else None
            member_display_id = saheli_card_number

            # If card column contains a name and Name column is blank, use that as member name
            if not saheli_card_number and is_name_like(raw_card) and not member_name:
                member_name = raw_card

            # Use emergency number as phone for now
            phone = emergency_phone

            resolved_session_name = INNERVA_SESSION_NAME

            output_rows.append({
                "SessionId": None,  # fill later by join
                "ParticipantId": None,  # fill later by participant lookup
                "SessionName": resolved_session_name,
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
        for w in warnings:
            notes_rows.append({"Type": "Warning", "Message": w})

        if notes_rows:
            pd.DataFrame(notes_rows).to_excel(writer, sheet_name="ImportNotes", index=False)

    print(f"Done. Export created: {output_file}")
    print(f"Rows exported: {len(result_df)}")

    if warnings:
        print("\nWarnings:")
        for w in warnings[:20]:
            print(f" - {w}")
        if len(warnings) > 20:
            print(f" ... and {len(warnings) - 20} more")


if __name__ == "__main__":
    build_session_attendance_export(INPUT_FILE, OUTPUT_FILE)