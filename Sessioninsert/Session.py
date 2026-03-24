import re
from pathlib import Path
from datetime import datetime, time
import pandas as pd


# =========================
# CONFIG
# =========================
INPUT_FILE = r"C:\Users\shonk\Downloads\Calthorpe Register 2026 (2).xlsx"
OUTPUT_FILE = r"C:\Users\shonk\Downloads\Calthorpe_Sessions_Export.xlsx"

VENUE_NAME = "Calthorpe Wellbeing Hub"
ASSIGNED_STAFF_ID = 10
FREQUENCY = "WEEKLY"
IS_RECURRING_WEEKLY = 0
IS_BOOKING_REQUIRED = 0
IS_CANCELLED = 0
CAPACITY = None
NOTES = None
SUBCATEGORY = None
ARRIVAL_TIME = None


# =========================
# SHEET -> SESSION MAPPING
# =========================
# Change ActivityName / Category / ActivityCategory exactly how you want them in dbo.Sessions
SHEET_CONFIG = {
    "Mens Multisport": {
        "ActivityName": "Mens Multisports",
        "Category": "Fitness",
        "ActivityCategory": "Fitness",
    },
    "Zumba": {
        "ActivityName": "Zumba",
        "Category": "Fitness",
        "ActivityCategory": "Fitness",
    },
    "HIIT": {
        "ActivityName": "HIIT",
        "Category": "Fitness",
        "ActivityCategory": "Fitness",
    },
    "Chair Exercise": {
        "ActivityName": "Chair Exercise",
        "Category": "Fitness",
        "ActivityCategory": "Fitness",
    },
    "Social Knit and Crochet": {
        "ActivityName": "Social Knit and Crochet",
        "Category": "Social",
        "ActivityCategory": "Social",
    },
    "Strength and Stretch": {
        "ActivityName": "Strenth and Stretch",   # kept to match your sample spelling
        "Category": "Fitness",
        "ActivityCategory": "Fitness",
    },
    "ESOL": {
        "ActivityName": "ESOL",
        "Category": "Social",
        "ActivityCategory": "Social",
    },
    "Yoga": {
        "ActivityName": "Yoga",
        "Category": "Fitness",
        "ActivityCategory": "Fitness",
    },
    "Arts and Craft": {
        "ActivityName": "Arts and Craft",
        "Category": "Social",
        "ActivityCategory": "Social",
    },
    "Salsa": {
        "ActivityName": "Salsa/Belly Dancing",
        "Category": "Fitness",
        "ActivityCategory": "Fitness",
    },
    "Circuits Class": {
        "ActivityName": "Circuits Class",
        "Category": "Fitness",
        "ActivityCategory": "Fitness",
    },
    "Body Conditioning": {
        "ActivityName": "Body Conditioning",
        "Category": "Fitness",
        "ActivityCategory": "Fitness",
    },
    "Pilate Floor Base": {
        "ActivityName": "Pilate Floor Base",
        "Category": "Fitness",
        "ActivityCategory": "Fitness",
    },
    "Workshops": {
        "ActivityName": "Workshops",
        "Category": "Social",
        "ActivityCategory": "Social",
    },
    "Tennis": {
        "ActivityName": "Tennis",
        "Category": "Fitness",
        "ActivityCategory": "Fitness",
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


# =========================
# MAIN
# =========================
def build_sessions_export(input_file: str, output_file: str):
    input_path = Path(input_file)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")

    xls = pd.ExcelFile(input_path)
    output_rows = []
    skipped_sheets = []
    warnings = []
    created_at_utc = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    for sheet_name in xls.sheet_names:
        df = pd.read_excel(input_path, sheet_name=sheet_name)

        if df.empty:
            continue

        config = SHEET_CONFIG.get(sheet_name)
        if not config:
            skipped_sheets.append(sheet_name)
            continue

        date_col = find_column(df, ["Date", "SessionDate"])
        time_col = find_column(df, ["Time", "Session Time", "SessionTime"])
        session_col = find_column(df, ["Session", "Activity", "Class"])

        if not date_col or not time_col:
            warnings.append(
                f"Sheet '{sheet_name}' missing required columns. date_col={date_col}, time_col={time_col}"
            )
            continue

        for idx, row in df.iterrows():
            if not row_has_session_data(row, date_col, time_col):
                continue

            session_date = excel_date_to_python(row.get(date_col))
            start_time, end_time = parse_time_range(row.get(time_col))

            if session_date is None:
                continue

            if start_time is None or end_time is None:
                warnings.append(
                    f"Could not parse time on sheet '{sheet_name}', row {idx + 2}: {row.get(time_col)}"
                )
                continue

            session_name_from_row = clean_text(row.get(session_col)) if session_col else None

            output_rows.append({
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

    result_df = pd.DataFrame(output_rows)

    if result_df.empty:
        raise ValueError("No valid session rows were extracted from the workbook.")

    result_df = result_df.drop_duplicates(
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

    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        result_df.to_excel(writer, sheet_name="SessionsExport", index=False)

        info_rows = []
        for s in skipped_sheets:
            info_rows.append({"Type": "SkippedSheet", "Message": s})
        for w in warnings:
            info_rows.append({"Type": "Warning", "Message": w})

        if info_rows:
            pd.DataFrame(info_rows).to_excel(writer, sheet_name="ImportNotes", index=False)

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
    build_sessions_export(INPUT_FILE, OUTPUT_FILE)