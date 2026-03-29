import re
import sys
import uuid
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Optional, Dict, Tuple

import pandas as pd
import pyodbc

# =========================
# CONFIG
# =========================
INPUT_FILE = r"C:\Users\shonk\Downloads\funding_projects_import_ready.csv"
SHEET_NAME = None  # use None for CSV, or set sheet name for Excel
SERVER = r"20.68.160.100,1433"
DATABASE = r"SahelihubCRM"
USE_TRUSTED_CONNECTION = False
USERNAME = "saheli_app"
PASSWORD = "309183"

TABLE_NAME = "dbo.FundingProjects"
CREATE_MISSING_EMAIL_COLUMN = True
SKIP_DUPLICATES_BY_FUNDER_PROJECT = True
WRITE_ERROR_FILES = True
DRY_RUN = False  # True = validate only, no insert
USE_FAST_EXECUTEMANY = False  # Disabled: pyodbc fast_executemany has buffer issues with long strings
TRUNCATE_OVERLONG_VALUES = False  # Only truncate if explicitly enabled; default is to skip/error

OUTPUT_FOLDER = Path(r"C:\Users\shonk\Downloads\funding_projects_import_output")
OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)

# =========================
# EXPECTED COLUMNS
# =========================
EXPECTED_COLUMNS = [
    "funderProject",
    "colorHex",
    "fundingManagementLead",
    "responsibleForReport",
    "responsibleForReportEmails",
    "strategicObjectives",
    "valueGBP",
    "startDate",
    "endDate",
    "siteArea",
    "targets",
    "reportingEvaluation",
    "deadlines",
    "status",
    "commentary",
    "link",
    "comments",
    "projectTracker",
    "deadlineTracker",
]

REQUIRED_COLUMNS = [
    "funderProject",
    "fundingManagementLead",
    "responsibleForReport",
    "startDate",
    "endDate",
    "siteArea",
    "status",
]

HEX_COLOR_RE = re.compile(r"^#(?:[0-9A-Fa-f]{6})$")
EMAIL_RE = re.compile(
    r"^[a-z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?"
    r"(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)*$",
    re.IGNORECASE,
)


# =========================
# HELPERS
# =========================
def clean_text(value: object) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null"}:
        return None
    return text


def clean_currency(value: object) -> Optional[Decimal]:
    text = clean_text(value)
    if text is None:
        return None

    # keep digits, minus, decimal point
    text = text.replace(",", "").replace("£", "")
    text = re.sub(r"[^0-9.\-]", "", text)

    if not text:
        return None

    try:
        return Decimal(text)
    except InvalidOperation:
        return None


def clean_date(value: object) -> Optional[pd.Timestamp]:
    """Parse date safely, avoiding dayfirst ambiguity for YYYY-MM-DD format."""
    text = clean_text(value)
    if text is None:
        return None

    # Check if already in YYYY-MM-DD format to avoid dayfirst warning
    if re.match(r'^\d{4}-\d{2}-\d{2}', text):
        parsed = pd.to_datetime(text, format='%Y-%m-%d', errors='coerce')
    else:
        # For other formats, use dayfirst=True
        parsed = pd.to_datetime(text, errors='coerce', dayfirst=True)

    if pd.isna(parsed):
        return None
    return parsed.normalize()


def clean_color(value: object) -> Optional[str]:
    text = clean_text(value)
    if text is None:
        return None

    text = text.upper()
    if not text.startswith("#"):
        text = f"#{text}"

    return text if HEX_COLOR_RE.fullmatch(text) else None


def clean_email_list(value: object) -> tuple[Optional[str], list[str]]:
    text = clean_text(value)
    if text is None:
        return None, []

    emails = [part.strip() for part in text.split(";") if part.strip()]
    invalid = [email for email in emails if not EMAIL_RE.fullmatch(email)]
    if invalid:
        return None, invalid

    # de-duplicate while keeping order
    seen = set()
    cleaned: list[str] = []
    for email in emails:
        key = email.lower()
        if key not in seen:
            seen.add(key)
            cleaned.append(email)

    return ("; ".join(cleaned) if cleaned else None), []


def normalize_project_name(value: object) -> str:
    return (clean_text(value) or "").strip().lower()


def to_sql_date(value: Optional[pd.Timestamp]) -> Optional[str]:
    if value is None:
        return None
    return value.strftime("%Y-%m-%d")


def fetch_table_schema(cursor: pyodbc.Cursor, table_name: str) -> Dict[str, Optional[int]]:
    """
    Query SQL Server metadata for column max lengths.
    Returns dict: column_name -> max_length (None for VARCHAR(MAX), NVARCHAR(MAX), or non-string types).
    """
    schema_map: Dict[str, Optional[int]] = {}

    sql = """
    SELECT 
        COLUMN_NAME,
        CHARACTER_MAXIMUM_LENGTH,
        DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = ?
    AND TABLE_SCHEMA = 'dbo'
    ORDER BY ORDINAL_POSITION
    """

    table_short_name = table_name.split(".")[-1]
    cursor.execute(sql, table_short_name)
    rows = cursor.fetchall()

    for col_name, char_max_len, data_type in rows:
        # Store max length for string types only
        if data_type in {"varchar", "nvarchar", "char", "nchar"}:
            if char_max_len == -1:
                # VARCHAR(MAX) / NVARCHAR(MAX)
                schema_map[col_name] = None
            else:
                schema_map[col_name] = char_max_len
        else:
            schema_map[col_name] = None

    return schema_map


def validate_row_lengths(
    row: dict, schema_map: Dict[str, Optional[int]]
) -> Tuple[bool, list]:
    """
    Validate that string fields in row do not exceed their DB column max lengths.
    Returns (is_valid, list_of_error_reasons)
    """
    errors = []

    for col_name, value in row.items():
        if value is None or not isinstance(value, str):
            continue

        if col_name not in schema_map:
            continue

        max_len = schema_map[col_name]
        if max_len is None:
            # VARCHAR(MAX) or non-string, no limit
            continue

        value_len = len(value)
        if value_len > max_len:
            errors.append(
                f"Column '{col_name}': {value_len} chars exceeds max {max_len}"
            )

    return (len(errors) == 0, errors)


def get_connection() -> pyodbc.Connection:
    if USE_TRUSTED_CONNECTION:
        conn_str = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={SERVER};"
            f"DATABASE={DATABASE};"
            "Trusted_Connection=yes;"
        )
    else:
        conn_str = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={SERVER};"
            f"DATABASE={DATABASE};"
            f"UID={USERNAME};"
            f"PWD={PASSWORD};"
        )

    return pyodbc.connect(conn_str)


def ensure_email_column_exists(cursor: pyodbc.Cursor) -> None:
    cursor.execute(
        """
        IF COL_LENGTH('dbo.FundingProjects', 'ResponsibleForReportEmails') IS NULL
        BEGIN
            ALTER TABLE dbo.FundingProjects
            ADD ResponsibleForReportEmails NVARCHAR(1000) NULL;
        END
        """
    )


def fetch_existing_projects(cursor: pyodbc.Cursor) -> set[str]:
    cursor.execute("SELECT FunderProject FROM dbo.FundingProjects")
    return {normalize_project_name(row[0]) for row in cursor.fetchall() if row[0]}


def load_input_file(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()

    if suffix == ".csv":
        return pd.read_csv(path, dtype=str, keep_default_na=False)
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path, sheet_name=SHEET_NAME, dtype=str)

    raise ValueError(f"Unsupported file type: {suffix}")


def validate_columns(df: pd.DataFrame) -> None:
    missing = [col for col in EXPECTED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(
            "Input file is missing expected columns:\n- " + "\n- ".join(missing)
        )


# =========================
# MAIN
# =========================
def main() -> int:
    input_path = Path(INPUT_FILE)

    if not input_path.exists():
        print(f"Input file not found: {input_path}")
        return 1

    print(f"Reading input file: {input_path}")
    df = load_input_file(input_path)
    df.columns = [str(col).strip() for col in df.columns]
    validate_columns(df)

    rows_to_insert: list[dict] = []
    skipped_rows: list[dict] = []
    duplicate_rows: list[dict] = []
    overlong_rows: list[dict] = []

    with get_connection() as conn:
        conn.autocommit = False
        cursor = conn.cursor()

        if CREATE_MISSING_EMAIL_COLUMN:
            ensure_email_column_exists(cursor)
            conn.commit()

        # Fetch the actual table schema to validate string lengths
        print("Fetching table schema from database...")
        schema_map = fetch_table_schema(cursor, TABLE_NAME)
        if not schema_map:
            print("Warning: Could not fetch table schema. Proceeding without column length validation.")
        else:
            print(f"Schema loaded: {len(schema_map)} columns validated")

        existing_projects = fetch_existing_projects(cursor)
        batch_seen: set[str] = set()

        for index, raw in df.iterrows():
            row_number = index + 2  # header is row 1 in Excel/CSV
            record = {col: raw.get(col, None) for col in EXPECTED_COLUMNS}
            reasons: list[str] = []

            funder_project = clean_text(record["funderProject"])
            color_hex = clean_color(record["colorHex"])
            funding_management_lead = clean_text(record["fundingManagementLead"])
            responsible_for_report = clean_text(record["responsibleForReport"])
            responsible_for_report_emails, invalid_emails = clean_email_list(
                record["responsibleForReportEmails"]
            )
            strategic_objectives = clean_text(record["strategicObjectives"]) or ""
            value_gbp = clean_currency(record["valueGBP"])
            start_date = clean_date(record["startDate"])
            end_date = clean_date(record["endDate"])
            site_area = clean_text(record["siteArea"])
            targets = clean_text(record["targets"]) or ""
            reporting_evaluation = clean_text(record["reportingEvaluation"]) or ""
            deadlines = clean_text(record["deadlines"]) or ""
            status = clean_text(record["status"])
            commentary = clean_text(record["commentary"])
            link = clean_text(record["link"])
            comments = clean_text(record["comments"])
            project_tracker = clean_text(record["projectTracker"])
            deadline_tracker = clean_text(record["deadlineTracker"])

            if invalid_emails:
                reasons.append(f"Invalid email(s): {', '.join(invalid_emails)}")

            for required_col, required_val in [
                ("funderProject", funder_project),
                ("fundingManagementLead", funding_management_lead),
                ("responsibleForReport", responsible_for_report),
                ("startDate", start_date),
                ("endDate", end_date),
                ("siteArea", site_area),
                ("status", status),
            ]:
                if required_val is None or required_val == "":
                    reasons.append(f"Missing or invalid required field: {required_col}")

            if start_date is not None and end_date is not None and start_date > end_date:
                reasons.append("StartDate is after EndDate")

            if clean_text(record["colorHex"]) and color_hex is None:
                reasons.append("Invalid ColorHex; expected #RRGGBB")

            normalized_name = normalize_project_name(funder_project)
            if not normalized_name:
                reasons.append("Normalized FunderProject is empty")
            else:
                if SKIP_DUPLICATES_BY_FUNDER_PROJECT and normalized_name in existing_projects:
                    reasons.append("Duplicate against existing dbo.FundingProjects")
                if normalized_name in batch_seen:
                    reasons.append("Duplicate inside current input file")

            prepared = {
                "SourceRowNumber": row_number,
                "Id": str(uuid.uuid4()),
                "FunderProject": funder_project,
                "ColorHex": color_hex,
                "FundingManagementLead": funding_management_lead,
                "ResponsibleForReport": responsible_for_report,
                "ResponsibleForReportEmails": responsible_for_report_emails,
                "StrategicObjectives": strategic_objectives,
                "ValueGBP": float(value_gbp) if value_gbp is not None else 0.0,
                "StartDate": to_sql_date(start_date),
                "EndDate": to_sql_date(end_date),
                "SiteArea": site_area,
                "Targets": targets,
                "ReportingEvaluation": reporting_evaluation,
                "Deadlines": deadlines,
                "Status": status,
                "Commentary": commentary,
                "Link": link,
                "Comments": comments,
                "ProjectTracker": project_tracker,
                "DeadlineTracker": deadline_tracker,
                "Reasons": " | ".join(reasons) if reasons else "",
            }

            # Check for schema-based length violations
            if schema_map:
                is_valid_length, length_errors = validate_row_lengths(prepared, schema_map)
                if not is_valid_length:
                    if TRUNCATE_OVERLONG_VALUES:
                        # TODO: implement truncation if flag is True
                        reasons.extend(length_errors)
                    else:
                        reasons.extend(length_errors)

            if reasons:
                skipped_rows.append(prepared)
                if any("Duplicate" in reason for reason in reasons):
                    duplicate_rows.append(prepared)
                if any("exceeds max" in reason for reason in reasons):
                    overlong_rows.append(prepared)
                continue

            rows_to_insert.append(prepared)
            batch_seen.add(normalized_name)

        print(f"Valid rows ready to insert: {len(rows_to_insert)}")
        print(f"Skipped rows: {len(skipped_rows)}")
        print(f"Duplicate rows: {len(duplicate_rows)}")
        print(f"Overlong rows: {len(overlong_rows)}")

        if WRITE_ERROR_FILES:
            if rows_to_insert:
                pd.DataFrame(rows_to_insert).to_excel(
                    OUTPUT_FOLDER / "funding_projects_valid_rows.xlsx",
                    index=False
                )
            if skipped_rows:
                pd.DataFrame(skipped_rows).to_excel(
                    OUTPUT_FOLDER / "funding_projects_skipped_rows.xlsx",
                    index=False
                )
            if duplicate_rows:
                pd.DataFrame(duplicate_rows).to_excel(
                    OUTPUT_FOLDER / "funding_projects_duplicate_rows.xlsx",
                    index=False
                )
            if overlong_rows:
                pd.DataFrame(overlong_rows).to_excel(
                    OUTPUT_FOLDER / "funding_projects_overlong_rows.xlsx",
                    index=False
                )

        if DRY_RUN:
            print("DRY_RUN=True, no rows inserted.")
            return 0

        if not rows_to_insert:
            print("No valid rows to insert.")
            return 0

        insert_sql = f"""
        INSERT INTO {TABLE_NAME}
        (
            Id,
            FunderProject,
            ColorHex,
            FundingManagementLead,
            ResponsibleForReport,
            ResponsibleForReportEmails,
            StrategicObjectives,
            ValueGBP,
            StartDate,
            EndDate,
            SiteArea,
            Targets,
            ReportingEvaluation,
            Deadlines,
            Status,
            Commentary,
            Link,
            Comments,
            ProjectTracker,
            DeadlineTracker,
            CreatedAtUtc,
            UpdatedAtUtc
        )
        VALUES
        (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME(), NULL
        )
        """

        insert_values = [
            (
                row["Id"],
                row["FunderProject"],
                row["ColorHex"],
                row["FundingManagementLead"],
                row["ResponsibleForReport"],
                row["ResponsibleForReportEmails"],
                row["StrategicObjectives"],
                row["ValueGBP"],
                row["StartDate"],
                row["EndDate"],
                row["SiteArea"],
                row["Targets"],
                row["ReportingEvaluation"],
                row["Deadlines"],
                row["Status"],
                row["Commentary"],
                row["Link"],
                row["Comments"],
                row["ProjectTracker"],
                row["DeadlineTracker"],
            )
            for row in rows_to_insert
        ]

        # Use normal executemany (not fast) to avoid buffer issues with long strings
        if USE_FAST_EXECUTEMANY:
            try:
                cursor.fast_executemany = True
            except Exception:
                pass

        cursor.executemany(insert_sql, insert_values)
        conn.commit()

        print(f"Inserted rows: {len(rows_to_insert)}")
        print(f"Output folder: {OUTPUT_FOLDER}")
        return 0


if __name__ == "__main__":
    sys.exit(main())
