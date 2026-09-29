from pathlib import Path
from datetime import datetime, date, timedelta
import re
import hashlib
from difflib import SequenceMatcher

from docx import Document
from pypdf import PdfReader
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils import get_column_letter


# ============================================================
# SETTINGS
# ============================================================

POLICY_FOLDER = Path(r"C:\Users\shonk\Saheli Hub\Saheli Hub - Policies")

OUTPUT_FILE = Path.cwd() / "Saheli_Policy_Audit.xlsx"

DUE_SOON_DAYS = 90

SUPPORTED_EXTENSIONS = {
    ".docx",
    ".pdf",
    ".xlsx",
    ".xls",
    ".doc",
    ".txt",
    ".rtf",
}


# ============================================================
# HELPERS
# ============================================================

def clean_text(value):
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def normalize_policy_name(filename):
    """
    Creates a simplified name so that:
      Safeguarding Policy v2.docx
      Safeguarding Policy FINAL.pdf
      Safeguarding Policy 2025.docx

    can be recognised as potentially the same policy.
    """

    name = Path(filename).stem.lower()

    # Remove common version/date/final/copy wording
    patterns = [
        r"\bversion\s*\d+(\.\d+)?\b",
        r"\bv\s*\d+(\.\d+)?\b",
        r"\bver\s*\d+(\.\d+)?\b",
        r"\bfinal\b",
        r"\bdraft\b",
        r"\bapproved\b",
        r"\brevised\b",
        r"\bupdated\b",
        r"\bcopy\b",
        r"\bold\b",
        r"\bnew\b",
        r"\b20\d{2}\b",
        r"\(\d+\)",
    ]

    for pattern in patterns:
        name = re.sub(pattern, " ", name, flags=re.I)

    name = re.sub(r"[_\-]+", " ", name)
    name = re.sub(r"\s+", " ", name).strip()

    return name


def file_hash(path):
    """
    Detect exact duplicate files.
    """
    try:
        sha = hashlib.sha256()

        with open(path, "rb") as f:
            while True:
                chunk = f.read(1024 * 1024)
                if not chunk:
                    break
                sha.update(chunk)

        return sha.hexdigest()

    except Exception:
        return ""


# ============================================================
# READ POLICY CONTENT
# ============================================================

def read_docx(path):
    try:
        doc = Document(path)

        parts = []

        for paragraph in doc.paragraphs:
            text = clean_text(paragraph.text)
            if text:
                parts.append(text)

        # Important: many policy dates are inside tables
        for table in doc.tables:
            for row in table.rows:
                row_values = []

                for cell in row.cells:
                    value = clean_text(cell.text)
                    if value:
                        row_values.append(value)

                if row_values:
                    parts.append(" | ".join(row_values))

        return "\n".join(parts), ""

    except Exception as e:
        return "", f"DOCX read error: {e}"


def read_pdf(path):
    try:
        reader = PdfReader(str(path))

        parts = []

        for page in reader.pages:
            try:
                text = page.extract_text() or ""
                if text:
                    parts.append(text)
            except Exception:
                continue

        text = "\n".join(parts)

        if not clean_text(text):
            return "", "PDF contains no extractable text - possibly scanned"

        return text, ""

    except Exception as e:
        return "", f"PDF read error: {e}"


def read_txt(path):
    try:
        return path.read_text(
            encoding="utf-8",
            errors="ignore"
        ), ""

    except Exception as e:
        return "", f"Text read error: {e}"


def read_policy(path):

    suffix = path.suffix.lower()

    if suffix == ".docx":
        return read_docx(path)

    if suffix == ".pdf":
        return read_pdf(path)

    if suffix == ".txt":
        return read_txt(path)

    if suffix == ".doc":
        return "", "Old .doc format - manual check required"

    if suffix in {".xls", ".xlsx"}:
        return "", "Spreadsheet policy/checklist - manual/content review recommended"

    if suffix == ".rtf":
        return "", "RTF file - manual check required"

    return "", "Unsupported content format"


# ============================================================
# DATE DETECTION
# ============================================================

MONTHS = (
    "January|February|March|April|May|June|July|August|"
    "September|October|November|December|"
    "Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec"
)


def parse_date(value):

    if not value:
        return None

    value = clean_text(value)

    formats = [
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%d.%m.%Y",
        "%Y-%m-%d",
        "%d %B %Y",
        "%d %b %Y",
        "%B %Y",
        "%b %Y",
    ]

    for fmt in formats:
        try:
            parsed = datetime.strptime(value, fmt)

            # Month-only dates: use final day approximately
            if fmt in ("%B %Y", "%b %Y"):
                return parsed.date().replace(day=1)

            return parsed.date()

        except ValueError:
            pass

    return None


DATE_VALUE_PATTERN = (
    rf"(?:"
    rf"\d{{1,2}}[\/\-.]\d{{1,2}}[\/\-.]\d{{2,4}}"
    rf"|"
    rf"\d{{4}}-\d{{1,2}}-\d{{1,2}}"
    rf"|"
    rf"\d{{1,2}}\s+(?:{MONTHS})\s+\d{{4}}"
    rf"|"
    rf"(?:{MONTHS})\s+\d{{4}}"
    rf")"
)


def find_labelled_date(text, labels):

    for label in labels:

        patterns = [
            rf"{label}\s*[:\-]?\s*({DATE_VALUE_PATTERN})",
            rf"{label}.{{0,30}}?({DATE_VALUE_PATTERN})",
        ]

        for pattern in patterns:

            match = re.search(
                pattern,
                text,
                flags=re.I | re.S
            )

            if match:
                raw = clean_text(match.group(1))
                parsed = parse_date(raw)

                if parsed:
                    return parsed, raw

    return None, ""


def extract_dates(text):

    review_labels = [
        r"next\s+review\s+date",
        r"date\s+of\s+next\s+review",
        r"review\s+due",
        r"review\s+date",
        r"next\s+review",
    ]

    approved_labels = [
        r"date\s+approved",
        r"approval\s+date",
        r"approved\s+on",
        r"approved",
        r"date\s+of\s+approval",
    ]

    issued_labels = [
        r"issue\s+date",
        r"date\s+issued",
        r"effective\s+date",
        r"publication\s+date",
    ]

    review_date, review_raw = find_labelled_date(
        text,
        review_labels
    )

    approved_date, approved_raw = find_labelled_date(
        text,
        approved_labels
    )

    issued_date, issued_raw = find_labelled_date(
        text,
        issued_labels
    )

    return {
        "review_date": review_date,
        "review_raw": review_raw,
        "approved_date": approved_date,
        "approved_raw": approved_raw,
        "issued_date": issued_date,
        "issued_raw": issued_raw,
    }


# ============================================================
# VERSION / OWNER DETECTION
# ============================================================

def extract_version(text):

    patterns = [
        r"\bversion\s*[:\-]?\s*([0-9]+(?:\.[0-9]+)*)",
        r"\bversion\s*[:\-]?\s*([A-Za-z0-9.\-]+)",
        r"\bv\s*([0-9]+(?:\.[0-9]+)*)\b",
    ]

    for pattern in patterns:
        match = re.search(pattern, text, flags=re.I)

        if match:
            return clean_text(match.group(1))

    return ""


def extract_owner(text):

    labels = [
        r"policy\s+owner",
        r"document\s+owner",
        r"responsible\s+person",
        r"policy\s+lead",
        r"owner",
    ]

    for label in labels:

        match = re.search(
            rf"{label}\s*[:\-]\s*([^\n|]{{2,100}})",
            text,
            flags=re.I
        )

        if match:
            return clean_text(match.group(1))

    return ""


# ============================================================
# STATUS
# ============================================================

def calculate_status(review_date, read_error):

    today = date.today()

    if read_error:
        return "CHECK"

    if review_date is None:
        return "CHECK"

    if review_date < today:
        return "OVERDUE"

    if review_date <= today + timedelta(days=DUE_SOON_DAYS):
        return "DUE SOON"

    return "CURRENT"


# ============================================================
# SCAN
# ============================================================

if not POLICY_FOLDER.exists():
    raise SystemExit(
        f"\nERROR: Folder does not exist:\n{POLICY_FOLDER}\n"
    )


print()
print("=" * 70)
print("SAHELI POLICY AUDIT")
print("=" * 70)
print(f"Folder: {POLICY_FOLDER}")
print("Mode: READ ONLY")
print()


files = [
    p for p in POLICY_FOLDER.rglob("*")
    if p.is_file()
    and p.suffix.lower() in SUPPORTED_EXTENSIONS
    and not p.name.startswith("~$")
]


print(f"Found {len(files)} policy/document files.")
print()


records = []


for index, path in enumerate(files, start=1):

    print(f"[{index}/{len(files)}] {path.name}")

    text, read_error = read_policy(path)

    dates = extract_dates(text)

    version = extract_version(text)
    owner = extract_owner(text)

    status = calculate_status(
        dates["review_date"],
        read_error
    )

    stat = path.stat()

    record = {
        "FileName": path.name,
        "PolicyName": normalize_policy_name(path.name),
        "Folder": str(path.parent),
        "Extension": path.suffix.lower(),
        "Version": version,
        "PolicyOwner": owner,
        "ApprovalDate": dates["approved_date"],
        "ReviewDate": dates["review_date"],
        "ReviewDateFoundAs": dates["review_raw"],
        "IssueDate": dates["issued_date"],
        "Status": status,
        "AttentionReason": "",
        "PossibleDuplicate": "",
        "ExactDuplicate": "",
        "FileModified": datetime.fromtimestamp(stat.st_mtime),
        "FileSizeKB": round(stat.st_size / 1024, 1),
        "ReadResult": read_error if read_error else "OK",
        "Hash": file_hash(path),
        "FullPath": str(path),
    }

    records.append(record)


# ============================================================
# DUPLICATE DETECTION
# ============================================================

for i, a in enumerate(records):

    duplicate_names = []

    for j, b in enumerate(records):

        if i == j:
            continue

        # Exact duplicate file
        if (
            a["Hash"]
            and b["Hash"]
            and a["Hash"] == b["Hash"]
        ):
            a["ExactDuplicate"] = "YES"

            if b["FileName"] not in duplicate_names:
                duplicate_names.append(b["FileName"])

            continue

        name_a = a["PolicyName"]
        name_b = b["PolicyName"]

        if not name_a or not name_b:
            continue

        similarity = SequenceMatcher(
            None,
            name_a,
            name_b
        ).ratio()

        if similarity >= 0.88:
            if b["FileName"] not in duplicate_names:
                duplicate_names.append(b["FileName"])

    if duplicate_names:
        a["PossibleDuplicate"] = "; ".join(duplicate_names)


# ============================================================
# FINAL ATTENTION REASON
# ============================================================

for record in records:

    reasons = []

    if record["Status"] == "OVERDUE":
        reasons.append("Policy review date has passed")

    elif record["Status"] == "DUE SOON":
        reasons.append(
            f"Review due within {DUE_SOON_DAYS} days"
        )

    elif record["Status"] == "CHECK":
        if record["ReadResult"] != "OK":
            reasons.append(record["ReadResult"])
        else:
            reasons.append(
                "No reliable review date found"
            )

    if record["ExactDuplicate"] == "YES":
        reasons.append("Exact duplicate file detected")

    elif record["PossibleDuplicate"]:
        reasons.append(
            "Possible duplicate/older version detected"
        )

    record["AttentionReason"] = "; ".join(reasons)


# ============================================================
# EXCEL REPORT
# ============================================================

wb = Workbook()

ws = wb.active
ws.title = "All Policies"


headers = [
    "File Name",
    "Policy Name",
    "Folder",
    "Type",
    "Version",
    "Policy Owner",
    "Approval Date",
    "Review Date",
    "Review Date Text",
    "Issue Date",
    "Status",
    "Attention Reason",
    "Possible Duplicate",
    "Exact Duplicate",
    "Last File Modified",
    "Size KB",
    "Read Result",
    "Full Path",
]


for col, header in enumerate(headers, start=1):

    cell = ws.cell(row=1, column=col, value=header)

    cell.font = Font(
        bold=True,
        color="FFFFFF"
    )

    cell.fill = PatternFill(
        "solid",
        fgColor="1F4E78"
    )

    cell.alignment = Alignment(
        horizontal="center",
        vertical="center"
    )


def record_values(r):

    return [
        r["FileName"],
        r["PolicyName"],
        r["Folder"],
        r["Extension"],
        r["Version"],
        r["PolicyOwner"],
        r["ApprovalDate"],
        r["ReviewDate"],
        r["ReviewDateFoundAs"],
        r["IssueDate"],
        r["Status"],
        r["AttentionReason"],
        r["PossibleDuplicate"],
        r["ExactDuplicate"],
        r["FileModified"],
        r["FileSizeKB"],
        r["ReadResult"],
        r["FullPath"],
    ]


for row_num, record in enumerate(records, start=2):

    values = record_values(record)

    for col_num, value in enumerate(values, start=1):
        ws.cell(
            row=row_num,
            column=col_num,
            value=value
        )


# ============================================================
# NEEDS REVIEW SHEET
# ============================================================

needs_review = wb.create_sheet("Needs Review")


for col, header in enumerate(headers, start=1):

    cell = needs_review.cell(
        row=1,
        column=col,
        value=header
    )

    cell.font = Font(
        bold=True,
        color="FFFFFF"
    )

    cell.fill = PatternFill(
        "solid",
        fgColor="C65911"
    )


attention_records = [
    r for r in records
    if (
        r["Status"] in {
            "OVERDUE",
            "DUE SOON",
            "CHECK"
        }
        or r["PossibleDuplicate"]
        or r["ExactDuplicate"] == "YES"
    )
]


for row_num, record in enumerate(
    attention_records,
    start=2
):

    values = record_values(record)

    for col_num, value in enumerate(
        values,
        start=1
    ):
        needs_review.cell(
            row=row_num,
            column=col_num,
            value=value
        )


# ============================================================
# DUPLICATES SHEET
# ============================================================

duplicates = wb.create_sheet("Possible Duplicates")


duplicate_records = [
    r for r in records
    if r["PossibleDuplicate"]
    or r["ExactDuplicate"] == "YES"
]


for col, header in enumerate(headers, start=1):

    cell = duplicates.cell(
        row=1,
        column=col,
        value=header
    )

    cell.font = Font(
        bold=True,
        color="FFFFFF"
    )

    cell.fill = PatternFill(
        "solid",
        fgColor="7030A0"
    )


for row_num, record in enumerate(
    duplicate_records,
    start=2
):

    values = record_values(record)

    for col_num, value in enumerate(
        values,
        start=1
    ):
        duplicates.cell(
            row=row_num,
            column=col_num,
            value=value
        )


# ============================================================
# FORMATTING
# ============================================================

for sheet in [
    ws,
    needs_review,
    duplicates
]:

    sheet.freeze_panes = "A2"
    sheet.auto_filter.ref = sheet.dimensions

    for column in sheet.columns:

        letter = get_column_letter(
            column[0].column
        )

        max_length = 0

        for cell in column:
            if cell.value is not None:
                max_length = max(
                    max_length,
                    len(str(cell.value))
                )

        sheet.column_dimensions[letter].width = min(
            max(max_length + 2, 12),
            45
        )


# Date formatting
for sheet in [
    ws,
    needs_review,
    duplicates
]:

    for row in range(2, sheet.max_row + 1):

        for col in [7, 8, 10, 15]:

            sheet.cell(
                row=row,
                column=col
            ).number_format = "dd/mm/yyyy"


# ============================================================
# SUMMARY
# ============================================================

summary = wb.create_sheet(
    "Summary",
    0
)

summary["A1"] = "Saheli Policy Audit"
summary["A1"].font = Font(
    bold=True,
    size=18
)

summary["A3"] = "Audit Date"
summary["B3"] = datetime.now()
summary["B3"].number_format = "dd/mm/yyyy hh:mm"

summary["A4"] = "Policy Folder"
summary["B4"] = str(POLICY_FOLDER)

summary["A6"] = "Total Files"
summary["B6"] = len(records)

summary["A7"] = "Current"
summary["B7"] = sum(
    r["Status"] == "CURRENT"
    for r in records
)

summary["A8"] = "Due Soon"
summary["B8"] = sum(
    r["Status"] == "DUE SOON"
    for r in records
)

summary["A9"] = "Overdue"
summary["B9"] = sum(
    r["Status"] == "OVERDUE"
    for r in records
)

summary["A10"] = "Need Manual Check"
summary["B10"] = sum(
    r["Status"] == "CHECK"
    for r in records
)

summary["A11"] = "Possible / Exact Duplicates"
summary["B11"] = len(
    duplicate_records
)

summary["A13"] = "Important"
summary["B13"] = (
    "This is an automated audit. "
    "Policies flagged as CHECK should be manually verified "
    "before changing or deleting any policy."
)

summary.column_dimensions["A"].width = 28
summary.column_dimensions["B"].width = 90


# ============================================================
# SAVE
# ============================================================

wb.save(OUTPUT_FILE)


print()
print("=" * 70)
print("AUDIT COMPLETE")
print("=" * 70)

print(f"Total files:       {len(records)}")
print(
    f"Current:           "
    f"{sum(r['Status'] == 'CURRENT' for r in records)}"
)
print(
    f"Due soon:          "
    f"{sum(r['Status'] == 'DUE SOON' for r in records)}"
)
print(
    f"Overdue:           "
    f"{sum(r['Status'] == 'OVERDUE' for r in records)}"
)
print(
    f"Manual check:      "
    f"{sum(r['Status'] == 'CHECK' for r in records)}"
)
print(
    f"Duplicate flags:   "
    f"{len(duplicate_records)}"
)

print()
print(f"Excel report created:")
print(OUTPUT_FILE)
print()
print("NO POLICY FILES WERE MODIFIED.")