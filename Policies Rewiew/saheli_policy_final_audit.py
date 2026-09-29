from pathlib import Path
from datetime import date, datetime
from collections import defaultdict
from difflib import SequenceMatcher
import hashlib
import re
import sys

from docx import Document
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.table import Table, TableStyleInfo


# ============================================================
# CONFIGURATION
# ============================================================

POLICY_FOLDER = Path(
    r"C:\Users\shonk\Saheli Hub\Saheli Hub - Policies"
)

OUTPUT_FOLDER = Path(
    r"C:\Users\shonk\source\PythonCodes"
)

TODAY = date.today()

# A policy with a known date/month within this number of days
# will be marked DUE SOON.
DUE_SOON_DAYS = 90

OUTPUT_FILE = OUTPUT_FOLDER / (
    f"Saheli_Policy_Final_Audit_{TODAY.strftime('%Y%m%d')}.xlsx"
)


# ============================================================
# SAFETY
# ============================================================

print("=" * 78)
print("SAHELI POLICY FINAL AUDIT")
print("=" * 78)
print()
print(f"Policy folder : {POLICY_FOLDER}")
print(f"Audit date    : {TODAY.strftime('%d/%m/%Y')}")
print(f"Output        : {OUTPUT_FILE}")
print()
print("READ-ONLY SOURCE MODE")
print("No policy files will be modified, renamed, moved or deleted.")
print()


# ============================================================
# BASIC HELPERS
# ============================================================

def clean(value):
    if value is None:
        return ""

    value = str(value)
    value = value.replace("\xa0", " ")
    value = value.replace("\u200b", "")
    value = re.sub(r"\s+", " ", value)

    return value.strip()


def normalise_for_compare(value):
    value = clean(value).lower()

    replacements = [
        " - copy",
        "_copy",
        " copy",
    ]

    for item in replacements:
        value = value.replace(item, "")

    value = re.sub(r"\.docx$", "", value)
    value = re.sub(r"[_\-]+", " ", value)
    value = re.sub(r"\s+", " ", value)

    return value.strip()


def sha256_file(path):
    sha = hashlib.sha256()

    with open(path, "rb") as f:
        while True:
            block = f.read(1024 * 1024)

            if not block:
                break

            sha.update(block)

    return sha.hexdigest()


# ============================================================
# DOCUMENT CLASSIFICATION
# ============================================================

def classify_document(filename, text):
    name = filename.lower()
    text_lower = text.lower()

    if "risk assessment" in name:
        return "Risk Assessment"

    if "checklist" in name:
        return "Checklist"

    if "standard operating procedure" in name:
        return "SOP"

    if re.search(r"\bsop\b", name):
        return "SOP"

    if "manual" in name:
        return "Manual"

    if "code of conduct" in name:
        return "Code of Conduct"

    if "handbook" in name:
        return "Handbook"

    if "policy" in name:
        return "Policy"

    # Content fallback
    if "standard operating procedure" in text_lower:
        return "SOP"

    return "Other Document"


# ============================================================
# DOCX EXTRACTION
# ============================================================

def extract_docx(path):
    """
    Extract paragraphs and tables locally.

    Nothing is sent anywhere.
    Nothing is modified.
    """

    doc = Document(str(path))

    paragraphs = []

    for paragraph in doc.paragraphs:
        text = clean(paragraph.text)

        if text:
            paragraphs.append(text)

    tables = []

    for table_index, table in enumerate(doc.tables, start=1):
        for row_index, row in enumerate(table.rows, start=1):

            cells = [
                clean(cell.text)
                for cell in row.cells
            ]

            tables.append({
                "table": table_index,
                "row": row_index,
                "cells": cells,
                "text": " | ".join(
                    c for c in cells if c
                )
            })

    all_text = "\n".join(
        paragraphs +
        [row["text"] for row in tables]
    )

    return paragraphs, tables, all_text


# ============================================================
# TABLE FIELD EXTRACTION
# ============================================================

def find_value_after_label(cells, labels):
    """
    Handles tables like:

    Last Reviewed | Sept 2024
    Next review   | Oct 2026

    AND:

    Version no. | 2.0 | Approved by Trustees: |
    May 2024 | Review date: | 2027
    """

    for i, cell in enumerate(cells):

        c = clean(cell).lower()

        for label in labels:

            label_lower = label.lower()

            if label_lower in c:

                # Sometimes value is in same cell:
                # "Next review: Oct 2026"
                if ":" in cell:
                    after = clean(
                        cell.split(":", 1)[1]
                    )

                    if after:
                        return after

                # Usually value is next cell
                for j in range(i + 1, len(cells)):

                    candidate = clean(cells[j])

                    if candidate:
                        return candidate

    return ""


def extract_control_fields(tables):
    result = {
        "author": "",
        "authorised_by_ceo": "",
        "authorised_by_board": "",
        "version": "",
        "last_reviewed": "",
        "next_review": "",
        "approved_date": "",
    }

    for row in tables:

        cells = row["cells"]

        if not cells:
            continue

        # Author
        if not result["author"]:
            value = find_value_after_label(
                cells,
                ["author"]
            )

            if value:
                result["author"] = value

        # CEO / COO approval
        if not result["authorised_by_ceo"]:
            value = find_value_after_label(
                cells,
                [
                    "authorised by ceo/coo",
                    "authorised by ceo",
                    "authorized by ceo",
                ]
            )

            if value:
                result["authorised_by_ceo"] = value

        # Board approval
        if not result["authorised_by_board"]:
            value = find_value_after_label(
                cells,
                [
                    "authorised by board chairperson",
                    "authorized by board chairperson",
                    "board chairperson or vice chair",
                ]
            )

            if value:
                result["authorised_by_board"] = value

        # Version
        if not result["version"]:
            value = find_value_after_label(
                cells,
                [
                    "version no.",
                    "version number",
                ]
            )

            if value:
                result["version"] = value

        # Last Reviewed
        if not result["last_reviewed"]:
            value = find_value_after_label(
                cells,
                [
                    "last reviewed",
                    "last review",
                ]
            )

            if value:
                result["last_reviewed"] = value

        # Next Review
        if not result["next_review"]:
            value = find_value_after_label(
                cells,
                [
                    "next review",
                    "review due",
                    "review date",
                ]
            )

            if value:
                result["next_review"] = value

        # Trustee approval date
        if not result["approved_date"]:
            value = find_value_after_label(
                cells,
                [
                    "approved by trustees",
                    "trustee approval",
                    "date approved",
                    "approved date",
                ]
            )

            if value:
                result["approved_date"] = value

    return result


# ============================================================
# DATE PARSING
# ============================================================

MONTHS = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}


def parse_review_date(value):
    """
    Returns:
        parsed_date
        precision
        display_value

    precision:
        DAY
        MONTH
        YEAR
        MISSING
        UNKNOWN
    """

    original = clean(value)

    if not original:
        return None, "MISSING", ""

    text = original.lower()

    # Remove common annotations
    text = text.replace("(draft)", "")
    text = clean(text)

    # ---------------------------------------
    # Exact numeric dates
    # ---------------------------------------

    formats = [
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%d.%m.%Y",
        "%d/%m/%y",
        "%d-%m-%y",
        "%d.%m.%y",
        "%Y-%m-%d",
    ]

    for fmt in formats:
        try:
            parsed = datetime.strptime(
                text,
                fmt
            ).date()

            return parsed, "DAY", original

        except ValueError:
            pass

    # ---------------------------------------
    # Text exact dates
    # 19 Feb 2025
    # 20 March 2027
    # ---------------------------------------

    formats = [
        "%d %b %Y",
        "%d %B %Y",
        "%d %b %y",
        "%d %B %y",
    ]

    for fmt in formats:
        try:
            parsed = datetime.strptime(
                text,
                fmt
            ).date()

            return parsed, "DAY", original

        except ValueError:
            pass

    # ---------------------------------------
    # Month + year
    # Sept 2024
    # Oct 2026
    # ---------------------------------------

    match = re.fullmatch(
        r"([a-z]+)\s+(\d{4})",
        text
    )

    if match:
        month_text = match.group(1)
        year = int(match.group(2))

        if month_text in MONTHS:
            month = MONTHS[month_text]

            # Use first of month internally,
            # but precision remains MONTH.
            parsed = date(
                year,
                month,
                1
            )

            return parsed, "MONTH", original

    # ---------------------------------------
    # Year only
    # ---------------------------------------

    match = re.fullmatch(
        r"(20\d{2})",
        text
    )

    if match:
        year = int(match.group(1))

        return date(year, 1, 1), "YEAR", original

    return None, "UNKNOWN", original


# ============================================================
# REVIEW STATUS
# ============================================================

def determine_status(next_review_raw):
    parsed, precision, display = parse_review_date(
        next_review_raw
    )

    if precision == "MISSING":
        return (
            "MISSING DATE",
            "No next review date found",
            None,
            precision
        )

    if precision == "UNKNOWN":
        return (
            "CHECK DATE",
            f"Review date could not be interpreted: {display}",
            None,
            precision
        )

    # ---------------------------------------
    # Year only
    # ---------------------------------------

    if precision == "YEAR":

        if parsed.year < TODAY.year:
            return (
                "OVERDUE",
                f"Review year was {parsed.year}",
                parsed,
                precision
            )

        if parsed.year == TODAY.year:
            return (
                "DUE IN 2026 - EXACT DATE REQUIRED",
                "Document only specifies review year 2026",
                parsed,
                precision
            )

        return (
            "CURRENT",
            f"Review year: {parsed.year}",
            parsed,
            precision
        )

    # ---------------------------------------
    # Month precision
    # ---------------------------------------

    if precision == "MONTH":

        review_year = parsed.year
        review_month = parsed.month

        current_key = (
            TODAY.year * 12 +
            TODAY.month
        )

        review_key = (
            review_year * 12 +
            review_month
        )

        difference = review_key - current_key

        if difference < 0:
            return (
                "OVERDUE",
                f"Review month passed: {display}",
                parsed,
                precision
            )

        if difference == 0:
            return (
                "DUE NOW",
                f"Review due this month: {display}",
                parsed,
                precision
            )

        if difference <= 3:
            return (
                "DUE SOON",
                f"Review due: {display}",
                parsed,
                precision
            )

        return (
            "CURRENT",
            f"Review due: {display}",
            parsed,
            precision
        )

    # ---------------------------------------
    # Exact date
    # ---------------------------------------

    days = (parsed - TODAY).days

    if days < 0:
        return (
            "OVERDUE",
            f"{abs(days)} days overdue",
            parsed,
            precision
        )

    if days == 0:
        return (
            "DUE NOW",
            "Review due today",
            parsed,
            precision
        )

    if days <= DUE_SOON_DAYS:
        return (
            "DUE SOON",
            f"{days} days until review",
            parsed,
            precision
        )

    return (
        "CURRENT",
        f"{days} days until review",
        parsed,
        precision
    )


# ============================================================
# DIARY GUIDANCE
# ============================================================

def diary_guidance(raw_date):
    parsed, precision, display = parse_review_date(
        raw_date
    )

    if precision == "DAY":
        return (
            parsed.strftime("%d/%m/%Y"),
            "Exact review date available"
        )

    if precision == "MONTH":
        return (
            display,
            "Month only - confirm exact diary date before creating calendar event"
        )

    if precision == "YEAR":
        return (
            str(parsed.year),
            "Year only - exact review date required before creating calendar event"
        )

    return (
        "",
        "Review date must be confirmed"
    )


# ============================================================
# EXCEL FORMATTING
# ============================================================

HEADER_FILL = PatternFill(
    "solid",
    fgColor="1F4E78"
)

HEADER_FONT = Font(
    color="FFFFFF",
    bold=True
)

OVERDUE_FILL = PatternFill(
    "solid",
    fgColor="F4CCCC"
)

DUE_FILL = PatternFill(
    "solid",
    fgColor="FCE5CD"
)

SOON_FILL = PatternFill(
    "solid",
    fgColor="FFF2CC"
)

CURRENT_FILL = PatternFill(
    "solid",
    fgColor="D9EAD3"
)

CHECK_FILL = PatternFill(
    "solid",
    fgColor="D9EAF7"
)

MISSING_FILL = PatternFill(
    "solid",
    fgColor="EADCF8"
)

thin_border = Border(
    left=Side(style="thin", color="D9E1F2"),
    right=Side(style="thin", color="D9E1F2"),
    top=Side(style="thin", color="D9E1F2"),
    bottom=Side(style="thin", color="D9E1F2"),
)


def style_sheet(ws):
    ws.freeze_panes = "A2"
    ws.auto_filter.ref = ws.dimensions

    for cell in ws[1]:
        cell.fill = HEADER_FILL
        cell.font = HEADER_FONT
        cell.alignment = Alignment(
            horizontal="center",
            vertical="center"
        )

    for row in ws.iter_rows():
        for cell in row:
            cell.border = thin_border
            cell.alignment = Alignment(
                vertical="top",
                wrap_text=True
            )

    for column in ws.columns:
        max_length = 0

        letter = get_column_letter(
            column[0].column
        )

        for cell in column:
            value = clean(cell.value)

            if len(value) > max_length:
                max_length = len(value)

        ws.column_dimensions[letter].width = min(
            max(max_length + 2, 12),
            45
        )


def apply_status_colours(ws, status_column):
    for row in range(2, ws.max_row + 1):

        cell = ws.cell(
            row=row,
            column=status_column
        )

        value = clean(cell.value).upper()

        if value == "OVERDUE":
            cell.fill = OVERDUE_FILL

        elif value == "DUE NOW":
            cell.fill = DUE_FILL

        elif value == "DUE SOON":
            cell.fill = SOON_FILL

        elif value == "CURRENT":
            cell.fill = CURRENT_FILL

        elif "EXACT DATE REQUIRED" in value:
            cell.fill = CHECK_FILL

        elif "MISSING" in value:
            cell.fill = MISSING_FILL

        elif "CHECK" in value:
            cell.fill = CHECK_FILL


def add_excel_table(ws, name):
    if ws.max_row < 2:
        return

    ref = (
        f"A1:"
        f"{get_column_letter(ws.max_column)}"
        f"{ws.max_row}"
    )

    table = Table(
        displayName=name,
        ref=ref
    )

    style = TableStyleInfo(
        name="TableStyleMedium2",
        showFirstColumn=False,
        showLastColumn=False,
        showRowStripes=True,
        showColumnStripes=False
    )

    table.tableStyleInfo = style

    ws.add_table(table)


# ============================================================
# SCAN DOCUMENTS
# ============================================================

files = sorted(
    POLICY_FOLDER.rglob("*.docx"),
    key=lambda p: p.name.lower()
)

print(f"Found {len(files)} DOCX documents.")
print()

records = []

for index, path in enumerate(files, start=1):

    print(
        f"[{index:02}/{len(files):02}] "
        f"{path.name}"
    )

    record = {
        "filename": path.name,
        "path": str(path),
        "type": "",
        "version": "",
        "author": "",
        "approved_by_ceo": "",
        "approved_by_board": "",
        "approved_date": "",
        "last_reviewed": "",
        "next_review": "",
        "review_precision": "",
        "status": "",
        "status_reason": "",
        "diary_date": "",
        "diary_note": "",
        "sha256": "",
        "size_kb": 0,
        "read_error": "",
        "duplicate_status": "",
        "duplicate_with": "",
    }

    try:
        paragraphs, tables, all_text = extract_docx(
            path
        )

        fields = extract_control_fields(
            tables
        )

        record["type"] = classify_document(
            path.name,
            all_text
        )

        record["version"] = fields["version"]
        record["author"] = fields["author"]
        record["approved_by_ceo"] = (
            fields["authorised_by_ceo"]
        )
        record["approved_by_board"] = (
            fields["authorised_by_board"]
        )
        record["approved_date"] = (
            fields["approved_date"]
        )
        record["last_reviewed"] = (
            fields["last_reviewed"]
        )
        record["next_review"] = (
            fields["next_review"]
        )

        (
            status,
            reason,
            parsed_date,
            precision
        ) = determine_status(
            record["next_review"]
        )

        record["status"] = status
        record["status_reason"] = reason
        record["review_precision"] = precision

        diary_date, diary_note = diary_guidance(
            record["next_review"]
        )

        record["diary_date"] = diary_date
        record["diary_note"] = diary_note

        record["sha256"] = sha256_file(path)

        record["size_kb"] = round(
            path.stat().st_size / 1024,
            1
        )

    except Exception as e:
        record["read_error"] = (
            f"{type(e).__name__}: {e}"
        )

        record["status"] = "READ ERROR"
        record["status_reason"] = (
            "Document could not be fully audited"
        )

    records.append(record)


# ============================================================
# EXACT DUPLICATES
# ============================================================

hash_groups = defaultdict(list)

for record in records:

    if record["sha256"]:
        hash_groups[
            record["sha256"]
        ].append(record)


for hash_value, group in hash_groups.items():

    if len(group) > 1:

        names = [
            item["filename"]
            for item in group
        ]

        for record in group:

            other_names = [
                name
                for name in names
                if name != record["filename"]
            ]

            record["duplicate_status"] = (
                "EXACT FILE DUPLICATE"
            )

            record["duplicate_with"] = "; ".join(
                other_names
            )


# ============================================================
# POSSIBLE DUPLICATES / OLD VERSIONS
# ============================================================

for i in range(len(records)):

    for j in range(i + 1, len(records)):

        a = records[i]
        b = records[j]

        # Already exact duplicate
        if (
            a["sha256"] and
            a["sha256"] == b["sha256"]
        ):
            continue

        name_a = normalise_for_compare(
            a["filename"]
        )

        name_b = normalise_for_compare(
            b["filename"]
        )

        similarity = SequenceMatcher(
            None,
            name_a,
            name_b
        ).ratio()

        # Exact normalised filename:
        # Example:
        # Policy.docx
        # Policy - Copy.docx
        if name_a == name_b:

            if not a["duplicate_status"]:
                a["duplicate_status"] = (
                    "POSSIBLE DUPLICATE / DIFFERENT FILE"
                )

            if not b["duplicate_status"]:
                b["duplicate_status"] = (
                    "POSSIBLE DUPLICATE / DIFFERENT FILE"
                )

            if b["filename"] not in a["duplicate_with"]:
                a["duplicate_with"] = (
                    (
                        a["duplicate_with"] + "; "
                        if a["duplicate_with"]
                        else ""
                    )
                    + b["filename"]
                )

            if a["filename"] not in b["duplicate_with"]:
                b["duplicate_with"] = (
                    (
                        b["duplicate_with"] + "; "
                        if b["duplicate_with"]
                        else ""
                    )
                    + a["filename"]
                )

        elif similarity >= 0.92:

            if not a["duplicate_status"]:
                a["duplicate_status"] = (
                    "SIMILAR NAME - CHECK"
                )

            if not b["duplicate_status"]:
                b["duplicate_status"] = (
                    "SIMILAR NAME - CHECK"
                )

            if b["filename"] not in a["duplicate_with"]:
                a["duplicate_with"] = (
                    (
                        a["duplicate_with"] + "; "
                        if a["duplicate_with"]
                        else ""
                    )
                    + b["filename"]
                )

            if a["filename"] not in b["duplicate_with"]:
                b["duplicate_with"] = (
                    (
                        b["duplicate_with"] + "; "
                        if b["duplicate_with"]
                        else ""
                    )
                    + a["filename"]
                )


# ============================================================
# CREATE WORKBOOK
# ============================================================

wb = Workbook()

# Remove default sheet
default_sheet = wb.active
wb.remove(default_sheet)


# ============================================================
# SUMMARY
# ============================================================

ws = wb.create_sheet("Summary")

summary_rows = [
    ["SAHELI POLICY LIBRARY AUDIT", ""],
    ["Audit date", TODAY.strftime("%d/%m/%Y")],
    ["Policy folder", str(POLICY_FOLDER)],
    ["Total DOCX documents", len(records)],
    [
        "Policies",
        sum(
            1 for r in records
            if r["type"] == "Policy"
        )
    ],
    [
        "Non-policy documents",
        sum(
            1 for r in records
            if r["type"] != "Policy"
        )
    ],
    [
        "Overdue",
        sum(
            1 for r in records
            if r["status"] == "OVERDUE"
        )
    ],
    [
        "Due now",
        sum(
            1 for r in records
            if r["status"] == "DUE NOW"
        )
    ],
    [
        "Due soon",
        sum(
            1 for r in records
            if r["status"] == "DUE SOON"
        )
    ],
    [
        "Current",
        sum(
            1 for r in records
            if r["status"] == "CURRENT"
        )
    ],
    [
        "Exact review date required",
        sum(
            1 for r in records
            if "EXACT DATE REQUIRED"
            in r["status"]
        )
    ],
    [
        "Missing review date",
        sum(
            1 for r in records
            if r["status"] == "MISSING DATE"
        )
    ],
    [
        "Duplicate / similarity flags",
        sum(
            1 for r in records
            if r["duplicate_status"]
        )
    ],
    [
        "Read errors",
        sum(
            1 for r in records
            if r["read_error"]
        )
    ],
]

for row in summary_rows:
    ws.append(row)

ws["A1"].font = Font(
    bold=True,
    size=16,
    color="FFFFFF"
)

ws["A1"].fill = HEADER_FILL
ws["B1"].fill = HEADER_FILL

ws.column_dimensions["A"].width = 35
ws.column_dimensions["B"].width = 70

for row in ws.iter_rows():
    for cell in row:
        cell.alignment = Alignment(
            vertical="top",
            wrap_text=True
        )


# ============================================================
# MAIN POLICY LIBRARY
# ============================================================

headers = [
    "Document",
    "Document Type",
    "Version",
    "Approved Date",
    "Last Reviewed",
    "Next Review",
    "Date Precision",
    "Status",
    "Status / Action",
    "Duplicate Status",
    "Duplicate / Similar To",
    "Author",
    "CEO/COO Approval",
    "Board Approval",
    "Size KB",
    "Read Error",
]

ws = wb.create_sheet("Policy Library")
ws.append(headers)

for r in records:

    ws.append([
        r["filename"],
        r["type"],
        r["version"],
        r["approved_date"],
        r["last_reviewed"],
        r["next_review"],
        r["review_precision"],
        r["status"],
        r["status_reason"],
        r["duplicate_status"],
        r["duplicate_with"],
        r["author"],
        r["approved_by_ceo"],
        r["approved_by_board"],
        r["size_kb"],
        r["read_error"],
    ])

style_sheet(ws)
apply_status_colours(ws, 8)
add_excel_table(ws, "PolicyLibraryTable")


# ============================================================
# NEEDS REVIEW
# ============================================================

ws = wb.create_sheet("Needs Review")

needs_headers = [
    "Document",
    "Type",
    "Last Reviewed",
    "Next Review",
    "Status",
    "Reason / Required Action",
    "Duplicate Flag",
]

ws.append(needs_headers)

review_statuses = {
    "OVERDUE",
    "DUE NOW",
    "DUE SOON",
    "MISSING DATE",
    "CHECK DATE",
    "READ ERROR",
    "DUE IN 2026 - EXACT DATE REQUIRED",
}

for r in records:

    if r["status"] in review_statuses:

        ws.append([
            r["filename"],
            r["type"],
            r["last_reviewed"],
            r["next_review"],
            r["status"],
            r["status_reason"],
            r["duplicate_status"],
        ])

style_sheet(ws)
apply_status_colours(ws, 5)
add_excel_table(ws, "NeedsReviewTable")


# ============================================================
# DUPLICATES
# ============================================================

ws = wb.create_sheet("Duplicates")

ws.append([
    "Document",
    "Duplicate Status",
    "Duplicate / Similar To",
    "Version",
    "Last Reviewed",
    "Next Review",
    "Action",
])

for r in records:

    if r["duplicate_status"]:

        if (
            r["duplicate_status"]
            == "EXACT FILE DUPLICATE"
        ):
            action = (
                "Exact binary duplicate. "
                "Confirm which filename/location should be retained "
                "before deleting anything."
            )

        else:
            action = (
                "Compare versions/content before deciding which "
                "document should be retained."
            )

        ws.append([
            r["filename"],
            r["duplicate_status"],
            r["duplicate_with"],
            r["version"],
            r["last_reviewed"],
            r["next_review"],
            action,
        ])

style_sheet(ws)
add_excel_table(ws, "DuplicatesTable")


# ============================================================
# MISSING DATES
# ============================================================

ws = wb.create_sheet("Missing Dates")

ws.append([
    "Document",
    "Type",
    "Last Reviewed",
    "Next Review",
    "Status",
    "Required Action",
])

for r in records:

    if r["status"] in {
        "MISSING DATE",
        "CHECK DATE",
        "DUE IN 2026 - EXACT DATE REQUIRED",
    }:

        ws.append([
            r["filename"],
            r["type"],
            r["last_reviewed"],
            r["next_review"],
            r["status"],
            r["status_reason"],
        ])

style_sheet(ws)
apply_status_colours(ws, 5)
add_excel_table(ws, "MissingDatesTable")


# ============================================================
# NON-POLICIES
# ============================================================

ws = wb.create_sheet("Non-Policies")

ws.append([
    "Document",
    "Document Type",
    "Version",
    "Last Reviewed",
    "Next Review",
    "Status",
    "Notes",
])

for r in records:

    if r["type"] != "Policy":

        ws.append([
            r["filename"],
            r["type"],
            r["version"],
            r["last_reviewed"],
            r["next_review"],
            r["status"],
            r["status_reason"],
        ])

style_sheet(ws)
apply_status_colours(ws, 6)
add_excel_table(ws, "NonPoliciesTable")


# ============================================================
# AA DIARY
# ============================================================

ws = wb.create_sheet("AA Diary")

ws.append([
    "Document",
    "Document Type",
    "Review Date From Document",
    "Date Precision",
    "Status",
    "Diary Date / Period",
    "Diary Action",
    "Suggested Reminder",
    "Ready for Calendar?",
])

for r in records:

    # Include policies plus governance documents
    # where a review date exists or needs confirmation.
    if r["next_review"] or r["status"] == "MISSING DATE":

        ready = (
            "YES"
            if r["review_precision"] == "DAY"
            else "NO - CONFIRM EXACT DATE"
        )

        reminder = (
            "Schedule reminder in advance once exact date is confirmed"
        )

        if r["review_precision"] == "DAY":
            reminder = (
                "Suggested: reminder 30 days before review date"
            )

        ws.append([
            r["filename"],
            r["type"],
            r["next_review"],
            r["review_precision"],
            r["status"],
            r["diary_date"],
            r["diary_note"],
            reminder,
            ready,
        ])

style_sheet(ws)
apply_status_colours(ws, 5)
add_excel_table(ws, "AADiaryTable")


# ============================================================
# SAVE
# ============================================================

OUTPUT_FOLDER.mkdir(
    parents=True,
    exist_ok=True
)

wb.save(OUTPUT_FILE)


# ============================================================
# TERMINAL SUMMARY
# ============================================================

print()
print("=" * 78)
print("AUDIT COMPLETE")
print("=" * 78)

print(f"Documents checked : {len(records)}")

print(
    "Policies          :",
    sum(
        1 for r in records
        if r["type"] == "Policy"
    )
)

print(
    "Non-policies      :",
    sum(
        1 for r in records
        if r["type"] != "Policy"
    )
)

print(
    "Overdue           :",
    sum(
        1 for r in records
        if r["status"] == "OVERDUE"
    )
)

print(
    "Due now           :",
    sum(
        1 for r in records
        if r["status"] == "DUE NOW"
    )
)

print(
    "Due soon          :",
    sum(
        1 for r in records
        if r["status"] == "DUE SOON"
    )
)

print(
    "Current           :",
    sum(
        1 for r in records
        if r["status"] == "CURRENT"
    )
)

print(
    "Exact date needed :",
    sum(
        1 for r in records
        if "EXACT DATE REQUIRED"
        in r["status"]
    )
)

print(
    "Missing date      :",
    sum(
        1 for r in records
        if r["status"] == "MISSING DATE"
    )
)

print(
    "Duplicate flags   :",
    sum(
        1 for r in records
        if r["duplicate_status"]
    )
)

print()
print("Excel report:")
print(OUTPUT_FILE)

print()
print("NO SOURCE DOCUMENTS WERE MODIFIED.")
print("=" * 78)