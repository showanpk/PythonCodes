import re
import sys
import unicodedata
from pathlib import Path
from typing import Any

import pandas as pd
from docx import Document
from openpyxl.styles import Alignment, Font, PatternFill
from pypdf import PdfReader
from striprtf.striprtf import rtf_to_text


# =========================================================
# FOLDERS
# =========================================================

CV_FOLDER = (
    Path.home()
    / "Documents"
    / "Saheli Recruitment"
    / "Health and Lifestyle Coordinator"
    / "CVs"
)

RESULTS_FOLDER = CV_FOLDER.parent

OUTPUT_EXCEL = RESULTS_FOLDER / "Health_Lifestyle_CV_Shortlist.xlsx"
OUTPUT_CSV = RESULTS_FOLDER / "Health_Lifestyle_All_Candidates.csv"

SUPPORTED_EXTENSIONS = {
    ".pdf",
    ".docx",
    ".txt",
    ".rtf",
}


# =========================================================
# SCORING CRITERIA
# =========================================================
#
# Total available weight = 100.
#
# The script only checks evidence written in the CV.
# It does not assume experience that has not been mentioned.
#

CRITERIA: list[dict[str, Any]] = [
    {
        "name": "Physical activity or sports qualification",
        "weight": 12,
        "essential": True,
        "full_hits": 1,
        "pass_ratio": 1.0,
        "patterns": [
            r"(?:level\s*[1-6]|certificate|diploma|degree|bsc|ba|msc|"
            r"nvq|cyq|cimspa|reps|qualification|qualified)"
            r".{0,70}"
            r"(?:sport|fitness|exercise|physical activity|personal train|"
            r"gym instruct|sports coach)",

            r"(?:sport|fitness|exercise|physical activity|personal train|"
            r"gym instruct|sports coach)"
            r".{0,70}"
            r"(?:level\s*[1-6]|certificate|diploma|degree|bsc|ba|msc|"
            r"nvq|cyq|cimspa|reps|qualification|qualified)",

            r"\bexercise referral\b",
            r"\blevel\s*2\s+(?:gym|fitness|exercise)\b",
            r"\blevel\s*3\s+(?:personal trainer|exercise|fitness)\b",
            r"\bqualified\s+(?:fitness instructor|sports coach|"
            r"personal trainer|exercise instructor)\b",
        ],
    },
    {
        "name": "Relevant community, health or social care experience",
        "weight": 7,
        "essential": False,
        "full_hits": 2,
        "pass_ratio": 0.5,
        "patterns": [
            r"\bcommunity health\b",
            r"\bhealth and wellbeing\b",
            r"\bwellbeing coordinator\b",
            r"\bhealth coach\b",
            r"\bsocial prescrib(?:er|ing)\b",
            r"\bcommunity development\b",
            r"\bsocial care\b",
            r"\bsupport worker\b",
            r"\bpublic health\b",
            r"\bcounsell(?:ing|or)\b",
            r"\bcommunity support\b",
        ],
    },
    {
        "name": "Outreach, referrals and community partnerships",
        "weight": 12,
        "essential": True,
        "full_hits": 3,
        "pass_ratio": 0.5,
        "patterns": [
            r"\boutreach\b",
            r"\bcommunity engagement\b",
            r"\breferral pathway",
            r"\b(?:receive|generate|manage|make|coordinate)"
            r".{0,35}\breferrals?\b",
            r"\bsocial prescribing\b",
            r"\bGP practice",
            r"\bprimary care\b",
            r"\bNHS\b",
            r"\bVCFSE\b",
            r"\bpartner organisations?\b",
            r"\bcommunity partnerships?\b",
            r"\bstakeholder engagement\b",
        ],
    },
    {
        "name": "Personalised care, triage and caseload support",
        "weight": 10,
        "essential": True,
        "full_hits": 3,
        "pass_ratio": 0.5,
        "patterns": [
            r"\bperson[- ]centred\b",
            r"\bpersonalised care\b",
            r"\bholistic support\b",
            r"\btriage\b",
            r"\binitial assessment",
            r"\blifestyle assessment",
            r"\bneeds assessment",
            r"\bcaseload\b",
            r"\bcase management\b",
            r"\bsupport plan",
            r"\baction plan",
            r"\bcare plan",
            r"\bone[- ]to[- ]one\b",
            r"\bgoal setting\b",
        ],
    },
    {
        "name": "Diet, nutrition and healthy lifestyle knowledge",
        "weight": 9,
        "essential": True,
        "full_hits": 2,
        "pass_ratio": 0.5,
        "patterns": [
            r"\bnutrition\b",
            r"\bhealthy eating\b",
            r"\bdietary\b",
            r"\bdiet plan",
            r"\bfood choices?\b",
            r"\bweight management\b",
            r"\bdiabetes\b",
            r"\bblood sugar\b",
            r"\blong[- ]term conditions?\b",
            r"\bcultural(?:ly)? appropriate diet",
            r"\bSouth Asian diet",
            r"\blifestyle change\b",
            r"\bbehaviou?r change\b",
        ],
    },
    {
        "name": "Safeguarding knowledge and practice",
        "weight": 8,
        "essential": True,
        "full_hits": 1,
        "pass_ratio": 1.0,
        "patterns": [
            r"\bsafeguarding\b",
            r"\bchild protection\b",
            r"\badult protection\b",
            r"\bvulnerable adults?\b.{0,40}\bsafeguard",
            r"\bsafeguard.{0,40}\bvulnerable adults?\b",
        ],
    },
    {
        "name": "CRM, monitoring, evaluation and reporting",
        "weight": 10,
        "essential": True,
        "full_hits": 3,
        "pass_ratio": 0.5,
        "patterns": [
            r"\bCRM\b",
            r"\bREDCap\b",
            r"\bcase management system\b",
            r"\bclient database\b",
            r"\bmonitoring and evaluation\b",
            r"\bmonitoring\b",
            r"\bevaluation\b",
            r"\bfunder reporting\b",
            r"\bquarterly report",
            r"\bperformance targets?\b",
            r"\bKPI(?:s)?\b",
            r"\boutcome measures?\b",
            r"\bbaseline\b",
            r"\bfollow[- ]up assessment",
            r"\bprogress notes?\b",
            r"\bcase stud(?:y|ies)\b",
            r"\brecord keeping\b",
            r"\bdata collection\b",
        ],
    },
    {
        "name": "Diverse communities and health inequalities",
        "weight": 7,
        "essential": True,
        "full_hits": 2,
        "pass_ratio": 0.5,
        "patterns": [
            r"\bethnically diverse\b",
            r"\bdiverse communities\b",
            r"\bculturally sensitive\b",
            r"\bcultural awareness\b",
            r"\bminority communities\b",
            r"\bmigrant communities\b",
            r"\brefugees?\b",
            r"\bhealth inequalities\b",
            r"\bdeprivation\b",
            r"\bbarriers to access\b",
            r"\bequal opportunities\b",
            r"\bseldom heard\b",
            r"\bunderserved communities\b",
        ],
    },
    {
        "name": "Volunteer recruitment and support",
        "weight": 6,
        "essential": True,
        "full_hits": 2,
        "pass_ratio": 0.5,
        "patterns": [
            r"\brecruit(?:ed|ing)? volunteers?\b",
            r"\bvolunteer recruitment\b",
            r"\btrain(?:ed|ing)? volunteers?\b",
            r"\bsupervis(?:e|ed|ing) volunteers?\b",
            r"\bsupport(?:ed|ing)? volunteers?\b",
            r"\bmotivat(?:e|ed|ing) volunteers?\b",
            r"\bvolunteer coordinator\b",
            r"\bmanage(?:d|ment of)? volunteers?\b",
        ],
    },
    {
        "name": "Communication and stakeholder engagement",
        "weight": 6,
        "essential": True,
        "full_hits": 2,
        "pass_ratio": 0.5,
        "patterns": [
            r"\bstakeholder engagement\b",
            r"\bstakeholder management\b",
            r"\bpartnership working\b",
            r"\bnetworking meetings?\b",
            r"\bmulti[- ]agency\b",
            r"\bpresentation skills?\b",
            r"\bdelivered presentations?\b",
            r"\bfacilitat(?:e|ed|ing) workshops?\b",
            r"\bexcellent communication\b",
            r"\bwritten and verbal communication\b",
            r"\bactive listening\b",
            r"\brelationship building\b",
        ],
    },
    {
        "name": "IT, databases and administration",
        "weight": 5,
        "essential": True,
        "full_hits": 2,
        "pass_ratio": 0.5,
        "patterns": [
            r"\bMicrosoft Office\b",
            r"\bMicrosoft Excel\b",
            r"\bExcel\b",
            r"\bPowerPoint\b",
            r"\bMicrosoft Word\b",
            r"\bdatabase\b",
            r"\bdata entry\b",
            r"\badministration\b",
            r"\badministrative\b",
            r"\bIT systems?\b",
            r"\bdigital systems?\b",
        ],
    },
    {
        "name": "Flexible, autonomous and team-based delivery",
        "weight": 5,
        "essential": True,
        "full_hits": 2,
        "pass_ratio": 0.5,
        "patterns": [
            r"\bevenings? and weekends?\b",
            r"\bweekend working\b",
            r"\bflexible working\b",
            r"\bwork(?:ed|ing)? autonomously\b",
            r"\bindependent working\b",
            r"\bmanage(?:d|ment of)? own workload\b",
            r"\bprioritis(?:e|ed|ing)\b",
            r"\bwork(?:ed|ing)? under pressure\b",
            r"\bteam working\b",
            r"\bmultidisciplinary team\b",
            r"\bmulti[- ]site\b",
            r"\bdeliver(?:ed|ing)? physical activit",
            r"\bencourag(?:e|ed|ing).{0,40}physical activit",
        ],
    },
    {
        "name": "Additional language ability",
        "weight": 2,
        "essential": False,
        "full_hits": 1,
        "pass_ratio": 1.0,
        "patterns": [
            r"\bbilingual\b",
            r"\bmultilingual\b",
            r"\bfluent in\b",
            r"\bnative speaker\b",
            r"\blanguages?\s*:",
        ],
    },
    {
        "name": "English and Maths qualification",
        "weight": 1,
        "essential": False,
        "full_hits": 1,
        "pass_ratio": 1.0,
        "patterns": [
            r"\bGCSE.{0,50}(?:English|Maths|Mathematics)\b",
            r"\b(?:English|Maths|Mathematics).{0,50}GCSE\b",
            r"\bfunctional skills?.{0,40}(?:English|Maths)\b",
        ],
    },
]


# =========================================================
# TEXT EXTRACTION
# =========================================================

def extract_pdf_text(file_path: Path) -> str:
    reader = PdfReader(str(file_path))

    pages: list[str] = []

    for page in reader.pages:
        page_text = page.extract_text() or ""
        pages.append(page_text)

    return "\n".join(pages)


def extract_docx_text(file_path: Path) -> str:
    document = Document(str(file_path))

    content: list[str] = []

    for paragraph in document.paragraphs:
        if paragraph.text.strip():
            content.append(paragraph.text)

    for table in document.tables:
        for row in table.rows:
            row_values = [
                cell.text.strip()
                for cell in row.cells
                if cell.text.strip()
            ]

            if row_values:
                content.append(" | ".join(row_values))

    return "\n".join(content)


def extract_txt_text(file_path: Path) -> str:
    return file_path.read_text(
        encoding="utf-8",
        errors="ignore",
    )


def extract_rtf_text(file_path: Path) -> str:
    raw_text = file_path.read_text(
        encoding="utf-8",
        errors="ignore",
    )

    return rtf_to_text(raw_text)


def extract_cv_text(file_path: Path) -> tuple[str, str]:
    """
    Return:
        extracted text,
        extraction warning or error
    """

    try:
        extension = file_path.suffix.lower()

        if extension == ".pdf":
            text = extract_pdf_text(file_path)

        elif extension == ".docx":
            text = extract_docx_text(file_path)

        elif extension == ".txt":
            text = extract_txt_text(file_path)

        elif extension == ".rtf":
            text = extract_rtf_text(file_path)

        else:
            return "", f"Unsupported file type: {extension}"

        text = text.strip()

        if len(text) < 150:
            return (
                text,
                "Very little text was extracted. "
                "The CV may be scanned or image-based and needs manual review.",
            )

        return text, ""

    except Exception as error:
        return "", f"Could not extract text: {error}"


# =========================================================
# TEXT CLEANING
# =========================================================

def normalise_text(text: str) -> str:
    text = unicodedata.normalize("NFKC", text)

    text = text.replace("\u00a0", " ")
    text = text.replace("–", "-")
    text = text.replace("—", "-")

    text = re.sub(r"\s+", " ", text)

    return text.lower().strip()


def split_sentences(text: str) -> list[str]:
    sentences = re.split(
        r"(?<=[.!?])\s+|\n+|\r+",
        text,
    )

    cleaned_sentences = []

    for sentence in sentences:
        sentence = re.sub(r"\s+", " ", sentence).strip()

        if sentence:
            cleaned_sentences.append(sentence)

    return cleaned_sentences


def shorten_evidence(text: str, maximum_length: int = 220) -> str:
    text = re.sub(r"\s+", " ", text).strip()

    if len(text) <= maximum_length:
        return text

    return text[: maximum_length - 3].rstrip() + "..."


# =========================================================
# CRITERION SCORING
# =========================================================

def find_evidence(
    original_sentences: list[str],
    pattern: str,
) -> str:
    compiled_pattern = re.compile(
        pattern,
        flags=re.IGNORECASE,
    )

    for sentence in original_sentences:
        if compiled_pattern.search(sentence):
            return shorten_evidence(sentence)

    return ""


def score_criterion(
    normalised_cv_text: str,
    original_sentences: list[str],
    criterion: dict[str, Any],
) -> dict[str, Any]:
    matched_patterns: list[str] = []
    evidence_items: list[str] = []

    for pattern in criterion["patterns"]:
        compiled_pattern = re.compile(
            pattern,
            flags=re.IGNORECASE,
        )

        if compiled_pattern.search(normalised_cv_text):
            matched_patterns.append(pattern)

            evidence = find_evidence(
                original_sentences,
                pattern,
            )

            if evidence and evidence not in evidence_items:
                evidence_items.append(evidence)

    full_hits = max(int(criterion["full_hits"]), 1)

    coverage_ratio = min(
        len(matched_patterns) / full_hits,
        1.0,
    )

    criterion_score = round(
        float(criterion["weight"]) * coverage_ratio,
        2,
    )

    criterion_met = (
        coverage_ratio >= float(criterion["pass_ratio"])
    )

    return {
        "score": criterion_score,
        "coverage": coverage_ratio,
        "met": criterion_met,
        "matches": len(matched_patterns),
        "evidence": " | ".join(evidence_items[:2]),
    }


# =========================================================
# CV SCORING
# =========================================================

def score_cv(file_path: Path) -> dict[str, Any]:
    cv_text, extraction_warning = extract_cv_text(file_path)

    normalised_cv_text = normalise_text(cv_text)
    original_sentences = split_sentences(cv_text)

    result: dict[str, Any] = {
        "Candidate": file_path.stem,
        "CV File": file_path.name,
        "File Path": str(file_path),
        "Overall Score": 0.0,
        "Essential Criteria Met": 0,
        "Essential Criteria Total": 0,
        "Essential Gaps": "",
        "Strong Evidence": "",
        "Extraction or Review Notes": extraction_warning,
    }

    overall_score = 0.0

    essential_met = 0
    essential_total = 0
    essential_gaps: list[str] = []

    criterion_results: list[dict[str, Any]] = []

    for criterion in CRITERIA:
        criterion_result = score_criterion(
            normalised_cv_text,
            original_sentences,
            criterion,
        )

        overall_score += criterion_result["score"]

        if criterion["essential"]:
            essential_total += 1

            if criterion_result["met"]:
                essential_met += 1
            else:
                essential_gaps.append(criterion["name"])

        criterion_results.append(
            {
                "name": criterion["name"],
                "weight": criterion["weight"],
                "score": criterion_result["score"],
                "coverage": criterion_result["coverage"],
                "evidence": criterion_result["evidence"],
            }
        )

        result[
            f"Score - {criterion['name']}"
        ] = criterion_result["score"]

        result[
            f"Evidence - {criterion['name']}"
        ] = criterion_result["evidence"]

    result["Overall Score"] = round(overall_score, 2)
    result["Essential Criteria Met"] = essential_met
    result["Essential Criteria Total"] = essential_total
    result["Essential Gaps"] = "; ".join(essential_gaps)

    strongest_criteria = sorted(
        [
            item
            for item in criterion_results
            if item["evidence"]
        ],
        key=lambda item: (
            item["coverage"],
            item["weight"],
            item["score"],
        ),
        reverse=True,
    )[:4]

    strong_evidence_items = []

    for item in strongest_criteria:
        strong_evidence_items.append(
            f"{item['name']}: {item['evidence']}"
        )

    result["Strong Evidence"] = " || ".join(
        strong_evidence_items
    )

    if not cv_text:
        result["Overall Score"] = 0.0

        if not result["Extraction or Review Notes"]:
            result["Extraction or Review Notes"] = (
                "No readable CV text was found."
            )

    return result


# =========================================================
# USER INPUT
# =========================================================

def ask_shortlist_count() -> int:
    while True:
        value = input(
            "How many candidates should be included "
            "in the shortlist? [20]: "
        ).strip()

        if not value:
            return 20

        try:
            count = int(value)

            if count < 1:
                print("Enter a number greater than zero.")
                continue

            if count > 500:
                print("Enter a number between 1 and 500.")
                continue

            return count

        except ValueError:
            print(
                "Enter a whole number, for example 20."
            )


# =========================================================
# EXCEL FORMATTING
# =========================================================

def format_worksheet(worksheet) -> None:
    worksheet.freeze_panes = "A2"
    worksheet.auto_filter.ref = worksheet.dimensions

    header_fill = PatternFill(
        fill_type="solid",
        fgColor="D9EAD3",
    )

    for cell in worksheet[1]:
        cell.font = Font(bold=True)
        cell.fill = header_fill
        cell.alignment = Alignment(
            horizontal="center",
            vertical="center",
            wrap_text=True,
        )

    for row in worksheet.iter_rows(min_row=2):
        for cell in row:
            cell.alignment = Alignment(
                vertical="top",
                wrap_text=True,
            )

    for column_cells in worksheet.columns:
        column_letter = column_cells[0].column_letter

        maximum_length = 0

        for cell in column_cells:
            value = "" if cell.value is None else str(cell.value)
            maximum_length = max(
                maximum_length,
                len(value),
            )

        worksheet.column_dimensions[
            column_letter
        ].width = min(max(maximum_length + 2, 12), 55)


# =========================================================
# REPORT CREATION
# =========================================================

def create_reports(
    all_results: list[dict[str, Any]],
    shortlist_count: int,
) -> None:
    if not all_results:
        raise RuntimeError(
            "No CV results were available."
        )

    dataframe = pd.DataFrame(all_results)

    dataframe = dataframe.sort_values(
        by=[
            "Essential Criteria Met",
            "Overall Score",
        ],
        ascending=[
            False,
            False,
        ],
        kind="stable",
    ).reset_index(drop=True)

    dataframe.insert(
        0,
        "Review Rank",
        range(1, len(dataframe) + 1),
    )

    shortlist_count = min(
        shortlist_count,
        len(dataframe),
    )

    top_candidates = dataframe.head(
        shortlist_count
    ).copy()

    shortlist_columns = [
        "Review Rank",
        "Candidate",
        "CV File",
        "Overall Score",
        "Essential Criteria Met",
        "Essential Criteria Total",
        "Essential Gaps",
        "Strong Evidence",
        "Extraction or Review Notes",
    ]

    criteria_rows = []

    for criterion in CRITERIA:
        criteria_rows.append(
            {
                "Criterion": criterion["name"],
                "Type": (
                    "Essential"
                    if criterion["essential"]
                    else "Desirable"
                ),
                "Maximum Weight": criterion["weight"],
                "Full Score Match Count": criterion["full_hits"],
            }
        )

    criteria_dataframe = pd.DataFrame(criteria_rows)

    RESULTS_FOLDER.mkdir(
        parents=True,
        exist_ok=True,
    )

    dataframe.to_csv(
        OUTPUT_CSV,
        index=False,
        encoding="utf-8-sig",
    )

    with pd.ExcelWriter(
        OUTPUT_EXCEL,
        engine="openpyxl",
    ) as writer:
        top_candidates[
            shortlist_columns
        ].to_excel(
            writer,
            sheet_name="Top Candidates",
            index=False,
        )

        dataframe.to_excel(
            writer,
            sheet_name="All Candidates",
            index=False,
        )

        criteria_dataframe.to_excel(
            writer,
            sheet_name="Scoring Criteria",
            index=False,
        )

        for worksheet in writer.book.worksheets:
            format_worksheet(worksheet)


# =========================================================
# MAIN
# =========================================================

def main() -> None:
    print("=" * 72)
    print("Health and Lifestyle Coordinator CV Review Tool")
    print("=" * 72)
    print()

    if not CV_FOLDER.exists():
        raise FileNotFoundError(
            "The CV folder does not exist:\n"
            f"{CV_FOLDER}"
        )

    shortlist_count = ask_shortlist_count()

    cv_files = sorted(
        [
            file
            for file in CV_FOLDER.iterdir()
            if file.is_file()
            and file.suffix.lower() in SUPPORTED_EXTENSIONS
        ],
        key=lambda file: file.name.lower(),
    )

    old_doc_files = sorted(
        CV_FOLDER.glob("*.doc")
    )

    if old_doc_files:
        print()
        print(
            "Warning: old .doc files cannot be read by this script."
        )
        print(
            "Convert these files to PDF or DOCX first:"
        )

        for old_doc in old_doc_files:
            print(f" - {old_doc.name}")

    if not cv_files:
        raise RuntimeError(
            "No supported CV files were found.\n"
            "Supported formats: PDF, DOCX, TXT and RTF."
        )

    print()
    print(f"CV folder: {CV_FOLDER}")
    print(f"Readable CV files found: {len(cv_files)}")
    print(f"Shortlist requested: {shortlist_count}")
    print()

    results: list[dict[str, Any]] = []

    for index, cv_file in enumerate(
        cv_files,
        start=1,
    ):
        print(
            f"[{index}/{len(cv_files)}] "
            f"Reviewing {cv_file.name}"
        )

        result = score_cv(cv_file)
        results.append(result)

    create_reports(
        results,
        shortlist_count,
    )

    print()
    print("=" * 72)
    print("Review completed")
    print("=" * 72)
    print(f"Candidates reviewed: {len(results)}")
    print(f"Excel report: {OUTPUT_EXCEL}")
    print(f"CSV report: {OUTPUT_CSV}")
    print()
    print(
        "Important: Review the original CVs and interview evidence "
        "before making any recruitment decision."
    )


if __name__ == "__main__":
    try:
        main()

    except PermissionError:
        print()
        print(
            "The result file could not be saved. "
            "Close the existing Excel or CSV report and run again."
        )

    except Exception as error:
        print()
        print(f"ERROR: {error}")

    input("\nPress ENTER to close...")