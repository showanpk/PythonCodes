import csv
import re
import shutil
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from docx import Document
from openpyxl.styles import Alignment, Font, PatternFill
from pypdf import PdfReader
from striprtf.striprtf import rtf_to_text


DEFAULT_CV_FOLDER = (
    Path.home()
    / "Documents"
    / "Saheli Recruitment"
    / "Health and Lifestyle Coordinator"
    / "CVs"
)

SUPPORTED_EXTENSIONS = {".pdf", ".docx", ".txt", ".rtf"}

# Scores total 100. The patterns are based only on job-related requirements.
CRITERIA: list[dict[str, Any]] = [
    {
        "name": "Physical activity or sports qualification",
        "weight": 12,
        "essential": True,
        "minimum_hits": 1,
        "patterns": [
            r"(?:level\s*[1-6]|certificate|diploma|degree|bsc|ba|msc|nvq|cyq|cimspa|reps|qualification|qualified).{0,80}(?:sport|fitness|exercise|physical activity|personal train|gym instruct|sports coach)",
            r"(?:sport|fitness|exercise|physical activity|personal train|gym instruct|sports coach).{0,80}(?:level\s*[1-6]|certificate|diploma|degree|bsc|ba|msc|nvq|cyq|cimspa|reps|qualification|qualified)",
            r"\bexercise referral\b",
            r"\blevel\s*2\s+(?:gym|fitness|exercise)\b",
            r"\blevel\s*3\s+(?:personal trainer|exercise|fitness)\b",
        ],
    },
    {
        "name": "Community, health or social care experience",
        "weight": 8,
        "essential": False,
        "minimum_hits": 2,
        "patterns": [
            r"\bcommunity health\b", r"\bhealth and wellbeing\b",
            r"\bhealth coach\b", r"\bsocial prescrib(?:er|ing)\b",
            r"\bcommunity development\b", r"\bsocial care\b",
            r"\bsupport worker\b", r"\bpublic health\b",
            r"\bcounsell(?:ing|or)\b", r"\bcommunity support\b",
        ],
    },
    {
        "name": "Outreach, referrals and partnerships",
        "weight": 12,
        "essential": True,
        "minimum_hits": 3,
        "patterns": [
            r"\boutreach\b", r"\bcommunity engagement\b",
            r"\breferral pathways?\b", r"\breferrals?\b",
            r"\bsocial prescribing\b", r"\bGP practices?\b",
            r"\bprimary care\b", r"\bNHS\b", r"\bVCFSE\b",
            r"\bpartner organisations?\b", r"\bcommunity partnerships?\b",
            r"\bstakeholder engagement\b",
        ],
    },
    {
        "name": "Personalised care, triage and caseload support",
        "weight": 10,
        "essential": True,
        "minimum_hits": 3,
        "patterns": [
            r"\bperson[- ]centred\b", r"\bpersonalised care\b",
            r"\bholistic support\b", r"\btriage\b",
            r"\binitial assessments?\b", r"\blifestyle assessments?\b",
            r"\bneeds assessments?\b", r"\bcaseload\b",
            r"\bcase management\b", r"\bsupport plans?\b",
            r"\baction plans?\b", r"\bcare plans?\b",
            r"\bone[- ]to[- ]one\b", r"\bgoal setting\b",
        ],
    },
    {
        "name": "Diet, nutrition and behaviour change",
        "weight": 9,
        "essential": True,
        "minimum_hits": 2,
        "patterns": [
            r"\bnutrition\b", r"\bhealthy eating\b", r"\bdietary\b",
            r"\bdiet plans?\b", r"\bfood choices?\b",
            r"\bweight management\b", r"\bdiabetes\b",
            r"\bblood sugar\b", r"\blong[- ]term conditions?\b",
            r"\bSouth Asian diet\b", r"\blifestyle change\b",
            r"\bbehaviou?r change\b",
        ],
    },
    {
        "name": "Safeguarding children and vulnerable adults",
        "weight": 8,
        "essential": True,
        "minimum_hits": 1,
        "patterns": [
            r"\bsafeguarding\b", r"\bchild protection\b",
            r"\badult protection\b", r"\bvulnerable adults?\b",
        ],
    },
    {
        "name": "CRM, monitoring, evaluation and reporting",
        "weight": 10,
        "essential": True,
        "minimum_hits": 3,
        "patterns": [
            r"\bCRM\b", r"\bREDCap\b", r"\bclient database\b",
            r"\bcase management system\b", r"\bmonitoring and evaluation\b",
            r"\bfunder reporting\b", r"\bquarterly reports?\b",
            r"\bperformance targets?\b", r"\bKPIs?\b",
            r"\boutcome measures?\b", r"\bbaseline\b",
            r"\bfollow[- ]up assessments?\b", r"\bprogress notes?\b",
            r"\bcase stud(?:y|ies)\b", r"\brecord keeping\b",
            r"\bdata collection\b", r"\breports?\b",
        ],
    },
    {
        "name": "Diverse communities and health inequalities",
        "weight": 7,
        "essential": True,
        "minimum_hits": 2,
        "patterns": [
            r"\bethnically diverse\b", r"\bdiverse communities\b",
            r"\bculturally sensitive\b", r"\bcultural awareness\b",
            r"\bminority communities\b", r"\bmigrant communities\b",
            r"\brefugees?\b", r"\bhealth inequalities\b",
            r"\bdeprivation\b", r"\bbarriers to access\b",
            r"\bequal opportunities\b", r"\bseldom heard\b",
            r"\bunderserved communities\b",
        ],
    },
    {
        "name": "Volunteer recruitment, training and support",
        "weight": 6,
        "essential": True,
        "minimum_hits": 2,
        "patterns": [
            r"\brecruit(?:ed|ing)? volunteers?\b",
            r"\btrain(?:ed|ing)? volunteers?\b",
            r"\bsupervis(?:e|ed|ing) volunteers?\b",
            r"\bsupport(?:ed|ing)? volunteers?\b",
            r"\bmotivat(?:e|ed|ing) volunteers?\b",
            r"\bvolunteer coordinator\b", r"\bvolunteer management\b",
        ],
    },
    {
        "name": "Communication and stakeholder engagement",
        "weight": 6,
        "essential": True,
        "minimum_hits": 2,
        "patterns": [
            r"\bstakeholder engagement\b", r"\bstakeholder management\b",
            r"\bpartnership working\b", r"\bnetworking meetings?\b",
            r"\bmulti[- ]agency\b", r"\bpresentation skills?\b",
            r"\bdelivered presentations?\b", r"\bfacilitat(?:e|ed|ing) workshops?\b",
            r"\bexcellent communication\b", r"\bactive listening\b",
            r"\brelationship building\b",
        ],
    },
    {
        "name": "IT, database and administration",
        "weight": 5,
        "essential": True,
        "minimum_hits": 2,
        "patterns": [
            r"\bMicrosoft Office\b", r"\bMicrosoft Excel\b", r"\bExcel\b",
            r"\bPowerPoint\b", r"\bMicrosoft Word\b", r"\bdatabase\b",
            r"\bdata entry\b", r"\badministration\b",
            r"\badministrative\b", r"\bIT systems?\b",
        ],
    },
    {
        "name": "Autonomous, team and flexible delivery",
        "weight": 5,
        "essential": True,
        "minimum_hits": 2,
        "patterns": [
            r"\bevenings? and weekends?\b", r"\bweekend working\b",
            r"\bflexible working\b", r"\bwork(?:ed|ing)? autonomously\b",
            r"\bindependent working\b", r"\bmanage(?:d|ment of)? own workload\b",
            r"\bprioritis(?:e|ed|ing)\b", r"\bwork(?:ed|ing)? under pressure\b",
            r"\bteam working\b", r"\bmultidisciplinary team\b",
            r"\bmulti[- ]site\b", r"\bphysical activity delivery\b",
        ],
    },
    {
        "name": "Additional language ability",
        "weight": 1,
        "essential": False,
        "minimum_hits": 1,
        "patterns": [
            r"\bbilingual\b", r"\bmultilingual\b", r"\bfluent in\b",
            r"\bnative speaker\b", r"\blanguages?\s*:",
        ],
    },
    {
        "name": "English and Maths qualification",
        "weight": 1,
        "essential": False,
        "minimum_hits": 1,
        "patterns": [
            r"\bGCSE.{0,60}(?:English|Maths|Mathematics)\b",
            r"\b(?:English|Maths|Mathematics).{0,60}GCSE\b",
            r"\bfunctional skills?.{0,50}(?:English|Maths)\b",
        ],
    },
]


def ask_cv_folder() -> Path:
    print(f"Default CV folder:\n{DEFAULT_CV_FOLDER}\n")
    value = input("Press ENTER to use it, or paste another CV folder path: ").strip().strip('"')
    return Path(value).expanduser() if value else DEFAULT_CV_FOLDER


def ask_shortlist_count() -> int:
    while True:
        value = input("How many candidates should be copied into the shortlist pack? [20]: ").strip()
        if not value:
            return 20
        try:
            count = int(value)
            if 1 <= count <= 500:
                return count
        except ValueError:
            pass
        print("Enter a whole number between 1 and 500.")


def extract_text(file_path: Path) -> tuple[str, str]:
    try:
        suffix = file_path.suffix.lower()
        if suffix == ".pdf":
            reader = PdfReader(str(file_path))
            text = "\n".join((page.extract_text() or "") for page in reader.pages)
        elif suffix == ".docx":
            document = Document(str(file_path))
            parts = [p.text for p in document.paragraphs if p.text.strip()]
            for table in document.tables:
                for row in table.rows:
                    values = [cell.text.strip() for cell in row.cells if cell.text.strip()]
                    if values:
                        parts.append(" | ".join(values))
            text = "\n".join(parts)
        elif suffix == ".txt":
            text = file_path.read_text(encoding="utf-8", errors="ignore")
        elif suffix == ".rtf":
            raw = file_path.read_text(encoding="utf-8", errors="ignore")
            text = rtf_to_text(raw)
        else:
            return "", f"Unsupported file type: {suffix}"

        text = text.strip()
        if len(text) < 150:
            return text, "Very little text was extracted. The CV may be scanned and needs manual review."
        return text, ""
    except Exception as error:
        return "", f"Could not extract text: {error}"


def normalise(text: str) -> str:
    text = unicodedata.normalize("NFKC", text)
    text = text.replace("\u00a0", " ").replace("–", "-").replace("—", "-")
    return re.sub(r"\s+", " ", text).lower().strip()


def sentences(text: str) -> list[str]:
    return [re.sub(r"\s+", " ", item).strip() for item in re.split(r"(?<=[.!?])\s+|[\r\n]+", text) if item.strip()]


def evidence_for(pattern: str, original_sentences: list[str]) -> str:
    compiled = re.compile(pattern, re.IGNORECASE)
    for sentence in original_sentences:
        if compiled.search(sentence):
            sentence = re.sub(r"\s+", " ", sentence).strip()
            return sentence if len(sentence) <= 220 else sentence[:217].rstrip() + "..."
    return ""


def score_cv(file_path: Path) -> dict[str, Any]:
    text, warning = extract_text(file_path)
    clean_text = normalise(text)
    original_sentences = sentences(text)

    result: dict[str, Any] = {
        "Candidate": file_path.stem,
        "CV File": file_path.name,
        "Original CV Path": str(file_path.resolve()),
        "Overall Score": 0.0,
        "Essential Criteria Met": 0,
        "Essential Criteria Total": sum(1 for c in CRITERIA if c["essential"]),
        "Essential Gaps": "",
        "Strong Evidence": "",
        "Manual Review Notes": warning,
    }

    total_score = 0.0
    essential_met = 0
    essential_gaps: list[str] = []
    strengths: list[tuple[float, int, str]] = []

    for criterion in CRITERIA:
        matched_patterns: list[str] = []
        evidence_items: list[str] = []

        for pattern in criterion["patterns"]:
            if re.search(pattern, clean_text, re.IGNORECASE):
                matched_patterns.append(pattern)
                evidence = evidence_for(pattern, original_sentences)
                if evidence and evidence not in evidence_items:
                    evidence_items.append(evidence)

        minimum_hits = max(int(criterion["minimum_hits"]), 1)
        coverage = min(len(matched_patterns) / minimum_hits, 1.0)
        score = round(float(criterion["weight"]) * coverage, 2)
        met = len(matched_patterns) >= minimum_hits
        evidence_text = " | ".join(evidence_items[:2])

        total_score += score
        result[f"Score - {criterion['name']}"] = score
        result[f"Evidence - {criterion['name']}"] = evidence_text

        if criterion["essential"]:
            if met:
                essential_met += 1
            else:
                essential_gaps.append(criterion["name"])

        if evidence_text:
            strengths.append((coverage, int(criterion["weight"]), f"{criterion['name']}: {evidence_text}"))

    result["Overall Score"] = round(total_score, 2) if text else 0.0
    result["Essential Criteria Met"] = essential_met
    result["Essential Gaps"] = "; ".join(essential_gaps)
    result["Strong Evidence"] = " || ".join(item[2] for item in sorted(strengths, reverse=True)[:4])

    if not text and not warning:
        result["Manual Review Notes"] = "No readable CV text was found."

    return result


def format_sheet(worksheet) -> None:
    worksheet.freeze_panes = "A2"
    worksheet.auto_filter.ref = worksheet.dimensions
    fill = PatternFill(fill_type="solid", fgColor="D9EAD3")

    for cell in worksheet[1]:
        cell.font = Font(bold=True)
        cell.fill = fill
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

    for row in worksheet.iter_rows(min_row=2):
        for cell in row:
            cell.alignment = Alignment(vertical="top", wrap_text=True)

    for column_cells in worksheet.columns:
        letter = column_cells[0].column_letter
        longest = max(len(str(cell.value or "")) for cell in column_cells)
        worksheet.column_dimensions[letter].width = min(max(longest + 2, 12), 55)


def create_pack(results: list[dict[str, Any]], shortlist_count: int, cv_folder: Path) -> tuple[Path, Path, int]:
    dataframe = pd.DataFrame(results).sort_values(
        by=["Essential Criteria Met", "Overall Score"],
        ascending=[False, False],
        kind="stable",
    ).reset_index(drop=True)
    dataframe.insert(0, "Review Rank", range(1, len(dataframe) + 1))

    shortlist_count = min(shortlist_count, len(dataframe))
    top = dataframe.head(shortlist_count).copy()

    base_folder = cv_folder.parent
    pack_folder = base_folder / "Health_Lifestyle_Shortlist_Pack"
    shortlisted_cv_folder = pack_folder / "Shortlisted_CVs"
    excel_path = pack_folder / "Health_Lifestyle_CV_Shortlist.xlsx"
    csv_path = pack_folder / "Health_Lifestyle_All_Candidates.csv"
    zip_path = base_folder / "Health_Lifestyle_Shortlist_Pack.zip"

    if pack_folder.exists():
        shutil.rmtree(pack_folder)
    shortlisted_cv_folder.mkdir(parents=True, exist_ok=True)

    top["Shortlisted CV"] = ""
    copied_count = 0

    for index, row in top.iterrows():
        source = Path(row["Original CV Path"])
        if not source.exists():
            note = str(row.get("Manual Review Notes", "") or "").strip()
            extra = f"Original CV not found: {source}"
            top.at[index, "Manual Review Notes"] = f"{note}; {extra}" if note else extra
            continue

        destination_name = f"{int(row['Review Rank']):02d}_{source.name}"
        destination = shortlisted_cv_folder / destination_name
        shutil.copy2(source, destination)
        top.at[index, "Shortlisted CV"] = f"Shortlisted_CVs/{destination_name}"
        copied_count += 1

    shortlist_columns = [
        "Review Rank", "Candidate", "CV File", "Shortlisted CV",
        "Overall Score", "Essential Criteria Met", "Essential Criteria Total",
        "Essential Gaps", "Strong Evidence", "Manual Review Notes",
    ]

    criteria_df = pd.DataFrame([
        {
            "Criterion": c["name"],
            "Type": "Essential" if c["essential"] else "Desirable",
            "Maximum Weight": c["weight"],
            "Evidence Hits for Full Score": c["minimum_hits"],
        }
        for c in CRITERIA
    ])

    manual_review_df = dataframe[
        dataframe["Manual Review Notes"].fillna("").astype(str).str.strip() != ""
    ][[
        "Review Rank", "Candidate", "CV File", "Overall Score",
        "Essential Criteria Met", "Manual Review Notes", "Original CV Path",
    ]].copy()

    dataframe.to_csv(csv_path, index=False, encoding="utf-8-sig")

    with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
        top[shortlist_columns].to_excel(writer, sheet_name="Top Candidates", index=False)
        dataframe.to_excel(writer, sheet_name="All Candidates", index=False)
        criteria_df.to_excel(writer, sheet_name="Scoring Criteria", index=False)
        manual_review_df.to_excel(writer, sheet_name="Manual Review", index=False)

        top_sheet = writer.book["Top Candidates"]
        cv_column = shortlist_columns.index("Shortlisted CV") + 1
        for excel_row, (_, candidate) in enumerate(top.iterrows(), start=2):
            link = str(candidate["Shortlisted CV"] or "").strip()
            cell = top_sheet.cell(row=excel_row, column=cv_column)
            if link:
                cell.value = "Open CV"
                cell.hyperlink = link
                cell.style = "Hyperlink"
            else:
                cell.value = "CV not copied"

        for worksheet in writer.book.worksheets:
            format_sheet(worksheet)

    if zip_path.exists():
        zip_path.unlink()
    shutil.make_archive(str(zip_path.with_suffix("")), "zip", root_dir=pack_folder)

    return pack_folder, zip_path, copied_count


def main() -> None:
    print("=" * 72)
    print("Health and Lifestyle Coordinator CV Shortlist Pack")
    print("=" * 72)
    print()

    cv_folder = ask_cv_folder()
    if not cv_folder.exists() or not cv_folder.is_dir():
        raise FileNotFoundError(f"CV folder was not found:\n{cv_folder}")

    shortlist_count = ask_shortlist_count()
    cv_files = sorted(
        [file for file in cv_folder.iterdir() if file.is_file() and file.suffix.lower() in SUPPORTED_EXTENSIONS],
        key=lambda file: file.name.lower(),
    )

    old_doc_files = sorted(cv_folder.glob("*.doc"))
    if old_doc_files:
        print("\nThese old .doc files need converting to PDF or DOCX before they can be scored:")
        for file in old_doc_files:
            print(f" - {file.name}")

    if not cv_files:
        raise RuntimeError("No readable CV files were found. Supported formats: PDF, DOCX, TXT and RTF.")

    print(f"\nCVs found: {len(cv_files)}")
    print(f"Shortlist requested: {shortlist_count}\n")

    results: list[dict[str, Any]] = []
    for number, cv_file in enumerate(cv_files, start=1):
        print(f"[{number}/{len(cv_files)}] Reviewing {cv_file.name}")
        results.append(score_cv(cv_file))

    pack_folder, zip_path, copied_count = create_pack(results, shortlist_count, cv_folder)

    print("\n" + "=" * 72)
    print("Completed")
    print("=" * 72)
    print(f"Candidates reviewed: {len(results)}")
    print(f"Shortlisted CVs copied: {copied_count}")
    print(f"Pack folder: {pack_folder}")
    print(f"ZIP file to share: {zip_path}")
    print("\nThe ranking is a review aid. Check the original CVs and interview evidence before deciding.")


if __name__ == "__main__":
    try:
        main()
    except PermissionError:
        print("\nERROR: Close the existing Excel/ZIP file and run the script again.")
    except Exception as error:
        print(f"\nERROR: {error}")
    input("\nPress ENTER to close...")