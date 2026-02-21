import os
import re
import pandas as pd

# ======================================
# CONFIG
# ======================================
INPUT_PATH = r"C:\Users\shonk\Downloads\Saheli Hub Health Assessment(1-1470).xlsx"
OUTPUT_PATH = os.path.join(os.path.dirname(INPUT_PATH), "Assessments.xlsx")

# Map Staff Name (from column 'Name') -> dbo.Staff.StaffID
STAFF_NAME_TO_ID = {
    # "Renjith Joseph": 1,
    # "Mark Richards": 2,
    # "Saima Ali": 3,
}

# Map Site Name (from your survey site column) -> dbo.Sites.SiteId
SITE_NAME_TO_ID = {
    # "Ward End Park": 1,
    # "Calthorpe Wellbeing Hub": 2,
    # "Alum Rock Community Centre": 3,
}

# If your site column header is known, put it here.
# Otherwise leave None and the script will auto-detect.
SITE_COLUMN = None  # e.g. "Take the Site" or "Site" or "Site:"


# ======================================
# HELPERS
# ======================================
def normalize_header(name: str) -> str:
    s = str(name).replace("\ufeff", "")
    s = s.strip().strip('"').strip("'")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def make_unique_columns(cols):
    seen = {}
    out = []
    for c in cols:
        if c not in seen:
            seen[c] = 0
            out.append(c)
        else:
            seen[c] += 1
            out.append(f"{c}.{seen[c]}")
    return out

def to_datetime(series):
    return pd.to_datetime(series, errors="coerce", dayfirst=True)

def parse_next_review_date(series):
    s = series.astype(str).str.replace("@", " ", regex=False)
    s = s.str.replace(r"\bam\b|\bpm\b", "", regex=True)
    s = s.str.replace(r"\.", ":", regex=True)  # 10.45 -> 10:45
    dt = pd.to_datetime(s, errors="coerce", dayfirst=True)
    return dt.dt.date

def assign_assessment_numbers(df, saheli_col, start_col, tie_col=None):
    temp = df.copy()
    temp["_saheli"] = temp[saheli_col].astype(str).str.strip()
    temp = temp[temp["_saheli"].ne("")].copy()

    sort_cols = [start_col]
    if tie_col and tie_col in temp.columns:
        sort_cols.append(tie_col)

    temp = temp.sort_values(sort_cols, ascending=True)
    temp["AssessmentNumber"] = temp.groupby("_saheli").cumcount() + 1
    return temp["AssessmentNumber"].reindex(df.index).astype("Int64")

def safe_get(df, col):
    if col in df.columns:
        return df[col]
    return pd.Series([pd.NA] * len(df))

def find_site_column(df):
    # Tries to detect a site column automatically
    candidates = []
    for col in df.columns:
        n = normalize_header(col).lower()
        if "site" in n:
            candidates.append(col)
    # Prefer "take the site" style
    for c in candidates:
        if "take the site" in normalize_header(c).lower():
            return c
    return candidates[0] if candidates else None


# ======================================
# LOAD EXCEL
# ======================================
df = pd.read_excel(INPUT_PATH, engine="openpyxl")
df.columns = [normalize_header(c) for c in df.columns]
df.columns = make_unique_columns(df.columns)

# Required columns
SAHELI_COL = "Saheli Card No:"
START_COL = "Start time"
COMPLETE_COL = "Completion time"
ID_COL = "ID"  # optional
STAFF_NAME_COL = "Name"

missing = [c for c in [SAHELI_COL, START_COL, STAFF_NAME_COL] if c not in df.columns]
if missing:
    raise ValueError(f"Missing required columns: {missing}")

df[START_COL] = to_datetime(df[START_COL])
df[COMPLETE_COL] = to_datetime(safe_get(df, COMPLETE_COL))

# AssessmentDate is a DATE in dbo.Assessments -> use start date part
df["_AssessmentDate"] = df[START_COL].dt.date

# Sequential AssessmentNumber per SaheliCardNumber
df["AssessmentNumber"] = assign_assessment_numbers(
    df=df,
    saheli_col=SAHELI_COL,
    start_col=START_COL,
    tie_col=ID_COL if ID_COL in df.columns else None,
)

# Site column
site_col = SITE_COLUMN if SITE_COLUMN else find_site_column(df)

# Next review column
NEXT_REVIEW_COL = "Date of next review appointment:"

# ======================================
# BUILD dbo.Assessments EXPORT
# ======================================
assessments = pd.DataFrame({
    "SaheliCardNumber": df[SAHELI_COL].astype(str).str.strip(),
    "AssessmentNumber": df["AssessmentNumber"].astype("Int64"),
    "AssessmentDate": df["_AssessmentDate"],

    # StaffID derived from staff name in column "Name"
    "StaffID": df[STAFF_NAME_COL].astype(str).str.strip().map(STAFF_NAME_TO_ID).astype("Int64"),

    # SiteID derived from site name column (if detected)
    "SiteID": (df[site_col].astype(str).str.strip().map(SITE_NAME_TO_ID).astype("Int64")
              if site_col else pd.Series([pd.NA] * len(df), dtype="Int64")),

    "NextReviewDate": (parse_next_review_date(df[NEXT_REVIEW_COL])
                       if NEXT_REVIEW_COL in df.columns else pd.Series([pd.NaT] * len(df))),

    "CreatedAt": df[COMPLETE_COL].fillna(df[START_COL]),
})

# Filter blanks
assessments = assessments[assessments["SaheliCardNumber"].ne("")].copy()

# Helpful debug columns so you can check mapping (remove if you don’t want them)
assessments["StaffName_DEBUG"] = df[STAFF_NAME_COL].astype(str).str.strip()
if site_col:
    assessments["SiteName_DEBUG"] = df[site_col].astype(str).str.strip()
else:
    assessments["SiteName_DEBUG"] = pd.Series([""] * len(df))

assessments.to_excel(OUTPUT_PATH, index=False)

print(f"✅ Exported dbo.Assessments extract to:\n{OUTPUT_PATH}")
print(f"Site column used: {site_col!r}")
