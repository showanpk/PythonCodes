# ============================================================
# Saheli Hub - Health Assessment Exporter (FULL SCRIPT)
# Reads one survey export Excel and splits into separate Excel files per DB table.
# Also exports dbo.Assessments (header table in your screenshot).
#
# - Assigns AssessmentNumber sequentially per SaheliCardNumber using Start time order.
# - Parses BP systolic/diastolic (e.g., "133/92")
# - Calculates BMI if missing
# - Calculates WaistHipRatio if missing
# - Handles duplicate column names like multiple "Comments:"
#
# OUTPUT: creates folder "ExportedTables" next to input Excel file.
# ============================================================

import os
import re
import pandas as pd

# -----------------------------
# CONFIG
# -----------------------------
INPUT_PATH = r"C:\Users\shonk\Downloads\Saheli Hub Health Assessment(1-1470).xlsx"
OUTPUT_DIR = os.path.join(os.path.dirname(INPUT_PATH), "ExportedTables")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# If the survey contains Site *names* (e.g., "Ward End Park"),
# map those to your dbo.Sites.SiteId values here.
# Leave empty if you want SiteID exported as blank for now.
SITE_NAME_TO_ID = {
    # "Alum Rock Community Centre": 1,
    # "Calthorpe Wellbeing Hub": 2,
    # "Omnia Medical Practice": 3,
    # "Ward End Park": 4,
    # "Handsworth": 5,
    # "St Pauls Trust": 6,
    # "Parkfield Community School": 7,
}

# Same idea for staff if you ever add a "Staff" column into the export.
STAFF_NAME_TO_ID = {
    # "Naseem": 1,
    # "Onjam": 2,
}

# -----------------------------
# Helpers
# -----------------------------
def normalize_header(name: str) -> str:
    s = str(name)
    s = s.replace("\ufeff", "")
    s = s.strip().strip('"').strip("'")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def make_unique_columns(cols):
    seen = {}
    out = []
    for c in cols:
        base = c
        if base not in seen:
            seen[base] = 0
            out.append(base)
        else:
            seen[base] += 1
            out.append(f"{base}.{seen[base]}")
    return out

def to_datetime(series):
    return pd.to_datetime(series, errors="coerce", dayfirst=True)

def to_date(series):
    return pd.to_datetime(series, errors="coerce", dayfirst=True).dt.date

def to_numeric(series):
    return pd.to_numeric(series, errors="coerce")

def yesno_to_bit(series):
    if series is None:
        return pd.Series([pd.NA])
    s = series.astype(str).str.strip().str.lower()
    return s.map({"yes": 1, "no": 0}).astype("Int64")

def parse_bp_sys_dia(series):
    systolic = []
    diastolic = []
    for v in series.fillna("").astype(str):
        m = re.search(r"(\d{2,3})\s*/\s*(\d{2,3})", v)
        if m:
            systolic.append(int(m.group(1)))
            diastolic.append(int(m.group(2)))
        else:
            systolic.append(pd.NA)
            diastolic.append(pd.NA)
    return pd.Series(systolic, dtype="Int64"), pd.Series(diastolic, dtype="Int64")

def export_excel(df, filename):
    path = os.path.join(OUTPUT_DIR, filename)
    df.to_excel(path, index=False)
    print(f"✅ Exported: {path}")

def safe_get(df, colname):
    if colname in df.columns:
        return df[colname]
    return pd.Series([pd.NA] * len(df))

def parse_next_review_date(raw_series):
    s = raw_series.astype(str).str.replace("@", " ", regex=False)
    s = s.str.replace(r"\bam\b|\bpm\b", "", regex=True)
    s = s.str.replace(r"\.", ":", regex=True)  # 10.45 -> 10:45
    dt = pd.to_datetime(s, errors="coerce", dayfirst=True)
    return dt.dt.date

def assign_assessment_numbers(df, saheli_col, start_dt_col, tie_id_col=None):
    temp = df.copy()
    temp["_saheli"] = temp[saheli_col].astype(str).str.strip()
    temp = temp[temp["_saheli"].ne("")].copy()

    sort_cols = [start_dt_col]
    if tie_id_col and tie_id_col in temp.columns:
        sort_cols.append(tie_id_col)

    temp = temp.sort_values(sort_cols, ascending=True)
    temp["AssessmentNumber"] = temp.groupby("_saheli").cumcount() + 1
    return temp["AssessmentNumber"].reindex(df.index)

def find_first_matching_column(df, patterns):
    """
    Find a column whose normalized name matches any regex pattern (case-insensitive).
    Returns the column name or None.
    """
    for col in df.columns:
        n = normalize_header(col).lower()
        for pat in patterns:
            if re.search(pat, n, flags=re.IGNORECASE):
                return col
    return None

# -----------------------------
# Load Excel
# -----------------------------
df = pd.read_excel(INPUT_PATH, engine="openpyxl")

df.columns = [normalize_header(c) for c in df.columns]
df.columns = make_unique_columns(df.columns)

print(f"Loaded {len(df)} rows and {len(df.columns)} columns from Excel.")

# -----------------------------
# Required key columns
# -----------------------------
COL_SAHELI = "Saheli Card No:"
COL_START = "Start time"
COL_COMPLETE = "Completion time"
COL_ID = "ID"

if COL_SAHELI not in df.columns:
    raise ValueError(f"Missing required column: {COL_SAHELI}")
if COL_START not in df.columns:
    raise ValueError(f"Missing required column: {COL_START}")

df[COL_START] = to_datetime(df[COL_START])
df[COL_COMPLETE] = to_datetime(safe_get(df, COL_COMPLETE))

df["_AssessmentDate_date"] = df[COL_START].dt.date  # date for child tables

df["AssessmentNumber"] = assign_assessment_numbers(
    df=df,
    saheli_col=COL_SAHELI,
    start_dt_col=COL_START,
    tie_id_col=COL_ID if COL_ID in df.columns else None
).astype("Int64")

# -----------------------------
# Base keys for child tables
# -----------------------------
base_keys = pd.DataFrame({
    "SaheliCardNumber": df[COL_SAHELI].astype(str).str.strip(),
    "AssessmentNumber": df["AssessmentNumber"].astype("Int64"),
    "AssessmentDate": df["_AssessmentDate_date"],
})
base_keys = base_keys[base_keys["SaheliCardNumber"].ne("")].copy()

# -----------------------------
# Try to detect a "Site" column in the survey
# (because dbo.Assessments has SiteID)
# -----------------------------
site_col = find_first_matching_column(df, [
    r"\btake the site\b",
    r"^site\b",
    r"\bsite:\b",
    r"\bsite name\b",
])
site_name_series = safe_get(df, site_col) if site_col else pd.Series([pd.NA] * len(df))

# Export unique sites for quick mapping
unique_sites = (
    pd.DataFrame({"SiteName": site_name_series.astype(str).str.strip()})
    .replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
    .dropna()
    .drop_duplicates()
    .sort_values("SiteName")
)
export_excel(unique_sites, "Unique_Sites_From_Survey.xlsx")

# -----------------------------
# Try to detect a "Staff" column (optional)
# -----------------------------
staff_col = find_first_matching_column(df, [
    r"\bstaff\b",
    r"\bassessor\b",
    r"\bcoach\b",
    r"\bdelivered by\b",
])
staff_name_series = safe_get(df, staff_col) if staff_col else pd.Series([pd.NA] * len(df))

# -----------------------------
# 1) dbo.Assessment_Master (your master/header)
# -----------------------------
COL_NEXT_REVIEW = "Date of next review appointment:"

master = pd.DataFrame({
    "SaheliCardNumber": df[COL_SAHELI].astype(str).str.strip(),
    "AssessmentNumber": df["AssessmentNumber"].astype("Int64"),
    # Your Assessment_Master has datetime2 for AssessmentDate in your sample
    "AssessmentDate": df[COL_START],
    "CreatedAtUtc": df[COL_COMPLETE].fillna(df[COL_START]),
    "CreatedByUserId": pd.Series([pd.NA] * len(df)),  # set during DB import if needed
    "SubmissionStartTime": df[COL_START],
    "SubmissionCompletionTime": df[COL_COMPLETE],
    "NextReviewDate": parse_next_review_date(safe_get(df, COL_NEXT_REVIEW)) if COL_NEXT_REVIEW in df.columns else pd.Series([pd.NaT] * len(df)),
})

master = master[master["SaheliCardNumber"].ne("")].copy()
export_excel(master, "Assessment_Master.xlsx")

# -----------------------------
# 2) dbo.Assessments  (TABLE IN YOUR SCREENSHOT)
# Columns: AssessmentID, SaheliCardNumber, AssessmentNumber, AssessmentDate, StaffID, SiteID, NextReviewDate, CreatedAt
# NOTE: We cannot create AssessmentID (identity). So export WITHOUT AssessmentID.
# StaffID/SiteID are best derived via mapping dictionaries.
# -----------------------------
assessments = pd.DataFrame({
    "SaheliCardNumber": df[COL_SAHELI].astype(str).str.strip(),
    "AssessmentNumber": df["AssessmentNumber"].astype("Int64"),
    # dbo.Assessments.AssessmentDate is date in your schema -> use date
    "AssessmentDate": df["_AssessmentDate_date"],

    # StaffID: map from detected staff name if present; else blank
    "StaffID": staff_name_series.astype(str).str.strip().map(STAFF_NAME_TO_ID).astype("Int64"),

    # SiteID: map from detected site name if present; else blank
    "SiteID": site_name_series.astype(str).str.strip().map(SITE_NAME_TO_ID).astype("Int64"),

    "NextReviewDate": parse_next_review_date(safe_get(df, COL_NEXT_REVIEW)) if COL_NEXT_REVIEW in df.columns else pd.Series([pd.NaT] * len(df)),

    # CreatedAt: use completion time if exists else start time
    "CreatedAt": df[COL_COMPLETE].fillna(df[COL_START]),
})

assessments = assessments[assessments["SaheliCardNumber"].ne("")].copy()

# Helpful debug columns (NOT for DB import) — comment these out if you want:
assessments["SiteName_DEBUG"] = site_name_series.astype(str).str.strip()
assessments["StaffName_DEBUG"] = staff_name_series.astype(str).str.strip()

export_excel(assessments, "Assessments.xlsx")

# -----------------------------
# 3) dbo.Assessment_BodyComposition
# -----------------------------
bc = base_keys.copy()
bc["WeightKg"] = to_numeric(safe_get(df, "Weight (KG):"))
bc["HeightCm"] = to_numeric(safe_get(df, "Height (CM):"))
bc["Bmivalue"] = to_numeric(safe_get(df, "BMI:"))
bc["Bmicategory"] = safe_get(df, "BMI Results:")
bc["WaistCm"] = to_numeric(safe_get(df, "Waist (CM):"))
bc["HipCm"] = to_numeric(safe_get(df, "Hip (CM):"))
bc["WaistHipRatio"] = to_numeric(safe_get(df, "Waist to Hip Ratio (CM):"))

# BMI auto-calc if missing (BMI = kg / (m^2))
mask_bmi = bc["Bmivalue"].isna() & bc["WeightKg"].notna() & bc["HeightCm"].notna() & (bc["HeightCm"] > 0)
height_m = (bc.loc[mask_bmi, "HeightCm"] / 100.0)
bc.loc[mask_bmi, "Bmivalue"] = (bc.loc[mask_bmi, "WeightKg"] / (height_m * height_m)).round(2)

# WaistHipRatio auto-calc if missing
mask_whr = bc["WaistHipRatio"].isna() & bc["WaistCm"].notna() & bc["HipCm"].notna() & (bc["HipCm"] != 0)
bc.loc[mask_whr, "WaistHipRatio"] = (bc.loc[mask_whr, "WaistCm"] / bc.loc[mask_whr, "HipCm"]).round(4)

bc["BodyFatCategory"] = safe_get(df, "Body Fat Percentage Result:")
bc["BodyFatScore"] = to_numeric(safe_get(df, "Body Fat Percentage Score:"))
bc["VisceralFatCategory"] = safe_get(df, "Visceral Fat Level Result:")
bc["VisceralFatScore"] = to_numeric(safe_get(df, "Visceral Fat Level Score:"))
bc["SkeletalMuscleCategory"] = safe_get(df, "Skeletal Muscle Percentage:")
bc["SkeletalMuscleScore"] = to_numeric(safe_get(df, "Skeletal Muscle Score:"))
bc["RestingMetabolism"] = to_numeric(safe_get(df, "Resting Metabolism:"))
export_excel(bc, "Assessment_BodyComposition.xlsx")

# -----------------------------
# 4) dbo.Assessment_HealthScreening
# -----------------------------
bp_raw_col = "Blood Pressure (Systolic/Diastolic):"
sys, dia = parse_bp_sys_dia(safe_get(df, bp_raw_col))

# Your export has multiple "Comments:" columns:
# "Comments:" is health comments; "Comments:.1" is physical activity comments.
HEALTH_COMMENTS_COL = "Comments:" if "Comments:" in df.columns else "Comments"

hs = base_keys.copy()
hs["HasHealthCondition"] = yesno_to_bit(safe_get(df, "Do You Have Any Health Condition?"))
hs["LastBpmeasurementDate"] = to_date(safe_get(df, "When did you last measure your blood pressure?"))
hs["BprecordedWithGp"] = safe_get(df, "Have you recorded your blood pressure measurement and registered it with a GP or Pharmacist? (Yes/No/Not sure)")
hs["KnowledgeHealthyBp"] = safe_get(df, "What is a healthy blood pressure for an adult?")
hs["KnowledgeBprisk"] = safe_get(df, "Why is a high blood pressure dangerous?")
hs["KnowledgeBpreduction"] = safe_get(df, "How can you help reduce your blood pressure?")
hs["SystolicBp"] = sys
hs["DiastolicBp"] = dia
hs["Bplevel"] = safe_get(df, "Blood Pressure Level:")
hs["HeartConditionTypes"] = safe_get(df, "Do You Have a Heart Condition?")
hs["HeartRateBpm"] = to_numeric(safe_get(df, "Heart Rate (BPM):")).astype("Int64")
hs["AtrialFibrillationResult"] = to_numeric(safe_get(df, "Atrial Fibrillation Result:")).astype("Int64")
hs["HeartAge"] = to_numeric(safe_get(df, "Heart Age:")).astype("Int64")
hs["DoctorAdvisedNoExercise"] = yesno_to_bit(safe_get(df, "Did Your Doctor Advise You Not to Exercise?"))
hs["ChestPain"] = yesno_to_bit(safe_get(df, "Do You Feel Pain in Chest at Rest/During Activity?"))
hs["ShortnessOfBreath"] = safe_get(df, "Do You Have Shortness of Breath?")
hs["DiabetesType"] = safe_get(df, "Do You Have Diabetes?")
hs["DiabetesRisk"] = safe_get(df, "Diabetes Risk:")
hs["GlucoseLevel"] = to_numeric(safe_get(df, "Glucose Level ( mg/dL):"))
hs["HbA1c"] = to_numeric(safe_get(df, "HbA1c:"))
hs["SugaryDrinkIntake"] = yesno_to_bit(safe_get(df, "Do You Take Sugary Drinks, Including Chai?"))

chol_col = None
for c in df.columns:
    if normalize_header(c).lower().startswith("do you have high cholesterol"):
        chol_col = c
        break
hs["HighCholesterol"] = yesno_to_bit(safe_get(df, chol_col)) if chol_col else pd.Series([pd.NA] * len(df), dtype="Int64")

hs["OtherHealthIssues"] = safe_get(df, "Do You Experience The Following Health Issues?")
hs["BoneJointConditions"] = safe_get(df, "Do You Have a Bone / joint Condition?")
hs["TakesPrescribedMedication"] = yesno_to_bit(safe_get(df, "Do You Take Any Prescribed Medication?"))
hs["ReferredToDoctor"] = yesno_to_bit(safe_get(df, "Referred to doctor for any concerning results?"))
hs["RiskStratification"] = safe_get(df, "Risk Stratification Score")
hs["HealthComments"] = safe_get(df, HEALTH_COMMENTS_COL)
hs["SelfManagementScore"] = to_numeric(safe_get(df, "How well do you manage your health/condition(s)? (Rating out of 10)")).astype("Int64")
export_excel(hs, "Assessment_HealthScreening.xlsx")

# -----------------------------
# 5) dbo.Assessment_PhysicalActivity
# -----------------------------
PHYS_COMMENTS_COL = "Comments:.1" if "Comments:.1" in df.columns else None

pa = base_keys.copy()
pa["ActiveDaysPerWeek"] = to_numeric(
    safe_get(df, "In the past week, on how many days have you done a total of 30 mins or more of physical activity, which was enough to raise your breathing rate?")
).astype("Int64")
pa["ActivityLevel"] = safe_get(df, "Physical Activity Level:")
pa["ActivityComments"] = safe_get(df, PHYS_COMMENTS_COL) if PHYS_COMMENTS_COL else pd.Series([pd.NA] * len(df))
export_excel(pa, "Assessment_PhysicalActivity.xlsx")

# -----------------------------
# 6) dbo.Assessment_WEMWBS
# -----------------------------
wem = base_keys.copy()
wem["FeelingOptimistic"] = to_numeric(safe_get(df, "I’ve been feeling optimistic about the future")).astype("Int64")
wem["FeelingUseful"] = to_numeric(safe_get(df, "I’ve been feeling useful")).astype("Int64")
wem["FeelingRelaxed"] = to_numeric(safe_get(df, "I’ve been feeling relaxed")).astype("Int64")
wem["FeelingInterestedInPeople"] = to_numeric(safe_get(df, "I’ve been feeling interested in other people")).astype("Int64")
wem["EnergyToSpare"] = to_numeric(safe_get(df, "I’ve had energy to spare")).astype("Int64")
wem["DealingWithProblems"] = to_numeric(safe_get(df, "I’ve been dealing with problems well")).astype("Int64")
wem["ThinkingClearly"] = to_numeric(safe_get(df, "I’ve been thinking clearly")).astype("Int64")
wem["FeelingGoodAboutSelf"] = to_numeric(safe_get(df, "I’ve been feeling good about myself")).astype("Int64")
wem["FeelingCloseToOthers"] = to_numeric(safe_get(df, "I’ve been feeling close to other people")).astype("Int64")
wem["FeelingConfident"] = to_numeric(safe_get(df, "I’ve been feeling confident")).astype("Int64")
wem["MakingOwnMindUp"] = to_numeric(safe_get(df, "I’ve been able to make up my own mind about things")).astype("Int64")
wem["FeelingLoved"] = to_numeric(safe_get(df, "I’ve been feeling loved")).astype("Int64")
wem["InterestedInNewThings"] = to_numeric(safe_get(df, "I’ve been interested in new things")).astype("Int64")
wem["FeelingCheerful"] = to_numeric(safe_get(df, "I’ve been feeling cheerful")).astype("Int64")
wem["Wemwbscomments"] = safe_get(df, "Comments:2")
export_excel(wem, "Assessment_WEMWBS.xlsx")

# -----------------------------
# 7) dbo.Assessment_Lifestyle
# -----------------------------
life = base_keys.copy()
life["Nourishment"] = to_numeric(safe_get(df, "Nourishment: Rate the quality of the food you put into your body on a daily basis")).astype("Int64")
life["Movement"] = to_numeric(safe_get(df, "Movement: Rate how often and for how long you move your body on a daily basis")).astype("Int64")
life["Connectedness"] = to_numeric(safe_get(df, "Connectedness: Rate how well you stay connected with family, friends and your higher power")).astype("Int64")
life["SleepQuality"] = to_numeric(safe_get(df, "Sleep: Rate the quality of your sleep")).astype("Int64")
life["HappySelf"] = to_numeric(safe_get(df, "Happy self: Rate how often and for how long you perform positive practices (gratitude, virtue awareness, meditation, prayer, etc.)")).astype("Int64")
life["Resilience"] = to_numeric(safe_get(df, "Resilience: Rate how well you are able to manage stress in your life")).astype("Int64")
life["GreenBlueSpace"] = to_numeric(safe_get(df, "Green and Blue: Rate how often and how long you spend in nature or outdoors")).astype("Int64")
life["ScreenTime"] = to_numeric(safe_get(df, "Screen time: Rate how happy you are with your current amount of screen time")).astype("Int64")
life["SubstanceUse"] = to_numeric(safe_get(df, "Substance use: Rate how comfortable you are with any current substance use (smoking, alcohol, drugs)")).astype("Int64")
life["Purpose"] = to_numeric(safe_get(df, "Purpose: Rate how well you feel you are fulfilling your passion, purpose or vocation in life")).astype("Int64")
life["LifestyleComments"] = safe_get(df, "Comments:3")
export_excel(life, "Assessment_Lifestyle.xlsx")

# -----------------------------
# 8) dbo.Assessment_SocialIsolation
# -----------------------------
si = base_keys.copy()
si["LackCompanionship"] = to_numeric(safe_get(df, "How often do you feel that you lack companionship?")).astype("Int64")
si["FeelLeftOut"] = to_numeric(safe_get(df, "How often do you feel left out?")).astype("Int64")
si["FeelIsolated"] = to_numeric(safe_get(df, "How often do you feel isolated from others?")).astype("Int64")
si["SocialIsolationComments"] = safe_get(df, "Comments:4")
export_excel(si, "Assessment_SocialIsolation.xlsx")

# -----------------------------
# 9) dbo.Assessment_CommunityConfidence
# -----------------------------
cc = base_keys.copy()
cc["ConfidenceToJoin"] = to_numeric(safe_get(df, "How confident are you to join activities?")).astype("Int64")
cc["NumberOfHobbies"] = to_numeric(safe_get(df, "How many hobbies and passions do you have?")).astype("Int64")
cc["CommunityInvolvement"] = to_numeric(safe_get(df, "How involved you feel in your community?")).astype("Int64")
cc["ServiceAwareness"] = to_numeric(safe_get(df, "How much you know about local support/services?")).astype("Int64")
export_excel(cc, "Assessment_CommunityConfidence.xlsx")

# -----------------------------
# 10) dbo.Assessment_AimsGoals
# -----------------------------
ag = base_keys.copy()
ag["AimsGoals"] = safe_get(df, "What are your aims & goals?")
ag["AimsDescription"] = safe_get(df, "Comments:5")
export_excel(ag, "Assessment_AimsGoals.xlsx")

# -----------------------------
# 11) dbo.Assessment_Barriers
# -----------------------------
bar = base_keys.copy()
bar["Barriers"] = safe_get(df, "What reasons stop you from joining activities?")
bar["BarrierComments"] = safe_get(df, "Comments:6")
export_excel(bar, "Assessment_Barriers.xlsx")

# -----------------------------
# 12) dbo.Assessment_PreferredActivities
# -----------------------------
pref = base_keys.copy()
pref["PreferredActivities"] = safe_get(df, "What are your preferred activities?")
pref["ActivityComments"] = safe_get(df, "Comments:7")
pref["NextReviewDate"] = parse_next_review_date(safe_get(df, COL_NEXT_REVIEW)) if COL_NEXT_REVIEW in df.columns else pd.Series([pd.NaT] * len(df))
export_excel(pref, "Assessment_PreferredActivities.xlsx")

print("\n✅ DONE. Files exported into:")
print(OUTPUT_DIR)

if not SITE_NAME_TO_ID:
    print("\n⚠️ SiteID is blank because SITE_NAME_TO_ID mapping is empty.")
    print("Fill SITE_NAME_TO_ID in this script using your dbo.Sites IDs, then re-run to populate SiteID.")
