import os
import re
import math
import pandas as pd
import pyodbc
from datetime import datetime, date

# =========================
# CONFIG
# =========================
CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=MIGHTYSUPERMAN;"
    "DATABASE=SahelihubCRM;"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
)

SOURCE_TABLE = "dbo.HealthAssessment_Full"

FAILED_FILE = r"C:\Users\shonk\source\PythonCodes\FAILED_Assessment_Imports.xlsx"

# If your source column is named exactly like this (as you showed):
COL_CARD = "SaheliCardNo:"
COL_ASSESSMENT_NO = "AssessmentNumber"
COL_ASSESSMENT_DATE = "Real date"   # fallback to "Start time" if missing/blank

# =========================
# HELPERS
# =========================
def clean(v):
    if v is None:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    if isinstance(v, str):
        s = v.strip()
        return s if s else None
    return v

def parse_date(v):
    v = clean(v)
    if v is None:
        return None
    if isinstance(v, (datetime, pd.Timestamp)):
        return v.date()
    if isinstance(v, date):
        return v
    d = pd.to_datetime(str(v), errors="coerce", dayfirst=True)
    if pd.isna(d):
        return None
    return d.date()

def to_int(v):
    v = clean(v)
    if v is None:
        return None
    try:
        # handles "72", "72.0", etc.
        return int(float(str(v)))
    except:
        return None

def to_float(v):
    v = clean(v)
    if v is None:
        return None
    try:
        return float(str(v))
    except:
        return None

def to_bool(v):
    """
    Converts typical Yes/No/True/False/1/0 into bit/boolean.
    Returns None if unknown.
    """
    v = clean(v)
    if v is None:
        return None
    s = str(v).strip().lower()
    if s in ("yes", "y", "true", "1"):
        return 1
    if s in ("no", "n", "false", "0"):
        return 0
    return None

def parse_bp(bp):
    """
    Source column: 'Blood Pressure (Systolic/Diastolic):' e.g. '114/73'
    returns (systolic:int, diastolic:int)
    """
    bp = clean(bp)
    if not bp:
        return (None, None)
    s = str(bp).strip()
    m = re.match(r"^\s*(\d+)\s*/\s*(\d+)\s*$", s)
    if not m:
        return (None, None)
    return (to_int(m.group(1)), to_int(m.group(2)))

def ensure_dir_for_file(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)

# =========================
# LOAD SOURCE DATA
# =========================
conn = pyodbc.connect(CONN_STR)
df = pd.read_sql(f"SELECT * FROM {SOURCE_TABLE}", conn)
conn.close()

# Clean all cells (pandas v3 removed applymap, so do column-wise map)
df = df.apply(lambda col: col.map(clean))

# Check required columns exist
missing = [c for c in [COL_CARD, COL_ASSESSMENT_NO] if c not in df.columns]
if missing:
    raise Exception(f"Missing required columns in {SOURCE_TABLE}: {missing}\nAvailable: {list(df.columns)}")

# Prepare key fields
df["SaheliCardNumber"] = df[COL_CARD].map(clean)
df["AssessmentNumberClean"] = df[COL_ASSESSMENT_NO].map(to_int)

# Date
if COL_ASSESSMENT_DATE in df.columns:
    df["AssessmentDate"] = df[COL_ASSESSMENT_DATE].map(parse_date)
else:
    df["AssessmentDate"] = None

# fallback date if AssessmentDate missing: use Start time
if df["AssessmentDate"].isna().all():
    if "Start time" in df.columns:
        df["AssessmentDate"] = df["Start time"].map(parse_date)

# Keep only valid keys
df_valid = df.dropna(subset=["SaheliCardNumber", "AssessmentNumberClean", "AssessmentDate"]).copy()

# =========================
# SQL STATEMENTS (10 TABLES)
# =========================
SQL_AIMS = """
INSERT INTO dbo.Assessment_AimsGoals
(SaheliCardNumber, AssessmentNumber, AssessmentDate, AimsGoals, AimsDescription)
VALUES (?, ?, ?, ?, ?)
"""

SQL_BARRIERS = """
INSERT INTO dbo.Assessment_Barriers
(SaheliCardNumber, AssessmentNumber, AssessmentDate, Barriers, BarrierComments)
VALUES (?, ?, ?, ?, ?)
"""

SQL_BODY = """
INSERT INTO dbo.Assessment_BodyComposition
(SaheliCardNumber, AssessmentNumber, AssessmentDate,
 WeightKg, HeightCm, BMICategory, BMIValue,
 WaistCm, HipCm, WaistHipRatio,
 BodyFatCategory, BodyFatScore,
 VisceralFatCategory, VisceralFatScore,
 SkeletalMuscleCategory, SkeletalMuscleScore,
 RestingMetabolism)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

SQL_COMMUNITY = """
INSERT INTO dbo.Assessment_CommunityConfidence
(SaheliCardNumber, AssessmentNumber, AssessmentDate,
 ConfidenceToJoin, NumberOfHobbies, CommunityInvolvement, ServiceAwareness)
VALUES (?, ?, ?, ?, ?, ?, ?)
"""

SQL_HEALTH = """
INSERT INTO dbo.Assessment_HealthScreening
(SaheliCardNumber, AssessmentNumber, AssessmentDate,
 HasHealthCondition, LastBPMeasurementDate, BPRecordedWithGP,
 KnowledgeHealthyBP, KnowledgeBPRisk, KnowledgeBPReduction,
 SystolicBP, DiastolicBP, BPLevel,
 HeartConditionTypes, HeartRateBPM, AtrialFibrillationResult, HeartAge,
 DoctorAdvisedNoExercise, ChestPain, ShortnessOfBreath,
 DiabetesType, DiabetesRisk, GlucoseLevel, HbA1c,
 SugaryDrinkIntake, HighCholesterol, OtherHealthIssues,
 BoneJointConditions, TakesPrescribedMedication, ReferredToDoctor,
 RiskStratification, HealthComments, SelfManagementScore)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

SQL_LIFESTYLE = """
INSERT INTO dbo.Assessment_Lifestyle
(SaheliCardNumber, AssessmentNumber, AssessmentDate,
 Nourishment, Movement, Connectedness, SleepQuality, HappySelf,
 Resilience, GreenBlueSpace, ScreenTime, SubstanceUse, Purpose, LifestyleComments)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

SQL_ACTIVITY = """
INSERT INTO dbo.Assessment_PhysicalActivity
(SaheliCardNumber, AssessmentNumber, AssessmentDate,
 ActiveDaysPerWeek, ActivityLevel, ActivityComments)
VALUES (?, ?, ?, ?, ?, ?)
"""

SQL_PREF = """
INSERT INTO dbo.Assessment_PreferredActivities
(SaheliCardNumber, AssessmentNumber, AssessmentDate,
 PreferredActivities, ActivityComments, NextReviewDate)
VALUES (?, ?, ?, ?, ?, ?)
"""

SQL_SOCIAL = """
INSERT INTO dbo.Assessment_SocialIsolation
(SaheliCardNumber, AssessmentNumber, AssessmentDate,
 LackCompanionship, FeelLeftOut, FeelIsolated, SocialIsolationComments)
VALUES (?, ?, ?, ?, ?, ?, ?)
"""

SQL_WEMWBS = """
INSERT INTO dbo.Assessment_WEMWBS
(SaheliCardNumber, AssessmentNumber, AssessmentDate,
 FeelingOptimistic, FeelingUseful, FeelingRelaxed, FeelingInterestedInPeople,
 EnergyToSpare, DealingWithProblems, ThinkingClearly, FeelingGoodAboutSelf,
 FeelingCloseToOthers, FeelingConfident, MakingOwnMindUp, FeelingLoved,
 InterestedInNewThings, FeelingCheerful, WEMWBSComments)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

# =========================
# COLUMN MAPPINGS (SOURCE -> TARGET)
# =========================
# NOTE: These match your HealthAssessment_Full column names exactly as you posted.
SRC = {
    "weight": "Weight (KG):",
    "height": "Height (CM):",
    "bmi_value": "BMI:",
    "bmi_category": "BMI Results:",
    "waist": "Waist (CM):",
    "hip": "Hip (CM):",
    "whr": "Waist to Hip Ratio (CM):",
    "bf_cat": "Body Fat Percentage Result:",
    "bf_score": "Body Fat Percentage Score:",
    "vf_cat": "Visceral Fat Level Result:",
    "vf_score": "Visceral Fat Level Score:",
    "sm_cat": "Skeletal Muscle Percentage:",  # stored into SkeletalMuscleCategory (kept as main sheet for now)
    "sm_score": "Skeletal Muscle Score:",
    "rm": "Resting Metabolism:",

    "has_health_cond": "Do You Have Any Health Condition?",
    "last_bp_date": "When did you last measure your blood pressure?",
    "bp_recorded": "Have you recorded your blood pressure measurement and registered it with a GP or Pharmacist? (Yes/No/Not sure)",
    "know_healthy": "What is a healthy blood pressure for an adult?",
    "know_risk": "Why is a high blood pressure dangerous?",
    "know_reduce": "How can you help reduce your blood pressure?",
    "bp_reading": "Blood Pressure (Systolic/Diastolic):",
    "bp_level": "Blood Pressure Level:",
    "heart_cond": "Do You Have a Heart Condition?",
    "hr": "Heart Rate (BPM):",
    "af": "Atrial Fibrillation Result:",
    "heart_age": "Heart Age:",
    "no_ex": "Did Your Doctor Advise You Not to Exercise?",
    "chest_pain": "Do You Feel Pain in Chest at Rest/During Activity?",
    "sob": "Do You Have Shortness of Breath?",
    "diabetes": "Do You Have Diabetes?",
    "diabetes_risk": "Diabetes Risk:",
    "glucose": "Glucose Level ( mg/dL):",
    "hba1c": "HbA1c:",
    "sugary": "Do You Take Sugary Drinks, Including Chai?",
    "chol": "Do You Have High Cholesterol? (Total/HDL)",
    "other_issues": "Do You Experience The Following Health Issues?",
    "bone": "Do You Have a Bone / joint Condition?",
    "meds": "Do You Take Any Prescribed Medication?",
    "referred": "Referred to doctor for any concerning results?",
    "risk_score": "Risk Stratification Score",
    "comments_health": "Comments:",
    "self_manage": "How well do you manage your health/condition(s)? (Rating out of 10)",

    "active_days": "In the past week, on how many days have you done a total of 30 mins or more of physical activity, which was enough to ra",
    "activity_level": "Physical Activity Level:",

    "wem_opt": "I’ve been feeling optimistic about the future",
    "wem_use": "I’ve been feeling useful",
    "wem_rel": "I’ve been feeling relaxed",
    "wem_int": "I’ve been feeling interested in other people",
    "wem_eng": "I’ve had energy to spare",
    "wem_prob": "I’ve been dealing with problems well",
    "wem_think": "I’ve been thinking clearly",
    "wem_good": "I’ve been feeling good about myself",
    "wem_close": "I’ve been feeling close to other people",
    "wem_conf": "I’ve been feeling confident",
    "wem_mind": "I’ve been able to make up my own mind about things",
    "wem_loved": "I’ve been feeling loved",
    "wem_new": "I’ve been interested in new things",
    "wem_cheer": "I’ve been feeling cheerful",
    "comments_wem": "Comments:2",

    "life_nour": "Nourishment: Rate the quality of the food you put into your body on a daily basis",
    "life_move": "Movement: Rate how often and for how long you move your body on a daily basis",
    "life_conn": "Connectedness: Rate how well you stay connected with family, friends and your higher power",
    "life_sleep": "Sleep: Rate the quality of your sleep",
    "life_happy": "Happy self: Rate how often and for how long you perform positive practices (gratitude, virtue awareness, meditation, pra",
    "life_res": "Resilience: Rate how well you are able to manage stress in your life",
    "life_green": "Green and Blue: Rate how often and how long you spend in nature or outdoors",
    "life_screen": "Screen time: Rate how happy you are with your current amount of screen time",
    "life_sub": "Substance use: Rate how comfortable you are with any current substance use (smoking, alcohol, drugs)",
    "life_purp": "Purpose: Rate how well you feel you are fulfilling your passion, purpose or vocation in life",
    "comments_life": "Comments:3",

    "lack": "How often do you feel that you lack companionship?",
    "leftout": "How often do you feel left out?",
    "isolated": "How often do you feel isolated from others?",
    "comments_social": "Comments:4",

    "conf_join": "How confident are you to join activities?",
    "hobbies": "How many hobbies and passions do you have?",
    "community": "How involved you feel in your community?",
    "services": "How much you know about local support/services?",

    "aims": "What are your aims & goals?",
    "comments_aims": "Comments:5",

    "barriers": "What reasons stop you from joining activities?",
    "comments_barriers": "Comments:6",

    "pref": "What are your preferred activities?",
    "comments_pref": "Comments:7",
    "next_review": "Date of next review appointment:",
}

def getv(row, colname):
    if colname not in row.index:
        return None
    return clean(row[colname])

# =========================
# INSERT LOOP
# =========================
conn = pyodbc.connect(CONN_STR)
cur = conn.cursor()

failed = []
ok_counts = {
    "AimsGoals": 0,
    "Barriers": 0,
    "BodyComposition": 0,
    "CommunityConfidence": 0,
    "HealthScreening": 0,
    "Lifestyle": 0,
    "PhysicalActivity": 0,
    "PreferredActivities": 0,
    "SocialIsolation": 0,
    "WEMWBS": 0,
}

def log_fail(target, row, ex):
    failed.append({
        "TargetTable": target,
        "SaheliCardNumber": row["SaheliCardNumber"],
        "AssessmentNumber": row["AssessmentNumberClean"],
        "AssessmentDate": row["AssessmentDate"],
        "Error": str(ex)
    })

for _, r in df_valid.iterrows():
    card = r["SaheliCardNumber"]
    anum = int(r["AssessmentNumberClean"])
    adate = r["AssessmentDate"]

    try:
        # ------------------ AIMS ------------------
        cur.execute(SQL_AIMS, card, anum, adate,
                    getv(r, SRC["aims"]),
                    getv(r, SRC["comments_aims"]))
        ok_counts["AimsGoals"] += 1
    except Exception as ex:
        log_fail("Assessment_AimsGoals", r, ex)

    try:
        # ------------------ BARRIERS ------------------
        cur.execute(SQL_BARRIERS, card, anum, adate,
                    getv(r, SRC["barriers"]),
                    getv(r, SRC["comments_barriers"]))
        ok_counts["Barriers"] += 1
    except Exception as ex:
        log_fail("Assessment_Barriers", r, ex)

    try:
        # ------------------ BODY ------------------
        cur.execute(SQL_BODY, card, anum, adate,
                    to_float(getv(r, SRC["weight"])),
                    to_float(getv(r, SRC["height"])),
                    getv(r, SRC["bmi_category"]),
                    to_float(getv(r, SRC["bmi_value"])),
                    to_float(getv(r, SRC["waist"])),
                    to_float(getv(r, SRC["hip"])),
                    to_float(getv(r, SRC["whr"])),
                    getv(r, SRC["bf_cat"]),
                    getv(r, SRC["bf_score"]),
                    getv(r, SRC["vf_cat"]),
                    getv(r, SRC["vf_score"]),
                    getv(r, SRC["sm_cat"]),   # keeping main sheet style for now
                    getv(r, SRC["sm_score"]),
                    getv(r, SRC["rm"]))
        ok_counts["BodyComposition"] += 1
    except Exception as ex:
        log_fail("Assessment_BodyComposition", r, ex)

    try:
        # ------------------ COMMUNITY CONFIDENCE ------------------
        cur.execute(SQL_COMMUNITY, card, anum, adate,
                    getv(r, SRC["conf_join"]),
                    getv(r, SRC["hobbies"]),
                    getv(r, SRC["community"]),
                    getv(r, SRC["services"]))
        ok_counts["CommunityConfidence"] += 1
    except Exception as ex:
        log_fail("Assessment_CommunityConfidence", r, ex)

    try:
        # ------------------ HEALTH SCREENING ------------------
        sys_bp, dia_bp = parse_bp(getv(r, SRC["bp_reading"]))

        cur.execute(SQL_HEALTH, card, anum, adate,
                    to_bool(getv(r, SRC["has_health_cond"])),
                    parse_date(getv(r, SRC["last_bp_date"])),
                    getv(r, SRC["bp_recorded"]),
                    getv(r, SRC["know_healthy"]),
                    getv(r, SRC["know_risk"]),
                    getv(r, SRC["know_reduce"]),
                    sys_bp,
                    dia_bp,
                    getv(r, SRC["bp_level"]),
                    getv(r, SRC["heart_cond"]),       # no "types" in sheet; keep as per main sheet
                    to_int(getv(r, SRC["hr"])),
                    to_int(getv(r, SRC["af"])),
                    to_int(getv(r, SRC["heart_age"])),
                    to_bool(getv(r, SRC["no_ex"])),
                    to_bool(getv(r, SRC["chest_pain"])),
                    getv(r, SRC["sob"]),
                    getv(r, SRC["diabetes"]),          # no type in sheet; keep as per main sheet
                    getv(r, SRC["diabetes_risk"]),
                    to_float(getv(r, SRC["glucose"])),
                    to_float(getv(r, SRC["hba1c"])),
                    to_bool(getv(r, SRC["sugary"])),
                    to_bool(getv(r, SRC["chol"])),
                    getv(r, SRC["other_issues"]),
                    getv(r, SRC["bone"]),
                    to_bool(getv(r, SRC["meds"])),
                    to_bool(getv(r, SRC["referred"])),
                    getv(r, SRC["risk_score"]),
                    getv(r, SRC["comments_health"]),
                    to_int(getv(r, SRC["self_manage"])))
        ok_counts["HealthScreening"] += 1
    except Exception as ex:
        log_fail("Assessment_HealthScreening", r, ex)

    try:
        # ------------------ LIFESTYLE ------------------
        cur.execute(SQL_LIFESTYLE, card, anum, adate,
                    to_int(getv(r, SRC["life_nour"])),
                    to_int(getv(r, SRC["life_move"])),
                    to_int(getv(r, SRC["life_conn"])),
                    to_int(getv(r, SRC["life_sleep"])),
                    to_int(getv(r, SRC["life_happy"])),
                    to_int(getv(r, SRC["life_res"])),
                    to_int(getv(r, SRC["life_green"])),
                    to_int(getv(r, SRC["life_screen"])),
                    to_int(getv(r, SRC["life_sub"])),
                    to_int(getv(r, SRC["life_purp"])),
                    getv(r, SRC["comments_life"]))
        ok_counts["Lifestyle"] += 1
    except Exception as ex:
        log_fail("Assessment_Lifestyle", r, ex)

    try:
        # ------------------ PHYSICAL ACTIVITY ------------------
        cur.execute(SQL_ACTIVITY, card, anum, adate,
                    to_int(getv(r, SRC["active_days"])),
                    getv(r, SRC["activity_level"]),
                    None)  # no dedicated comments field in your SELECT; keep NULL
        ok_counts["PhysicalActivity"] += 1
    except Exception as ex:
        log_fail("Assessment_PhysicalActivity", r, ex)

    try:
        # ------------------ PREFERRED ACTIVITIES ------------------
        cur.execute(SQL_PREF, card, anum, adate,
                    getv(r, SRC["pref"]),
                    getv(r, SRC["comments_pref"]),
                    parse_date(getv(r, SRC["next_review"])))
        ok_counts["PreferredActivities"] += 1
    except Exception as ex:
        log_fail("Assessment_PreferredActivities", r, ex)

    try:
        # ------------------ SOCIAL ISOLATION ------------------
        cur.execute(SQL_SOCIAL, card, anum, adate,
                    to_int(getv(r, SRC["lack"])),
                    to_int(getv(r, SRC["leftout"])),
                    to_int(getv(r, SRC["isolated"])),
                    getv(r, SRC["comments_social"]))
        ok_counts["SocialIsolation"] += 1
    except Exception as ex:
        log_fail("Assessment_SocialIsolation", r, ex)

    try:
        # ------------------ WEMWBS ------------------
        cur.execute(SQL_WEMWBS, card, anum, adate,
                    to_int(getv(r, SRC["wem_opt"])),
                    to_int(getv(r, SRC["wem_use"])),
                    to_int(getv(r, SRC["wem_rel"])),
                    to_int(getv(r, SRC["wem_int"])),
                    to_int(getv(r, SRC["wem_eng"])),
                    to_int(getv(r, SRC["wem_prob"])),
                    to_int(getv(r, SRC["wem_think"])),
                    to_int(getv(r, SRC["wem_good"])),
                    to_int(getv(r, SRC["wem_close"])),
                    to_int(getv(r, SRC["wem_conf"])),
                    to_int(getv(r, SRC["wem_mind"])),
                    to_int(getv(r, SRC["wem_loved"])),
                    to_int(getv(r, SRC["wem_new"])),
                    to_int(getv(r, SRC["wem_cheer"])),
                    getv(r, SRC["comments_wem"]))
        ok_counts["WEMWBS"] += 1
    except Exception as ex:
        log_fail("Assessment_WEMWBS", r, ex)

# commit once at the end
conn.commit()
cur.close()
conn.close()

print("✅ Done. Insert counts:")
for k, v in ok_counts.items():
    print(f"  - {k}: {v}")

print(f"❌ Failures: {len(failed)}")
if failed:
    ensure_dir_for_file(FAILED_FILE)
    pd.DataFrame(failed).to_excel(FAILED_FILE, index=False)
    print(f"📁 Failed rows exported: {FAILED_FILE}")
