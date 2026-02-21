import pandas as pd

FAILED_FILE = r"C:\Users\shonk\source\PythonCodes\FAILED_Assessment_Imports.xlsx"

df = pd.read_excel(FAILED_FILE)

print("Failures by TargetTable:")
print(df["TargetTable"].value_counts())

print("\nTop 15 error messages:")
print(df["Error"].value_counts().head(15))

# Show BodyComposition sample errors
print("\n--- BodyComposition sample ---")
print(df[df["TargetTable"].str.contains("Body", na=False)].head(10)[["TargetTable","SaheliCardNumber","AssessmentNumber","AssessmentDate","Error"]])

# Show WEMWBS sample errors
print("\n--- WEMWBS sample ---")
print(df[df["TargetTable"].str.contains("WEM", na=False)].head(10)[["TargetTable","SaheliCardNumber","AssessmentNumber","AssessmentDate","Error"]])
import os, re, math
import pandas as pd
import pyodbc
from datetime import datetime, date

CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=MIGHTYSUPERMAN;"
    "DATABASE=SahelihubCRM;"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
)

SOURCE_TABLE = "dbo.HealthAssessment_Full"   # your flattened table
FAILED_FILE = r"C:\Users\shonk\source\PythonCodes\FAILED_Assessment_Imports_FIXED.xlsx"

def clean(v):
    if v is None:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    if isinstance(v, str):
        s = v.strip()
        return s if s else None
    return v

def to_int(v):
    v = clean(v)
    if v is None:
        return None
    if isinstance(v, int):
        return int(v)
    s = str(v).strip()
    if s.lower() in ("nan", "n/a", "na", "not sure", "none", ""):
        return None
    m = re.search(r"-?\d+", s)
    return int(m.group(0)) if m else None

def to_float(v):
    v = clean(v)
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if s.lower() in ("nan", "n/a", "na", "not sure", "none", ""):
        return None
    s = s.replace(",", "")
    m = re.search(r"-?\d+(\.\d+)?", s)
    return float(m.group(0)) if m else None

def to_bool(v):
    v = clean(v)
    if v is None:
        return None
    s = str(v).strip().lower()
    if s in ("yes", "y", "true", "1"):
        return True
    if s in ("no", "n", "false", "0"):
        return False
    return None

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

def norm_col(s: str) -> str:
    if s is None:
        return ""
    s = str(s)
    s = s.replace("’", "'").replace("‘", "'").replace("“", '"').replace("”", '"')
    s = re.sub(r"[\r\n\t]+", " ", s).strip()
    s = re.sub(r"\s+", " ", s)
    return s

def getv(row, col):
    # row is a pandas Series, columns already normalised
    return row.get(col)

def ensure_dir(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)

# -----------------------------
# SQL INSERTS
# -----------------------------
SQL_AIMS = """
INSERT INTO dbo.Assessment_AimsGoals (SaheliCardNumber, AssessmentNumber, AssessmentDate, AimsGoals, AimsDescription)
VALUES (?,?,?,?,?)
"""

SQL_BARRIERS = """
INSERT INTO dbo.Assessment_Barriers (SaheliCardNumber, AssessmentNumber, AssessmentDate, Barriers, BarrierComments)
VALUES (?,?,?,?,?)
"""

SQL_BODY = """
INSERT INTO dbo.Assessment_BodyComposition (
  SaheliCardNumber, AssessmentNumber, AssessmentDate,
  WeightKg, HeightCm, BMICategory, BMIValue,
  WaistCm, HipCm, WaistHipRatio,
  BodyFatCategory, BodyFatScore,
  VisceralFatCategory, VisceralFatScore,
  SkeletalMuscleCategory, SkeletalMuscleScore,
  RestingMetabolism
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
"""

SQL_COMMUNITY = """
INSERT INTO dbo.Assessment_CommunityConfidence (
  SaheliCardNumber, AssessmentNumber, AssessmentDate,
  ConfidenceToJoin, NumberOfHobbies, CommunityInvolvement, ServiceAwareness
) VALUES (?,?,?,?,?,?,?)
"""

SQL_HEALTH = """
INSERT INTO dbo.Assessment_HealthScreening (
  SaheliCardNumber, AssessmentNumber, AssessmentDate,
  HasHealthCondition, LastBPMeasurementDate, BPRecordedWithGP,
  KnowledgeHealthyBP, KnowledgeBPRisk, KnowledgeBPReduction,
  SystolicBP, DiastolicBP, BPLevel,
  HeartConditionTypes, HeartRateBPM, AtrialFibrillationResult, HeartAge,
  DoctorAdvisedNoExercise, ChestPain, ShortnessOfBreath,
  DiabetesType, DiabetesRisk, GlucoseLevel, HbA1c,
  SugaryDrinkIntake, HighCholesterol, OtherHealthIssues, BoneJointConditions,
  TakesPrescribedMedication, ReferredToDoctor, RiskStratification,
  HealthComments, SelfManagementScore
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
"""

SQL_LIFESTYLE = """
INSERT INTO dbo.Assessment_Lifestyle (
  SaheliCardNumber, AssessmentNumber, AssessmentDate,
  Nourishment, Movement, Connectedness, SleepQuality, HappySelf,
  Resilience, GreenBlueSpace, ScreenTime, SubstanceUse, Purpose, LifestyleComments
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
"""

SQL_ACTIVITY = """
INSERT INTO dbo.Assessment_PhysicalActivity (
  SaheliCardNumber, AssessmentNumber, AssessmentDate,
  ActiveDaysPerWeek, ActivityLevel, ActivityComments
) VALUES (?,?,?,?,?,?)
"""

SQL_PREF = """
INSERT INTO dbo.Assessment_PreferredActivities (
  SaheliCardNumber, AssessmentNumber, AssessmentDate,
  PreferredActivities, ActivityComments, NextReviewDate
) VALUES (?,?,?,?,?,?)
"""

SQL_SOCIAL = """
INSERT INTO dbo.Assessment_SocialIsolation (
  SaheliCardNumber, AssessmentNumber, AssessmentDate,
  LackCompanionship, FeelLeftOut, FeelIsolated, SocialIsolationComments
) VALUES (?,?,?,?,?,?,?)
"""

# ✅ FIXED: 18 placeholders
SQL_WEMWBS = """
INSERT INTO dbo.Assessment_WEMWBS (
  SaheliCardNumber, AssessmentNumber, AssessmentDate,
  FeelingOptimistic, FeelingUseful, FeelingRelaxed, FeelingInterestedInPeople,
  EnergyToSpare, DealingWithProblems, ThinkingClearly, FeelingGoodAboutSelf,
  FeelingCloseToOthers, FeelingConfident, MakingOwnMindUp, FeelingLoved,
  InterestedInNewThings, FeelingCheerful,
  WEMWBSComments
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
"""

# -----------------------------
# LOAD SOURCE
# -----------------------------
conn = pyodbc.connect(CONN_STR)
df = pd.read_sql(f"SELECT * FROM {SOURCE_TABLE}", conn)

df.columns = [norm_col(c) for c in df.columns]
df = df.apply(lambda col: col.map(clean))

# identify columns (your names from screenshot)
COL_CARD = "SaheliCardNo:"
COL_DATE = "Real date"
COL_ANUM = "AssessmentNumber"

# fallbacks
if COL_CARD not in df.columns:
    COL_CARD = "Saheli Card No:"
if COL_DATE not in df.columns:
    COL_DATE = "Start time"

failed = []
counts = {
    "AimsGoals":0, "Barriers":0, "BodyComposition":0, "CommunityConfidence":0,
    "HealthScreening":0, "Lifestyle":0, "PhysicalActivity":0,
    "PreferredActivities":0, "SocialIsolation":0, "WEMWBS":0
}

cur = conn.cursor()

def log_fail(table, card, anum, adate, ex):
    failed.append({
        "TargetTable": table,
        "SaheliCardNumber": card,
        "AssessmentNumber": anum,
        "AssessmentDate": adate,
        "Error": str(ex)
    })

for idx, r in df.iterrows():
    card = clean(getv(r, COL_CARD))
    anum = to_int(getv(r, COL_ANUM))
    adate = parse_date(getv(r, COL_DATE))

    if not card or not anum or not adate:
        continue

    # AimsGoals
    try:
        cur.execute(SQL_AIMS, (card, anum, adate, clean(getv(r, "What are your aims & goals?")), clean(getv(r, "Comments:5"))))
        counts["AimsGoals"] += 1
    except Exception as ex:
        log_fail("Assessment_AimsGoals", card, anum, adate, ex)

    # Barriers
    try:
        cur.execute(SQL_BARRIERS, (card, anum, adate, clean(getv(r, "What reasons stop you from joining activities?")), clean(getv(r, "Comments:6"))))
        counts["Barriers"] += 1
    except Exception as ex:
        log_fail("Assessment_Barriers", card, anum, adate, ex)

    # BodyComposition
    try:
        cur.execute(SQL_BODY, (
            card, anum, adate,
            to_float(getv(r, "Weight (KG):")),
            to_float(getv(r, "Height (CM):")),
            clean(getv(r, "BMI Results:")),
            to_float(getv(r, "BMI:")),
            to_float(getv(r, "Waist (CM):")),
            to_float(getv(r, "Hip (CM):")),
            to_float(getv(r, "Waist to Hip Ratio (CM):")),
            clean(getv(r, "Body Fat Percentage Result:")),
            to_float(getv(r, "Body Fat Percentage Score:")),
            clean(getv(r, "Visceral Fat Level Result:")),
            to_float(getv(r, "Visceral Fat Level Score:")),
            clean(getv(r, "Skeletal Muscle Percentage:")),
            to_float(getv(r, "Skeletal Muscle Score:")),
            to_float(getv(r, "Resting Metabolism:")),
        ))
        counts["BodyComposition"] += 1
    except Exception as ex:
        log_fail("Assessment_BodyComposition", card, anum, adate, ex)

    # CommunityConfidence
    try:
        cur.execute(SQL_COMMUNITY, (
            card, anum, adate,
            to_int(getv(r, "How confident are you to join activities?")),
            to_int(getv(r, "How many hobbies and passions do you have?")),
            to_int(getv(r, "How involved you feel in your community?")),
            to_int(getv(r, "How much you know about local support/services?")),
        ))
        counts["CommunityConfidence"] += 1
    except Exception as ex:
        log_fail("Assessment_CommunityConfidence", card, anum, adate, ex)

    # HealthScreening (most values are text/yes/no so safe conversion is critical)
    try:
        # BP split
        bp = clean(getv(r, "Blood Pressure (Systolic/Diastolic):"))
        syst, dias = (None, None)
        if bp and "/" in bp:
            parts = bp.split("/")
            syst = to_int(parts[0])
            dias = to_int(parts[1])

        cur.execute(SQL_HEALTH, (
            card, anum, adate,
            to_bool(getv(r, "Do You Have Any Health Condition?")),
            parse_date(getv(r, "When did you last measure your blood pressure?")),
            clean(getv(r, "Have you recorded your blood pressure measurement and registered it with a GP or Pharmacist? (Yes/No/Not sure)")),
            clean(getv(r, "What is a healthy blood pressure for an adult?")),
            clean(getv(r, "Why is a high blood pressure dangerous?")),
            clean(getv(r, "How can you help reduce your blood pressure?")),
            syst,
            dias,
            clean(getv(r, "Blood Pressure Level:")),
            clean(getv(r, "Do You Have a Heart Condition?")),
            to_int(getv(r, "Heart Rate (BPM):")),
            to_int(getv(r, "Atrial Fibrillation Result:")),
            to_int(getv(r, "Heart Age:")),
            to_bool(getv(r, "Did Your Doctor Advise You Not to Exercise?")),
            to_bool(getv(r, "Do You Feel Pain in Chest at Rest/During Activity?")),
            clean(getv(r, "Do You Have Shortness of Breath?")),
            clean(getv(r, "Do You Have Diabetes?")),
            clean(getv(r, "Diabetes Risk:")),
            to_float(getv(r, "Glucose Level ( mg/dL):")),
            to_float(getv(r, "HbA1c:")),
            to_bool(getv(r, "Do You Take Sugary Drinks, Including Chai?")),
            to_bool(getv(r, "Do You Have High Cholesterol? (Total/HDL)")),
            clean(getv(r, "Do You Experience The Following Health Issues?")),
            clean(getv(r, "Do You Have a Bone / joint Condition?")),
            to_bool(getv(r, "Do You Take Any Prescribed Medication?")),
            to_bool(getv(r, "Referred to doctor for any concerning results?")),
            clean(getv(r, "Risk Stratification Score")),
            clean(getv(r, "Comments:")),
            to_int(getv(r, "How well do you manage your health/condition(s)? (Rating out of 10)")),
        ))
        counts["HealthScreening"] += 1
    except Exception as ex:
        log_fail("Assessment_HealthScreening", card, anum, adate, ex)

    # Lifestyle
    try:
        cur.execute(SQL_LIFESTYLE, (
            card, anum, adate,
            to_int(getv(r, "Nourishment: Rate the quality of the food you put into your body on a daily basis")),
            to_int(getv(r, "Movement: Rate how often and for how long you move your body on a daily basis")),
            to_int(getv(r, "Connectedness: Rate how well you stay connected with family, friends and your higher power")),
            to_int(getv(r, "Sleep: Rate the quality of your sleep")),
            to_int(getv(r, "Happy self: Rate how often and for how long you perform positive practices (gratitude, virtue awareness, meditation, pra")),
            to_int(getv(r, "Resilience: Rate how well you are able to manage stress in your life")),
            to_int(getv(r, "Green and Blue: Rate how often and how long you spend in nature or outdoors")),
            to_int(getv(r, "Screen time: Rate how happy you are with your current amount of screen time")),
            to_int(getv(r, "Substance use: Rate how comfortable you are with any current substance use (smoking, alcohol, drugs)")),
            to_int(getv(r, "Purpose: Rate how well you feel you are fulfilling your passion, purpose or vocation in life")),
            clean(getv(r, "Comments:3")),
        ))
        counts["Lifestyle"] += 1
    except Exception as ex:
        log_fail("Assessment_Lifestyle", card, anum, adate, ex)

    # PhysicalActivity
    try:
        cur.execute(SQL_ACTIVITY, (
            card, anum, adate,
            to_int(getv(r, "In the past week, on how many days have you done a total of 30 mins or more of physical activity, which was enough to ra")),
            clean(getv(r, "Physical Activity Level:")),
            clean(getv(r, "Comments:")),
        ))
        counts["PhysicalActivity"] += 1
    except Exception as ex:
        log_fail("Assessment_PhysicalActivity", card, anum, adate, ex)

    # PreferredActivities
    try:
        cur.execute(SQL_PREF, (
            card, anum, adate,
            clean(getv(r, "What are your preferred activities?")),
            clean(getv(r, "Comments:7")),
            parse_date(getv(r, "Date of next review appointment:")),
        ))
        counts["PreferredActivities"] += 1
    except Exception as ex:
        log_fail("Assessment_PreferredActivities", card, anum, adate, ex)

    # SocialIsolation
    try:
        cur.execute(SQL_SOCIAL, (
            card, anum, adate,
            to_int(getv(r, "How often do you feel that you lack companionship?")),
            to_int(getv(r, "How often do you feel left out?")),
            to_int(getv(r, "How often do you feel isolated from others?")),
            clean(getv(r, "Comments:4")),
        ))
        counts["SocialIsolation"] += 1
    except Exception as ex:
        log_fail("Assessment_SocialIsolation", card, anum, adate, ex)

    # ✅ WEMWBS (fixed)
    try:
        cur.execute(SQL_WEMWBS, (
            card, anum, adate,
            to_int(getv(r, "I've been feeling optimistic about the future")),
            to_int(getv(r, "I've been feeling useful")),
            to_int(getv(r, "I've been feeling relaxed")),
            to_int(getv(r, "I've been feeling interested in other people")),
            to_int(getv(r, "I've had energy to spare")),
            to_int(getv(r, "I've been dealing with problems well")),
            to_int(getv(r, "I've been thinking clearly")),
            to_int(getv(r, "I've been feeling good about myself")),
            to_int(getv(r, "I've been feeling close to other people")),
            to_int(getv(r, "I've been feeling confident")),
            to_int(getv(r, "I've been able to make up my own mind about things")),
            to_int(getv(r, "I've been feeling loved")),
            to_int(getv(r, "I've been interested in new things")),
            to_int(getv(r, "I've been feeling cheerful")),
            clean(getv(r, "Comments:2")),
        ))
        counts["WEMWBS"] += 1
    except Exception as ex:
        log_fail("Assessment_WEMWBS", card, anum, adate, ex)

conn.commit()

print("✅ Done. Insert counts:")
for k,v in counts.items():
    print(f"  - {k}: {v}")

print(f"❌ Failures: {len(failed)}")
if failed:
    ensure_dir(FAILED_FILE)
    pd.DataFrame(failed).to_excel(FAILED_FILE, index=False)
    print(f"📁 Failed rows exported: {FAILED_FILE}")

cur.close()
conn.close()
