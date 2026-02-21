import pyodbc
import pandas as pd
import json

# ==============================
# CONFIG
# ==============================
CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=MIGHTYSUPERMAN;"
    "DATABASE=SahelihubCRM;"
    "Trusted_Connection=yes;"
)

SOURCE_TABLE = "dbo.Staging_HealthAssessments_RawJson"
OUTPUT_FILE = "Assessments.xlsx"

# ==============================
# HELPERS
# ==============================
def clean(val):
    if val is None:
        return None
    val = str(val).strip()
    return val if val != "" else None

# Assessment key templates
ASSESSMENT_KEYS = {
    1: {
        "date": "1st Assessment",
        "weight": " Weight (KG):",
        "height": " Height (CM):",
        "bmi": " BMI:",
        "bp": " Blood Pressure (Systolic/Diastolic):",
        "hr": " Heart Rate (BPM):"
    },
    2: {
        "date": "2nd Assessment",
        "weight": " Weight (KG):2",
        "height": " Height (CM):3",
        "bmi": " BMI:4",
        "bp": " Blood Pressure (Systolic/Diastolic):22",
        "hr": " Heart Rate (BPM):25"
    },
    3: {
        "date": "3rd Assessment",
        "weight": " Weight (KG):86",
        "height": " Height (CM):87",
        "bmi": " BMI:88",
        "bp": " Blood Pressure (Systolic/Diastolic):106",
        "hr": " Heart Rate (BPM):109"
    },
    4: {
        "date": "4th Assessment",
        "weight": " Weight (KG):170",
        "height": " Height (CM):171",
        "bmi": " BMI:172",
        "bp": " Blood Pressure (Systolic/Diastolic):190",
        "hr": " Heart Rate (BPM):193"
    },
    5: {
        "date": "5th Assessment",
        "weight": " Weight (KG):256",
        "height": " Height (CM):257",
        "bmi": " BMI:258",
        "bp": " Blood Pressure (Systolic/Diastolic):276",
        "hr": " Heart Rate (BPM):279"
    },
    6: {
        "date": "6th Assessment",
        "weight": " Weight (KG):342",
        "height": " Height (CM):343",
        "bmi": " BMI:344",
        "bp": " Blood Pressure (Systolic/Diastolic):362",
        "hr": " Heart Rate (BPM):365"
    }
}

# ==============================
# LOAD RAW JSON
# ==============================
conn = pyodbc.connect(CONN_STR)
df = pd.read_sql(f"SELECT RawJson FROM {SOURCE_TABLE}", conn)

assessments = []

# ==============================
# PROCESS EACH PARTICIPANT
# ==============================
for _, row in df.iterrows():
    j = json.loads(row["RawJson"])

    card = clean(j.get("Saheli Card Number "))
    name = clean(j.get(" Full Name:"))

    for num, keys in ASSESSMENT_KEYS.items():
        assessment_date = clean(j.get(keys["date"]))

        # 🚨 Skip if this assessment does not exist
        if not assessment_date:
            continue

        assessments.append({
            "SaheliCardNumber": card,
            "FullName": name,
            "AssessmentNumber": num,
            "AssessmentDate": assessment_date,
            "WeightKG": clean(j.get(keys["weight"])),
            "HeightCM": clean(j.get(keys["height"])),
            "BMI": clean(j.get(keys["bmi"])),
            "BloodPressure": clean(j.get(keys["bp"])),
            "HeartRateBPM": clean(j.get(keys["hr"]))
        })

# ==============================
# EXPORT TO EXCEL
# ==============================
assessments_df = pd.DataFrame(assessments)
assessments_df.to_excel(OUTPUT_FILE, index=False)

print(f"✅ Assessments extracted: {len(assessments_df)}")
print(f"📁 Excel created: {OUTPUT_FILE}")
