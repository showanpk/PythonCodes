import pyodbc
import json
import pandas as pd

CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=MIGHTYSUPERMAN;"
    "DATABASE=SahelihubCRM;"
    "Trusted_Connection=yes;"
)

SOURCE_TABLE = "dbo.Staging_HealthAssessments_RawJson"

def clean(v):
    if v is None:
        return ""
    if isinstance(v, str):
        return v.strip()
    return v

conn = pyodbc.connect(CONN_STR)
df = pd.read_sql(f"SELECT RawJson FROM {SOURCE_TABLE}", conn)
conn.close()



assessments = []

for _, row in df.iterrows():
    try:
        j = json.loads(row["RawJson"])
    except:
        continue

    saheli = clean(j.get("Saheli Card Number ") or j.get("Saheli Card Number"))
    total = int(j.get("No of assessment completed", 0) or 0)

    for i in range(1, total + 1):
        suffix = "" if i == 1 else str(i)
        date_key = f"{i}st Assessment" if i == 1 else f"{i}nd Assessment" if i == 2 else f"{i}rd Assessment" if i == 3 else f"{i}th Assessment"

        assessment_date = clean(j.get(date_key))
        if not assessment_date:
            continue

        assessments.append({
            "SaheliCardNumber": saheli,
            "AssessmentNumber": i,
            "AssessmentDate": assessment_date,
            "StaffMember": clean(j.get(" Staff Member:")),
            "Site": clean(j.get("Site:")),
            "RiskStratificationScore": clean(j.get(f" Risk Stratification Score{suffix}")),
            "NextReviewDate": clean(j.get(f" Date of next review appointment:{suffix}")),
        })

pd.DataFrame(assessments).to_excel("Assessments.xlsx", index=False)

physical = []

for _, row in df.iterrows():
    try:
        j = json.loads(row["RawJson"])
    except:
        continue

    saheli = clean(j.get("Saheli Card Number ") or j.get("Saheli Card Number"))
    total = int(j.get("No of assessment completed", 0) or 0)

    for i in range(1, total + 1):
        suffix = "" if i == 1 else str(i)

        physical.append({
            "SaheliCardNumber": saheli,
            "AssessmentNumber": i,
            "WeightKG": clean(j.get(f" Weight (KG):{suffix}")),
            "HeightCM": clean(j.get(f" Height (CM):{suffix}")),
            "BMI": clean(j.get(f" BMI:{suffix}")),
            "WaistCM": clean(j.get(f" Waist (CM):{suffix}")),
            "HipCM": clean(j.get(f" Hip (CM):{suffix}")),
            "BloodPressure": clean(j.get(f" Blood Pressure (Systolic/Diastolic):{suffix}")),
            "HeartRateBPM": clean(j.get(f" Heart Rate (BPM):{suffix}")),
        })

pd.DataFrame(physical).to_excel("PhysicalMeasurements.xlsx", index=False)


lifestyle = []

for _, row in df.iterrows():
    try:
        j = json.loads(row["RawJson"])
    except:
        continue

    saheli = clean(j.get("Saheli Card Number ") or j.get("Saheli Card Number"))
    total = int(j.get("No of assessment completed", 0) or 0)

    for i in range(1, total + 1):
        suffix = "" if i == 1 else str(i)

        lifestyle.append({
            "SaheliCardNumber": saheli,
            "AssessmentNumber": i,
            "Nourishment": clean(j.get(f" Nourishment:{suffix}")),
            "Movement": clean(j.get(f" Movement:{suffix}")),
            "Sleep": clean(j.get(f" Sleep:{suffix}")),
            "Resilience": clean(j.get(f" Resilience:{suffix}")),
            "Connectedness": clean(j.get(f" Connectedness:{suffix}")),
            "ScreenTime": clean(j.get(f" Screen time:{suffix}")),
            "SubstanceUse": clean(j.get(f" Substance use:{suffix}")),
            "Purpose": clean(j.get(f" Purpose:{suffix}")),
        })

pd.DataFrame(lifestyle).to_excel("LifestyleScores.xlsx", index=False)
print(f"✅ Assessments extracted: {len(assessments)}")
print(f"✅ Physical measurements extracted: {len(physical)}")
print(f"✅ Lifestyle scores extracted: {len(lifestyle)}")

wemwbs = []

questions = [
    "I’ve been feeling optimistic about the future",
    "I’ve been feeling useful",
    "I’ve been feeling relaxed",
    "I’ve been feeling interested in other people",
    "I’ve had energy to spare",
    "I’ve been dealing with problems well",
    "I’ve been thinking clearly",
    "I’ve been feeling good about myself",
    "I’ve been feeling close to other people",
    "I’ve been feeling confident",
    "I’ve been able to make up my own mind about things",
    "I’ve been feeling loved",
    "I’ve been interested in new things",
    "I’ve been feeling cheerful",
]

for _, row in df.iterrows():
    try:
        j = json.loads(row["RawJson"])
    except:
        continue

    saheli = clean(j.get("Saheli Card Number ") or j.get("Saheli Card Number"))
    total = int(j.get("No of assessment completed", 0) or 0)

    for i in range(1, total + 1):
        suffix = "" if i == 1 else str(i)

        entry = {
            "SaheliCardNumber": saheli,
            "AssessmentNumber": i,
        }

        for q in questions:
            entry[q] = clean(j.get(f"{q}{suffix}"))

        wemwbs.append(entry)

pd.DataFrame(wemwbs).to_excel("WEMWBSScores.xlsx", index=False)
print(f"✅ WEMWBS scores extracted: {len(wemwbs)}")

social = []

for _, row in df.iterrows():
    try:
        j = json.loads(row["RawJson"])
    except:
        continue

    saheli = clean(j.get("Saheli Card Number ") or j.get("Saheli Card Number"))
    total = int(j.get("No of assessment completed", 0) or 0)

    for i in range(1, total + 1):
        suffix = "" if i == 1 else str(i)

        social.append({
            "SaheliCardNumber": saheli,
            "AssessmentNumber": i,
            "LackCompanionship": clean(j.get(f" How often do you feel that you lack companionship?{suffix}")),
            "FeelLeftOut": clean(j.get(f"How often do you feel left out?{suffix}")),
            "FeelIsolated": clean(j.get(f" How often do you feel isolated from others?{suffix}")),
        })

pd.DataFrame(social).to_excel("SocialIsolationScores.xlsx", index=False)
print(f"✅ Social isolation scores extracted: {len(social)}")

contacts = []

for _, row in df.iterrows():
    try:
        j = json.loads(row["RawJson"])
    except:
        continue

    contacts.append({
        "SaheliCardNumber": clean(j.get("Saheli Card Number ") or j.get("Saheli Card Number")),
        "ContactName": clean(j.get(" Emergency Contact Name:")),
        "ContactNumber": clean(j.get(" Emergency No:")),
        "Relationship": clean(j.get(" Emergency Relation To You:")),
    })

pd.DataFrame(contacts).to_excel("ParticipantEmergencyContacts.xlsx", index=False)
print(f"✅ Participant emergency contacts extracted: {len(contacts)}")