import pyodbc
import json
import pandas as pd

# ===============================
# DATABASE CONFIG
# ===============================
CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=MIGHTYSUPERMAN;"
    "DATABASE=SahelihubCRM;"
    "Trusted_Connection=yes;"
)

SOURCE_TABLE = "dbo.Staging_HealthAssessments_RawJson"
OUTPUT_FILE = "Participants.xlsx"

# ===============================
# SAFE VALUE CLEANER
# ===============================
def clean(v):
    if v is None:
        return ""
    if isinstance(v, str):
        return v.strip()
    return v

# ===============================
# LOAD RAW JSON
# ===============================
conn = pyodbc.connect(CONN_STR)
df = pd.read_sql(f"SELECT RawJson FROM {SOURCE_TABLE}", conn)
conn.close()

participants = []

# ===============================
# EXTRACT PARTICIPANTS ONLY
# ===============================
for _, row in df.iterrows():
    try:
        j = json.loads(row["RawJson"])
    except:
        continue

    participants.append({
        "NoOfAssessmentCompleted": clean(j.get("No of assessment completed")),
        "RegistrationDate": clean(j.get("Registration Date")),
        "SaheliCardNumber": clean(j.get("Saheli Card Number ") or j.get("Saheli Card Number")),
        "FullName": clean(j.get(" Full Name:")),
        "DateOfBirth": clean(j.get(" Date of Birth:")),
        "Age": clean(j.get("AGE")),
        "Address": clean(j.get(" Address:")),
        "Postcode": clean(j.get(" Postcode:")),
        "Email": clean(j.get(" Email:")),
        "MobileNumber": clean(j.get(" Mobile/Home No:")),
        "EmergencyContactName": clean(j.get(" Emergency Contact Name:")),
        "EmergencyContactNumber": clean(j.get(" Emergency No:")),
        "EmergencyContactRelation": clean(j.get(" Emergency Relation To You:")),
        "Gender": clean(j.get(" Gender:")),
        "GenderSameAsBirth": clean(j.get(" Is your gender the same as assigned at birth?")),
        "HealthConditionsDisability": clean(j.get(" Health Conditions/Disability:")),
        "Ethnicity": clean(j.get(" Ethnicity:")),
        "PreferredLanguage": clean(j.get(" Preferred spoken language:")),
        "Religion": clean(j.get(" Religion:")),
        "RelationshipStatus": clean(j.get(" Relationship status:")),
        "CaringResponsibilities": clean(j.get(" Caring responsibilities:")),
        "LivingAlone": clean(j.get(" Living alone:")),
        "Sexuality": clean(j.get(" Sexuality:")),
        "Occupation": clean(j.get(" Occupation:")),
        "ReferralReason": clean(j.get("Referral reason")),
        "HeardAboutSaheli": clean(j.get(" How heard about Saheli Hub?")),
        "GPSurgeryName": clean(j.get("GP Surgery Name:\n")),
        "ConsentToStoreInformation": clean(j.get(" Consent to store information:")),
        "HealthDeclaration": clean(j.get(" Health declaration:")),
        "PermissionWhatsapp": clean(j.get(" Permission to be added to Saheli WhatsApp group?")),
        "PermissionMedia": clean(j.get(" Permission to be in photos and videos? (Media consent)")),
        "Notes": clean(j.get("Notes:")),
        "StaffMember": clean(j.get(" Staff Member:")),
        "Site": clean(j.get("Site:")),
    })

# ===============================
# EXPORT TO EXCEL
# ===============================
participants_df = pd.DataFrame(participants)

participants_df.to_excel(
    OUTPUT_FILE,
    index=False,
    engine="openpyxl"
)

print(f"✅ Participants extracted: {len(participants_df)}")
print(f"📁 Excel created: {OUTPUT_FILE}")
