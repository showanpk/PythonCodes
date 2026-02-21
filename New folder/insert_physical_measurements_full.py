import json
import pyodbc
import pandas as pd

CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=MIGHTYSUPERMAN;"
    "DATABASE=SahelihubCRM;"
    "Trusted_Connection=yes;"
)

SOURCE_TABLE = "dbo.Staging_HealthAssessments_RawJson"
OUTPUT_FILE = "SocialIsolationScores.xlsx"

SOCIAL_KEYS = {
    1: {
        "LackCompanionship": ["How often do you feel that you lack companionship?"],
        "FeelLeftOut": ["How often do you feel left out?"],
        "FeelIsolated": ["How often do you feel isolated from others?"],
        "ConfidenceToJoin": ["How confident are you to join activities?"],
        "Hobbies": ["How many hobbies and passions do you have?"],
        "CommunityInvolvement": ["How involved you feel in your community?"],
        "ServiceAwareness": ["How much you know about local support/services?"],
    },
    2: {
        "LackCompanionship": ["How often do you feel that you lack companionship?72"],
        "FeelLeftOut": ["How often do you feel left out?73"],
        "FeelIsolated": ["How often do you feel isolated from others?74"],
        "ConfidenceToJoin": ["How confident are you to join activities?75"],
        "Hobbies": ["How many hobbies and passions do you have?76"],
        "CommunityInvolvement": ["How involved you feel in your community?77"],
        "ServiceAwareness": ["How much you know about local support/services?78"],
    },
    3: {
        "LackCompanionship": ["How often do you feel that you lack companionship?155"],
        "FeelLeftOut": ["How often do you feel left out?156"],
        "FeelIsolated": ["How often do you feel isolated from others?157"],
        "ConfidenceToJoin": ["How confident are you to join activities?159"],
        "Hobbies": ["How many hobbies and passions do you have?160"],
        "CommunityInvolvement": ["How involved you feel in your community?161"],
        "ServiceAwareness": ["How much you know about local support/services?162"],
    },
    4: {
        "LackCompanionship": ["How often do you feel that you lack companionship?240"],
        "FeelLeftOut": ["How often do you feel left out?241"],
        "FeelIsolated": ["How often do you feel isolated from others?242"],
        "ConfidenceToJoin": ["How confident are you to join activities?245"],
        "Hobbies": ["How many hobbies and passions do you have?246"],
        "CommunityInvolvement": ["How involved you feel in your community?247"],
        "ServiceAwareness": ["How much you know about local support/services?248"],
    },
    5: {
        "LackCompanionship": ["How often do you feel that you lack companionship?327"],
        "FeelLeftOut": ["How often do you feel left out?328"],
        "FeelIsolated": ["How often do you feel isolated from others?329"],
        "ConfidenceToJoin": ["How confident are you to join activities?331"],
        "Hobbies": ["How many hobbies and passions do you have?332"],
        "CommunityInvolvement": ["How involved you feel in your community?333"],
        "ServiceAwareness": ["How much you know about local support/services?334"],
    },
    6: {
        "LackCompanionship": ["How often do you feel that you lack companionship?413"],
        "FeelLeftOut": ["How often do you feel left out?414"],
        "FeelIsolated": ["How often do you feel isolated from others?415"],
        "ConfidenceToJoin": ["How confident are you to join activities?417"],
        "Hobbies": ["How many hobbies and passions do you have?418"],
        "CommunityInvolvement": ["How involved you feel in your community?419"],
        "ServiceAwareness": ["How much you know about local support/services?420"],
    },
}

ASSESSMENT_DATE_KEYS = {
    1: ["1st Assessment"],
    2: ["2nd Assessment"],
    3: ["3rd Assessment"],
    4: ["4th Assessment"],
    5: ["5th Assessment"],
    6: ["6th Assessment"],
}

def clean(v):
    if v is None:
        return ""
    if isinstance(v, str):
        return v.strip()
    return v

def pick_value(j, keys):
    for k in keys:
        if k in j:
            v = clean(j.get(k))
            if v != "":
                return v
    return ""

def get_card(j):
    return clean(j.get("Saheli Card Number ")) or clean(j.get("Saheli Card Number")) or ""

conn = pyodbc.connect(CONN_STR)
df_raw = pd.read_sql(f"SELECT RawJson FROM {SOURCE_TABLE}", conn)
conn.close()

rows = []
for _, r in df_raw.iterrows():
    try:
        j = json.loads(r["RawJson"])
        if not isinstance(j, dict):
            continue
    except:
        continue

    card = get_card(j)
    if card == "":
        continue

    for n in range(1, 7):
        rec = {
            "SaheliCardNumber": card,
            "AssessmentNumber": n,
            "AssessmentDate": pick_value(j, ASSESSMENT_DATE_KEYS[n]),
        }

        for col, keys in SOCIAL_KEYS[n].items():
            rec[col] = pick_value(j, keys)

        # keep only if any social fields exist
        if any(rec.get(k, "") != "" for k in SOCIAL_KEYS[n].keys()):
            rows.append(rec)

df_out = pd.DataFrame(rows, columns=[
    "SaheliCardNumber","AssessmentNumber","AssessmentDate",
    "LackCompanionship","FeelLeftOut","FeelIsolated",
    "ConfidenceToJoin","Hobbies","CommunityInvolvement","ServiceAwareness"
])
df_out.to_excel(OUTPUT_FILE, index=False)
print("✅ Created:", OUTPUT_FILE, "Rows:", len(df_out))

