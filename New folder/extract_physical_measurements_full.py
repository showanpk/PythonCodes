import pandas as pd
from db_utils import get_conn, clean, lookup_participant_id, lookup_assessment_id

FILE_IN = "PhysicalMeasurements_FULL.xlsx"
FAILED_OUT = "PhysicalMeasurements_INSERT_FAILED.xlsx"

df = pd.read_excel(FILE_IN).applymap(clean)

INSERT_SQL = """
INSERT INTO dbo.PhysicalMeasurements (
    AssessmentID,
    WeightKG, HeightCM, BMI,
    WaistCM, HipCM, WaistToHipRatio,
    BodyFatPercentage, VisceralFatLevel,
    SkeletalMusclePercentage, RestingMetabolism,
    BloodPressure, HeartRateBPM, HeartAge
)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
"""

conn = get_conn()
cursor = conn.cursor()
failed = []

for _, r in df.iterrows():
    try:
        pid = lookup_participant_id(cursor, r["SaheliCardNumber"])
        if not pid:
            raise Exception("Participant not found for SaheliCardNumber")

        aid = lookup_assessment_id(cursor, pid, int(r["AssessmentNumber"]))
        if not aid:
            raise Exception("Assessment not found (insert Assessments table first)")

        cursor.execute(
            INSERT_SQL,
            aid,
            r["WeightKG"], r["HeightCM"], r["BMI"],
            r["WaistCM"], r["HipCM"], r["WaistToHipRatio"],
            r["BodyFatPercentage"], r["VisceralFatLevel"],
            r["SkeletalMusclePercentage"], r["RestingMetabolism"],
            r["BloodPressure"], r["HeartRateBPM"], r["HeartAge"]
        )
        conn.commit()

    except Exception as e:
        failed.append({**r.to_dict(), "ERROR": str(e)})
        conn.rollback()

pd.DataFrame(failed).to_excel(FAILED_OUT, index=False)
cursor.close()
conn.close()

print(f"✅ Insert done. Failed rows: {len(failed)} -> {FAILED_OUT}")
