# ============================================================
# Q FULL FILE: saheli_excel_to_sql_insert_compare.py
# ------------------------------------------------------------
# Reads:
#   - REG_OUTPUT_FILE (Registrations_Cleaned.xlsx)
#   - HEALTH_OUTPUT_FILE (Healthassessments_Prepared.xlsx)
#
# Compares against SQL Server tables and inserts ONLY missing
# records (based on keys), into:
#
# Registration tables:
#   - Participants
#   - ParticipantEmergencyContacts
#
# Assessment tables:
#   - Assessment_Master
#   - Assessments
#   - Assessment_AimsGoals
#   - Assessment_Barriers
#   - Assessment_BodyComposition
#   - Assessment_CommunityConfidence
#   - Assessment_HealthScreening
#   - Assessment_Lifestyle
#   - Assessment_PhysicalActivity
#   - Assessment_PreferredActivities
#   - Assessment_SocialIsolation
#   - Assessment_WEMWBS
#
# Install:
#   pip install pandas openpyxl pyodbc
# ============================================================

from pathlib import Path
import re
from datetime import datetime
import pandas as pd
import pyodbc

# =========================
# CONFIG
# =========================
REG_OUTPUT_FILE = r"C:\Users\shonk\source\PythonCodes\New folder\Registrations_Cleaned.xlsx"
HEALTH_OUTPUT_FILE = r"C:\Users\shonk\source\PythonCodes\New folder\Healthassessments_Prepared.xlsx"

SQL_CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=mightysuperman;"
    "DATABASE=SahelihubCRM;"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
)

DEFAULT_CREATED_BY_USER_ID = None  # e.g. 1 for system user if needed

# Toggle if you want row-by-row debug on bulk insert failure
DEBUG_ROW_FALLBACK = True


# =========================
# HELPERS
# =========================
def normalize_header(h) -> str:
    if h is None:
        return ""
    s = str(h)
    s = s.replace("\r", "").replace("\n", "")
    s = s.strip().lower()
    s = s.replace(" ", "").replace(":", "")
    s = s.replace("/", "")
    s = s.replace("?", "")
    s = s.replace("(", "").replace(")", "")
    s = s.replace("-", "")
    s = s.replace(",", "")
    s = s.replace(".", "")
    s = s.replace("&", "and")
    s = s.replace("’", "").replace("'", "")
    return s


def build_normalized_col_map(df: pd.DataFrame):
    m = {}
    for c in df.columns:
        k = normalize_header(c)
        m.setdefault(k, []).append(c)
    return m


def pick_col(norm_map, *candidates, occurrence=1):
    for cand in candidates:
        k = normalize_header(cand)
        if k in norm_map and len(norm_map[k]) >= occurrence:
            return norm_map[k][occurrence - 1]
    return None


def keep_digits_only(v):
    if pd.isna(v):
        return pd.NA

    if isinstance(v, (int, float)):
        try:
            fv = float(v)
            if fv.is_integer():
                return str(int(fv))
            s_num = format(fv, "f")
            digits = re.sub(r"\D+", "", s_num)
            return digits if digits else pd.NA
        except Exception:
            pass

    s = str(v).strip()
    if re.fullmatch(r"\d+\.0+", s):
        return s.split(".")[0]

    digits = re.sub(r"\D+", "", s)
    return digits if digits else pd.NA


def to_saheli_key(v):
    d = keep_digits_only(v)
    if pd.isna(d):
        return None
    try:
        return str(int(float(d)))
    except Exception:
        return str(d)


def _parse_datetime_safely(v):
    """Less warning-prone parsing for mixed UK + ISO date strings."""
    if pd.isna(v) or v is None:
        return None

    s = str(v).strip()
    if s == "":
        return None

    iso_like = bool(re.match(r"^\d{4}-\d{1,2}-\d{1,2}", s))
    try:
        dt = pd.to_datetime(s, errors="coerce", dayfirst=not iso_like)
        if pd.isna(dt):
            return None
        return dt.to_pydatetime()
    except Exception:
        return None


def parse_date(v):
    dt = _parse_datetime_safely(v)
    return dt.date() if dt else None


def parse_datetime(v):
    return _parse_datetime_safely(v)


def to_int(v):
    if pd.isna(v) or v is None or str(v).strip() == "":
        return None
    try:
        return int(float(v))
    except Exception:
        return None


def to_float(v):
    if pd.isna(v) or v is None or str(v).strip() == "":
        return None
    s = str(v).strip()
    m = re.search(r"-?\d+(\.\d+)?", s)
    if m:
        try:
            return float(m.group(0))
        except Exception:
            return None
    try:
        return float(v)
    except Exception:
        return None


def to_str(v):
    if pd.isna(v) or v is None:
        return None
    s = str(v).strip()
    return s if s != "" else None


def to_bit(v):
    """
    Convert yes/no/true/false/1/0 to SQL bit (0/1).
    Returns None if unknown/blank.
    """
    if pd.isna(v) or v is None:
        return None

    if isinstance(v, (int, float)):
        try:
            return 1 if int(float(v)) != 0 else 0
        except Exception:
            return None

    s = str(v).strip().lower()
    if s == "":
        return None

    true_vals = {"yes", "y", "true", "1", "checked"}
    false_vals = {"no", "n", "false", "0", "unchecked"}

    if s in true_vals:
        return 1
    if s in false_vals:
        return 0
    return None


def to_first_int_from_text(v):
    """
    Extract first integer from text like:
      '62 BPM Normal' -> 62
      '81' -> 81
    """
    if pd.isna(v) or v is None:
        return None
    s = str(v).strip()
    if s == "":
        return None
    m = re.search(r"\d+", s)
    if m:
        try:
            return int(m.group(0))
        except Exception:
            return None
    return None


def split_bp(bp_value):
    """
    Parses blood pressure like:
      "120/80", "120 / 80", "120-80"
    Returns (systolic, diastolic)
    """
    s = to_str(bp_value)
    if not s:
        return (None, None)

    nums = re.findall(r"\d+", s)
    if len(nums) >= 2:
        return (to_int(nums[0]), to_int(nums[1]))
    return (None, None)


def risk_label_to_int(v):
    """
    For Assessments.RiskStratificationScore if SQL column is INT.
    Maps common labels to numeric score.
    """
    if v is None or pd.isna(v):
        return None

    n = to_int(v)
    if n is not None:
        return n

    s = str(v).strip().lower()
    if s == "":
        return None

    mapping = {
        "low": 1,
        "moderate": 2,
        "medium": 2,
        "high": 3,
        "very high": 4,
    }

    if s in mapping:
        return mapping[s]

    for k, val in mapping.items():
        if k in s:
            return val

    return None


def dedupe_by_key(df: pd.DataFrame, key_cols):
    return df.drop_duplicates(subset=key_cols, keep="first").copy()


def read_excel(path: str):
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"File not found: {p}")
    return pd.read_excel(p)


def sql_existing_keys(cursor, table_name, key_cols):
    cols = ", ".join([f"[{c}]" for c in key_cols])
    sql = f"SELECT {cols} FROM dbo.[{table_name}]"
    cursor.execute(sql)
    rows = cursor.fetchall()

    out = set()
    for r in rows:
        vals = []
        for i, _ in enumerate(key_cols):
            vals.append(_normalize_key_value(r[i]))
        out.add(tuple(vals))
    return out


def _normalize_key_value(x):
    if isinstance(x, datetime):
        return x.date()
    if isinstance(x, str):
        s = x.strip()
        if s == "":
            return None
        try:
            return int(s)
        except Exception:
            return s
    return x


def _make_key_tuple(row_dict, key_cols):
    return tuple(_normalize_key_value(row_dict.get(c)) for c in key_cols)


def _dedupe_row_dicts(rows, key_cols):
    seen = set()
    out = []
    for r in rows:
        k = _make_key_tuple(r, key_cols)
        if k in seen:
            continue
        seen.add(k)
        out.append(r)
    return out


def insert_rows(cursor, table_name, rows, columns):
    if not rows:
        return 0

    placeholders = ", ".join(["?"] * len(columns))
    col_sql = ", ".join([f"[{c}]" for c in columns])
    sql = f"INSERT INTO dbo.[{table_name}] ({col_sql}) VALUES ({placeholders})"

    data = [tuple(r.get(c) for c in columns) for r in rows]

    cursor.fast_executemany = True
    try:
        cursor.executemany(sql, data)
        return len(rows)
    except Exception as bulk_err:
        print(f"\n[DEBUG] Bulk insert failed for {table_name}. Trying row-by-row...")
        print(f"[DEBUG] Bulk error: {bulk_err}")

        if not DEBUG_ROW_FALLBACK:
            raise

        inserted = 0
        conn = cursor.connection
        row_cur = conn.cursor()
        row_cur.fast_executemany = False

        for idx, row in enumerate(data, start=1):
            try:
                row_cur.execute(sql, row)
                inserted += 1
            except Exception as row_err:
                print(f"[DEBUG] Failed row #{idx} in {table_name}")
                for c, v in zip(columns, row):
                    print(f"   {c}: {repr(v)}")
                print(f"[DEBUG] Error: {row_err}")
                row_cur.close()
                raise

        row_cur.close()
        return inserted


def ensure_health_core_columns(df_health):
    m = build_normalized_col_map(df_health)

    col_saheli = pick_col(m, "Saheli Card No", "SaheliCardNo", "Saheli Card Number")
    col_ass_no = pick_col(m, "Assessment Number", "AssessmentNumber")
    col_ass_date = pick_col(m, "Completion time", "AssessmentDate")

    if not col_saheli:
        raise KeyError("HEALTH_OUTPUT_FILE: Saheli Card No column not found")
    if not col_ass_no:
        raise KeyError("HEALTH_OUTPUT_FILE: AssessmentNumber column not found")
    if not col_ass_date:
        raise KeyError("HEALTH_OUTPUT_FILE: Completion time column not found")

    return m, col_saheli, col_ass_no, col_ass_date


def ensure_reg_core_columns(df_reg):
    m = build_normalized_col_map(df_reg)
    col_saheli = pick_col(m, "Saheli Card No", "SaheliCardNo", "Saheli Card Number")
    if not col_saheli:
        raise KeyError("REG_OUTPUT_FILE: Saheli Card No column not found")
    return m, col_saheli


# =========================
# REGISTRATION BUILDERS
# =========================
def build_participants_from_reg(df_reg: pd.DataFrame):
    m, col_saheli = ensure_reg_core_columns(df_reg)

    col_fullname = pick_col(m, "Full Name")
    col_dob = pick_col(m, "Date of Birth")
    col_age = pick_col(m, "Age")
    col_address = pick_col(m, "Address")
    col_postcode = pick_col(m, "Postcode")
    col_email = pick_col(m, "Email", occurrence=2) or pick_col(m, "Email", occurrence=1)
    col_mobile = pick_col(m, "Mobile/Home No", "Mobile Number", "Mobile")
    col_gender = pick_col(m, "Gender")
    col_gender_same = pick_col(m, "Is your gender the same as assigned at birth")
    col_ethnicity = pick_col(m, "Ethnicity")
    col_lang = pick_col(m, "Preferred spoken language")
    col_religion = pick_col(m, "Religion")
    col_sexuality = pick_col(m, "Sexuality")
    col_occupation = pick_col(m, "Occupation")
    col_living_alone = pick_col(m, "Living alone")
    col_caring = pick_col(m, "Caring responsibilities")
    col_referral = pick_col(m, "Referral reason")
    col_heard = pick_col(m, "How heard about Saheli Hub")
    col_gp = pick_col(m, "GP Surgery Name")
    col_created = pick_col(m, "Date") or pick_col(m, "Completion time") or pick_col(m, "Start time")

    df = df_reg.copy()
    df["_SaheliKey"] = df[col_saheli].apply(to_saheli_key)
    df = df[df["_SaheliKey"].notna()].copy()
    df = dedupe_by_key(df, ["_SaheliKey"])

    rows = []
    for _, r in df.iterrows():
        rows.append({
            "SaheliCardNumber": to_int(r["_SaheliKey"]),
            "FullName": to_str(r[col_fullname]) if col_fullname else None,
            "DateOfBirth": parse_date(r[col_dob]) if col_dob else None,
            "Age": to_int(r[col_age]) if col_age else None,
            "Address": to_str(r[col_address]) if col_address else None,
            "Postcode": to_str(r[col_postcode]) if col_postcode else None,
            "Email": to_str(r[col_email]) if col_email else None,
            "MobileNumber": to_str(r[col_mobile]) if col_mobile else None,
            "Gender": to_str(r[col_gender]) if col_gender else None,
            "GenderSameAsBirth": to_bit(r[col_gender_same]) if col_gender_same else None,
            "Ethnicity": to_str(r[col_ethnicity]) if col_ethnicity else None,
            "PreferredLanguage": to_str(r[col_lang]) if col_lang else None,
            "Religion": to_str(r[col_religion]) if col_religion else None,
            "Sexuality": to_str(r[col_sexuality]) if col_sexuality else None,
            "Occupation": to_str(r[col_occupation]) if col_occupation else None,
            "LivingAlone": to_bit(r[col_living_alone]) if col_living_alone else None,
            "CaringResponsibilities": to_bit(r[col_caring]) if col_caring else None,
            "ReferralReason": to_str(r[col_referral]) if col_referral else None,
            "HeardAboutSaheli": to_str(r[col_heard]) if col_heard else None,
            "GPSurgeryName": to_str(r[col_gp]) if col_gp else None,
            "CreatedAt": parse_datetime(r[col_created]) if col_created else None,
        })
    return rows


def build_emergency_contacts_from_reg(df_reg: pd.DataFrame, saheli_to_participant_id: dict):
    m, col_saheli = ensure_reg_core_columns(df_reg)

    col_name = pick_col(m, "Emergency Contact Name")
    col_num = pick_col(m, "Emergency No")
    col_rel = pick_col(m, "Emergency Relation To You")

    if not any([col_name, col_num, col_rel]):
        return []

    df = df_reg.copy()
    df["_SaheliKey"] = df[col_saheli].apply(to_saheli_key)
    df = df[df["_SaheliKey"].notna()].copy()
    df = dedupe_by_key(df, ["_SaheliKey"])

    rows = []
    for _, r in df.iterrows():
        saheli = to_int(r["_SaheliKey"])
        if saheli is None:
            continue
        pid = saheli_to_participant_id.get(saheli)
        if pid is None:
            continue

        contact_name = to_str(r[col_name]) if col_name else None
        contact_num = to_str(r[col_num]) if col_num else None
        rel = to_str(r[col_rel]) if col_rel else None

        if not any([contact_name, contact_num, rel]):
            continue

        rows.append({
            "SaheliCardNumber": saheli,
            "ContactName": contact_name,
            "ContactNumber": contact_num,
            "Relationship": rel,
            "ParticipantID": pid,
        })
    return rows


# =========================
# HEALTH BUILDERS
# =========================
def build_health_base(df_health: pd.DataFrame):
    m, col_saheli, col_ass_no, col_ass_date = ensure_health_core_columns(df_health)

    df = df_health.copy()
    df["_SaheliKey"] = df[col_saheli].apply(to_saheli_key)          # keep as string source key
    df["_AssessmentNumber"] = df[col_ass_no].apply(to_int)
    df["_AssessmentDate"] = df[col_ass_date].apply(parse_date)

    df = df[df["_SaheliKey"].notna() & df["_AssessmentNumber"].notna()].copy()
    df = dedupe_by_key(df, ["_SaheliKey", "_AssessmentNumber"])
    return df, m


def hcol(m, *names, occurrence=1):
    return pick_col(m, *names, occurrence=occurrence)


def build_assessment_master_rows(dfh, m):
    rows = []
    for _, r in dfh.iterrows():
        rows.append({
            "SaheliCardNumber": to_str(r["_SaheliKey"]),  # SQL varchar(50)
            "AssessmentNumber": to_int(r["_AssessmentNumber"]),
            "AssessmentDate": parse_datetime(r["_AssessmentDate"]) if r["_AssessmentDate"] else None,  # datetime2
            "CreatedAtUtc": parse_datetime(r["_AssessmentDate"]) if r["_AssessmentDate"] else None,    # datetime2
            "CreatedByUserId": DEFAULT_CREATED_BY_USER_ID,  # int nullable in your schema
        })
    return rows


def build_assessments_rows(dfh, m):
    c_staff = hcol(m, "Staff Name", "Name")
    c_site = hcol(m, "Site")
    c_risk = hcol(m, "Risk Stratification Score")
    c_next = hcol(m, "Date of next review appointment")

    rows = []
    for _, r in dfh.iterrows():
        rows.append({
            "SaheliCardNumber": to_str(r["_SaheliKey"]),
            "AssessmentNumber": to_int(r["_AssessmentNumber"]),
            "AssessmentDate": r["_AssessmentDate"],
            "StaffMember": to_str(r[c_staff]) if c_staff else None,
            "Site": to_str(r[c_site]) if c_site else None,
            "RiskStratificationScore": risk_label_to_int(r[c_risk]) if c_risk else None,
            "NextReviewDate": parse_date(r[c_next]) if c_next else None,
            "CreatedAt": parse_datetime(r["_AssessmentDate"]) if r["_AssessmentDate"] else None,
        })
    return rows


def build_aims_goals_rows(dfh, m):
    c_aims = hcol(m, "What are your aims & goals")
    c_desc = hcol(m, "Comments5")

    rows = []
    for _, r in dfh.iterrows():
        rows.append({
            "SaheliCardNumber": to_str(r["_SaheliKey"]),
            "AssessmentNumber": to_int(r["_AssessmentNumber"]),
            "AssessmentDate": r["_AssessmentDate"],
            "AimsGoals": to_str(r[c_aims]) if c_aims else None,
            "AimsDescription": to_str(r[c_desc]) if c_desc else None,
        })
    return rows


def build_barriers_rows(dfh, m):
    c_bar = hcol(m, "What reasons stop you from joining activities")
    c_com = hcol(m, "Comments6")

    rows = []
    for _, r in dfh.iterrows():
        rows.append({
            "SaheliCardNumber": to_str(r["_SaheliKey"]),
            "AssessmentNumber": to_int(r["_AssessmentNumber"]),
            "AssessmentDate": r["_AssessmentDate"],
            "Barriers": to_str(r[c_bar]) if c_bar else None,
            "BarrierComments": to_str(r[c_com]) if c_com else None,
        })
    return rows


def build_body_comp_rows(dfh, m):
    c_weight = hcol(m, "Weight (KG)")
    c_height = hcol(m, "Height (CM)")
    c_bmi_val = hcol(m, "BMI")
    c_bmi_cat = hcol(m, "BMI Results")
    c_waist = hcol(m, "Waist (CM)")
    c_hip = hcol(m, "Hip (CM)")
    c_whr = hcol(m, "Waist to Hip Ratio (CM)")
    c_bf_cat = hcol(m, "Body Fat Percentage Result")
    c_bf_score = hcol(m, "Body Fat Percentage Score")
    c_vf_cat = hcol(m, "Visceral Fat Level Result")
    c_vf_score = hcol(m, "Visceral Fat Level Score")
    c_sm_cat = hcol(m, "Skeletal Muscle Percentage")
    c_sm_score = hcol(m, "Skeletal Muscle Score")
    c_rm = hcol(m, "Resting Metabolism")

    rows = []
    for _, r in dfh.iterrows():
        rows.append({
            "SaheliCardNumber": to_str(r["_SaheliKey"]),
            "AssessmentNumber": to_int(r["_AssessmentNumber"]),
            "AssessmentDate": r["_AssessmentDate"],
            "WeightKg": to_float(r[c_weight]) if c_weight else None,
            "HeightCm": to_float(r[c_height]) if c_height else None,
            "Bmicategory": to_str(r[c_bmi_cat]) if c_bmi_cat else None,
            "Bmivalue": to_float(r[c_bmi_val]) if c_bmi_val else None,
            "WaistCm": to_float(r[c_waist]) if c_waist else None,
            "HipCm": to_float(r[c_hip]) if c_hip else None,
            "WaistHipRatio": to_float(r[c_whr]) if c_whr else None,
            "BodyFatCategory": to_str(r[c_bf_cat]) if c_bf_cat else None,
            "BodyFatScore": to_float(r[c_bf_score]) if c_bf_score else None,
            "VisceralFatCategory": to_str(r[c_vf_cat]) if c_vf_cat else None,
            "VisceralFatScore": to_float(r[c_vf_score]) if c_vf_score else None,
            "SkeletalMuscleCategory": to_str(r[c_sm_cat]) if c_sm_cat else None,
            "SkeletalMuscleScore": to_float(r[c_sm_score]) if c_sm_score else None,
            "RestingMetabolism": to_float(r[c_rm]) if c_rm else None,
        })
    return rows


def build_community_conf_rows(dfh, m):
    c_conf = hcol(m, "How confident are you to join activities")
    c_hobbies = hcol(m, "How many hobbies and passions do you have")
    c_comm = hcol(m, "How involved you feel in your community")
    c_serv = hcol(m, "How much you know about local support/services")

    rows = []
    for _, r in dfh.iterrows():
        rows.append({
            "SaheliCardNumber": to_str(r["_SaheliKey"]),
            "AssessmentNumber": to_int(r["_AssessmentNumber"]),
            "AssessmentDate": r["_AssessmentDate"],
            "ConfidenceToJoin": to_int(r[c_conf]) if c_conf else None,
            "NumberOfHobbies": to_int(r[c_hobbies]) if c_hobbies else None,
            "CommunityInvolvement": to_int(r[c_comm]) if c_comm else None,
            "ServiceAwareness": to_int(r[c_serv]) if c_serv else None,
        })
    return rows


def build_health_screening_rows(dfh, m):
    c_has_cond = hcol(m, "Do You Have Any Health Condition")
    c_last_bp = hcol(m, "When did you last measure your blood pressure")
    c_bp_rec = hcol(m, "Have you recorded your blood pressure measurement and registered it with a GP or Pharmacist")
    c_kn_bp = hcol(m, "What is a healthy blood pressure for an adult")
    c_kn_risk = hcol(m, "Why is a high blood pressure dangerous")
    c_kn_reduce = hcol(m, "How can you help reduce your blood pressure")
    c_bp = hcol(m, "Blood Pressure (Systolic/Diastolic)")
    c_bp_level = hcol(m, "Blood Pressure Level")
    c_heart_cond = hcol(m, "Do You Have a Heart Condition")
    c_hr = hcol(m, "Heart Rate (BPM)")
    c_af = hcol(m, "Atrial Fibrillation Result")
    c_ha = hcol(m, "Heart Age")
    c_doc_no_ex = hcol(m, "Did Your Doctor Advise You Not to Exercise")
    c_chest = hcol(m, "Do You Feel Pain in Chest at Rest/During Activity")
    c_sob = hcol(m, "Do You Have Shortness of Breath")
    c_diab = hcol(m, "Do You Have Diabetes")
    c_diab_risk = hcol(m, "Diabetes Risk")
    c_glu = hcol(m, "Glucose Level ( mg/dL)")
    c_hba1c = hcol(m, "HbA1c")
    c_sugar = hcol(m, "Do You Take Sugary Drinks, Including Chai")
    c_chol = hcol(m, "Do You Have High Cholesterol (Total/HDL)")
    c_other = hcol(m, "Do You Experience The Following Health Issues")
    c_bone = hcol(m, "Do You Have a Bone / joint Condition")
    c_meds = hcol(m, "Do You Take Any Prescribed Medication")
    c_refdoc = hcol(m, "Referred to doctor for any concerning results")
    c_risk = hcol(m, "Risk Stratification Score")
    c_comments = hcol(m, "Comments", occurrence=1)
    c_self_mgmt = hcol(m, "How well do you manage your health/condition(s) (Rating out of 10)")

    rows = []
    for _, r in dfh.iterrows():
        sys_bp, dia_bp = split_bp(r[c_bp] if c_bp else None)
        rows.append({
            "SaheliCardNumber": to_str(r["_SaheliKey"]),
            "AssessmentNumber": to_int(r["_AssessmentNumber"]),
            "AssessmentDate": r["_AssessmentDate"],

            # bit columns
            "HasHealthCondition": to_bit(r[c_has_cond]) if c_has_cond else None,
            "DoctorAdvisedNoExercise": to_bit(r[c_doc_no_ex]) if c_doc_no_ex else None,
            "ChestPain": to_bit(r[c_chest]) if c_chest else None,
            "SugaryDrinkIntake": to_bit(r[c_sugar]) if c_sugar else None,
            "HighCholesterol": to_bit(r[c_chol]) if c_chol else None,
            "TakesPrescribedMedication": to_bit(r[c_meds]) if c_meds else None,
            "ReferredToDoctor": to_bit(r[c_refdoc]) if c_refdoc else None,

            # NOTE: schema says nvarchar(max), not bit
            "BprecordedWithGp": to_str(r[c_bp_rec]) if c_bp_rec else None,
            "ShortnessOfBreath": to_str(r[c_sob]) if c_sob else None,

            # date/text/numeric columns
            "LastBpmeasurementDate": parse_date(r[c_last_bp]) if c_last_bp else None,
            "KnowledgeHealthyBp": to_str(r[c_kn_bp]) if c_kn_bp else None,
            "KnowledgeBprisk": to_str(r[c_kn_risk]) if c_kn_risk else None,
            "KnowledgeBpreduction": to_str(r[c_kn_reduce]) if c_kn_reduce else None,
            "SystolicBp": sys_bp,
            "DiastolicBp": dia_bp,
            "Bplevel": to_str(r[c_bp_level]) if c_bp_level else None,
            "HeartConditionTypes": to_str(r[c_heart_cond]) if c_heart_cond else None,
            "HeartRateBpm": to_first_int_from_text(r[c_hr]) if c_hr else None,
            "AtrialFibrillationResult": to_first_int_from_text(r[c_af]) if c_af else None,
            "HeartAge": to_int(r[c_ha]) if c_ha else None,
            "DiabetesType": to_str(r[c_diab]) if c_diab else None,
            "DiabetesRisk": to_str(r[c_diab_risk]) if c_diab_risk else None,
            "GlucoseLevel": to_float(r[c_glu]) if c_glu else None,
            "HbA1c": to_float(r[c_hba1c]) if c_hba1c else None,
            "OtherHealthIssues": to_str(r[c_other]) if c_other else None,
            "BoneJointConditions": to_str(r[c_bone]) if c_bone else None,
            "RiskStratification": to_str(r[c_risk]) if c_risk else None,
            "HealthComments": to_str(r[c_comments]) if c_comments else None,
            "SelfManagementScore": to_int(r[c_self_mgmt]) if c_self_mgmt else None,
        })
    return rows


def build_lifestyle_rows(dfh, m):
    c_n = hcol(m, "Nourishment: Rate the quality of the food you put into your body on a daily basis")
    c_m = hcol(m, "Movement: Rate how often and for how long you move your body on a daily basis")
    c_c = hcol(m, "Connectedness: Rate how well you stay connected with family, friends and your higher power")
    c_s = hcol(m, "Sleep: Rate the quality of your sleep")
    c_h = hcol(m, "Happy self: Rate how often and for how long you perform positive practices")
    c_r = hcol(m, "Resilience: Rate how well you are able to manage stress in your life")
    c_g = hcol(m, "Green and Blue: Rate how often and how long you spend in nature or outdoors")
    c_st = hcol(m, "Screen time: Rate how happy you are with your current amount of screen time")
    c_sub = hcol(m, "Substance use: Rate how comfortable you are with any current substance use")
    c_p = hcol(m, "Purpose: Rate how well you feel you are fulfilling your passion")
    c_com = hcol(m, "Comments3")

    rows = []
    for _, r in dfh.iterrows():
        rows.append({
            "SaheliCardNumber": to_str(r["_SaheliKey"]),
            "AssessmentNumber": to_int(r["_AssessmentNumber"]),
            "AssessmentDate": r["_AssessmentDate"],
            "Nourishment": to_int(r[c_n]) if c_n else None,
            "Movement": to_int(r[c_m]) if c_m else None,
            "Connectedness": to_int(r[c_c]) if c_c else None,
            "SleepQuality": to_int(r[c_s]) if c_s else None,
            "HappySelf": to_int(r[c_h]) if c_h else None,
            "Resilience": to_int(r[c_r]) if c_r else None,
            "GreenBlueSpace": to_int(r[c_g]) if c_g else None,
            "ScreenTime": to_int(r[c_st]) if c_st else None,
            "SubstanceUse": to_int(r[c_sub]) if c_sub else None,
            "Purpose": to_int(r[c_p]) if c_p else None,
            "LifestyleComments": to_str(r[c_com]) if c_com else None,
        })
    return rows


def build_physical_activity_rows(dfh, m):
    c_days = hcol(m, "In the past week, on how many days have you done a total of 30 mins or more of physical activity")
    c_level = hcol(m, "Physical Activity Level")
    c_com = hcol(m, "Comments", occurrence=2) or hcol(m, "CommentsPA")

    rows = []
    for _, r in dfh.iterrows():
        rows.append({
            "SaheliCardNumber": to_str(r["_SaheliKey"]),
            "AssessmentNumber": to_int(r["_AssessmentNumber"]),
            "AssessmentDate": r["_AssessmentDate"],
            "ActiveDaysPerWeek": to_int(r[c_days]) if c_days else None,
            "ActivityLevel": to_str(r[c_level]) if c_level else None,
            "ActivityComments": to_str(r[c_com]) if c_com else None,
        })
    return rows


def build_preferred_activities_rows(dfh, m):
    c_pref = hcol(m, "What are your preferred activities")
    c_com = hcol(m, "Comments7")
    c_next = hcol(m, "Date of next review appointment")

    rows = []
    for _, r in dfh.iterrows():
        rows.append({
            "SaheliCardNumber": to_str(r["_SaheliKey"]),
            "AssessmentNumber": to_int(r["_AssessmentNumber"]),
            "AssessmentDate": r["_AssessmentDate"],
            "PreferredActivities": to_str(r[c_pref]) if c_pref else None,
            "ActivityComments": to_str(r[c_com]) if c_com else None,
            "NextReviewDate": parse_date(r[c_next]) if c_next else None,
        })
    return rows


def build_social_isolation_rows(dfh, m):
    c_lack = hcol(m, "How often do you feel that you lack companionship")
    c_left = hcol(m, "How often do you feel left out")
    c_iso = hcol(m, "How often do you feel isolated from others")
    c_com = hcol(m, "Comments4")

    rows = []
    for _, r in dfh.iterrows():
        rows.append({
            "SaheliCardNumber": to_str(r["_SaheliKey"]),
            "AssessmentNumber": to_int(r["_AssessmentNumber"]),
            "AssessmentDate": r["_AssessmentDate"],
            "LackCompanionship": to_int(r[c_lack]) if c_lack else None,
            "FeelLeftOut": to_int(r[c_left]) if c_left else None,
            "FeelIsolated": to_int(r[c_iso]) if c_iso else None,
            "SocialIsolationComments": to_str(r[c_com]) if c_com else None,
        })
    return rows


def build_wemwbs_rows(dfh, m):
    cols = {
        "FeelingOptimistic": hcol(m, "I’ve been feeling optimistic about the future", "I've been feeling optimistic about the future"),
        "FeelingUseful": hcol(m, "I’ve been feeling useful", "I've been feeling useful"),
        "FeelingRelaxed": hcol(m, "I’ve been feeling relaxed", "I've been feeling relaxed"),
        "FeelingInterestedInPeople": hcol(m, "I’ve been feeling interested in other people", "I've been feeling interested in other people"),
        "EnergyToSpare": hcol(m, "I’ve had energy to spare", "I've had energy to spare"),
        "DealingWithProblems": hcol(m, "I’ve been dealing with problems well", "I've been dealing with problems well"),
        "ThinkingClearly": hcol(m, "I’ve been thinking clearly", "I've been thinking clearly"),
        "FeelingGoodAboutSelf": hcol(m, "I’ve been feeling good about myself", "I've been feeling good about myself"),
        "FeelingCloseToOthers": hcol(m, "I’ve been feeling close to other people", "I've been feeling close to other people"),
        "FeelingConfident": hcol(m, "I’ve been feeling confident", "I've been feeling confident"),
        "MakingOwnMindUp": hcol(m, "I’ve been able to make up my own mind about things", "I've been able to make up my own mind about things"),
        "FeelingLoved": hcol(m, "I’ve been feeling loved", "I've been feeling loved"),
        "InterestedInNewThings": hcol(m, "I’ve been interested in new things", "I've been interested in new things"),
        "FeelingCheerful": hcol(m, "I’ve been feeling cheerful", "I've been feeling cheerful"),
    }
    c_com = hcol(m, "Comments2")

    rows = []
    for _, r in dfh.iterrows():
        row = {
            "SaheliCardNumber": to_str(r["_SaheliKey"]),
            "AssessmentNumber": to_int(r["_AssessmentNumber"]),
            "AssessmentDate": r["_AssessmentDate"],
            "Wemwbscomments": to_str(r[c_com]) if c_com else None,
        }
        for out_col, src_col in cols.items():
            row[out_col] = to_int(r[src_col]) if src_col else None
        rows.append(row)
    return rows


# =========================
# INSERT LOGIC
# =========================
def insert_if_missing(cursor, table_name, rows, key_cols, insert_cols):
    if not rows:
        print(f"[{table_name}] Source rows: 0 | Unique source: 0 | New rows: 0 | Inserted: 0")
        return 0

    unique_rows = _dedupe_row_dicts(rows, key_cols)
    existing = sql_existing_keys(cursor, table_name, key_cols)

    to_insert = []
    for r in unique_rows:
        key = _make_key_tuple(r, key_cols)
        if key not in existing:
            to_insert.append(r)

    inserted = insert_rows(cursor, table_name, to_insert, insert_cols)

    print(
        f"[{table_name}] Source rows: {len(rows)} | Unique source: {len(unique_rows)} | "
        f"New rows: {len(to_insert)} | Inserted: {inserted}"
    )
    return inserted


def fetch_participant_id_map(cursor):
    cursor.execute("SELECT ParticipantID, SaheliCardNumber FROM dbo.Participants")
    m = {}
    for pid, saheli in cursor.fetchall():
        if saheli is not None:
            try:
                m[int(saheli)] = int(pid)
            except Exception:
                pass
    return m


# =========================
# MAIN
# =========================
def main():
    print("=== READ EXCEL FILES ===")
    df_reg = read_excel(REG_OUTPUT_FILE)
    df_health = read_excel(HEALTH_OUTPUT_FILE)

    print(f"REG rows:    {len(df_reg)}")
    print(f"HEALTH rows: {len(df_health)}")

    print("\n=== BUILD REGISTRATION ROWS ===")
    participant_rows = build_participants_from_reg(df_reg)
    print(f"Participants prepared rows: {len(participant_rows)}")

    print("\n=== BUILD HEALTH ROWS ===")
    dfh, hm = build_health_base(df_health)
    print(f"Assessments prepared rows (unique by Saheli+AssessmentNumber): {len(dfh)}")

    rows_assessment_master = build_assessment_master_rows(dfh, hm)
    rows_assessments = build_assessments_rows(dfh, hm)
    rows_aims = build_aims_goals_rows(dfh, hm)
    rows_barriers = build_barriers_rows(dfh, hm)
    rows_body = build_body_comp_rows(dfh, hm)
    rows_comm = build_community_conf_rows(dfh, hm)
    rows_healthscreen = build_health_screening_rows(dfh, hm)
    rows_lifestyle = build_lifestyle_rows(dfh, hm)
    rows_pa = build_physical_activity_rows(dfh, hm)
    rows_pref = build_preferred_activities_rows(dfh, hm)
    rows_social = build_social_isolation_rows(dfh, hm)
    rows_wem = build_wemwbs_rows(dfh, hm)

    print("\n=== SQL CONNECT & INSERT ===")
    conn = pyodbc.connect(SQL_CONN_STR)
    conn.autocommit = False
    cur = conn.cursor()

    try:
        # 1) Participants
        insert_if_missing(
            cur,
            "Participants",
            participant_rows,
            key_cols=["SaheliCardNumber"],
            insert_cols=[
                "SaheliCardNumber", "FullName", "DateOfBirth", "Age", "Address", "Postcode", "Email",
                "MobileNumber", "Gender", "GenderSameAsBirth", "Ethnicity", "PreferredLanguage", "Religion",
                "Sexuality", "Occupation", "LivingAlone", "CaringResponsibilities", "ReferralReason",
                "HeardAboutSaheli", "GPSurgeryName", "CreatedAt"
            ],
        )

        # 2) ParticipantEmergencyContacts
        saheli_to_pid = fetch_participant_id_map(cur)
        emergency_rows = build_emergency_contacts_from_reg(df_reg, saheli_to_pid)

        insert_if_missing(
            cur,
            "ParticipantEmergencyContacts",
            emergency_rows,
            key_cols=["SaheliCardNumber"],
            insert_cols=["SaheliCardNumber", "ContactName", "ContactNumber", "Relationship", "ParticipantID"],
        )

        # 3) Assessment tables
        insert_if_missing(
            cur,
            "Assessment_Master",
            rows_assessment_master,
            key_cols=["SaheliCardNumber", "AssessmentNumber"],
            insert_cols=["SaheliCardNumber", "AssessmentNumber", "AssessmentDate", "CreatedAtUtc", "CreatedByUserId"],
        )

        insert_if_missing(
            cur,
            "Assessments",
            rows_assessments,
            key_cols=["SaheliCardNumber", "AssessmentNumber"],
            insert_cols=[
                "SaheliCardNumber", "AssessmentNumber", "AssessmentDate", "StaffMember", "Site",
                "RiskStratificationScore", "NextReviewDate", "CreatedAt"
            ],
        )

        insert_if_missing(
            cur,
            "Assessment_AimsGoals",
            rows_aims,
            key_cols=["SaheliCardNumber", "AssessmentNumber"],
            insert_cols=["SaheliCardNumber", "AssessmentNumber", "AssessmentDate", "AimsGoals", "AimsDescription"],
        )

        insert_if_missing(
            cur,
            "Assessment_Barriers",
            rows_barriers,
            key_cols=["SaheliCardNumber", "AssessmentNumber"],
            insert_cols=["SaheliCardNumber", "AssessmentNumber", "AssessmentDate", "Barriers", "BarrierComments"],
        )

        insert_if_missing(
            cur,
            "Assessment_BodyComposition",
            rows_body,
            key_cols=["SaheliCardNumber", "AssessmentNumber"],
            insert_cols=[
                "SaheliCardNumber", "AssessmentNumber", "AssessmentDate", "WeightKg", "HeightCm", "Bmicategory",
                "Bmivalue", "WaistCm", "HipCm", "WaistHipRatio", "BodyFatCategory", "BodyFatScore",
                "VisceralFatCategory", "VisceralFatScore", "SkeletalMuscleCategory", "SkeletalMuscleScore",
                "RestingMetabolism"
            ],
        )

        insert_if_missing(
            cur,
            "Assessment_CommunityConfidence",
            rows_comm,
            key_cols=["SaheliCardNumber", "AssessmentNumber"],
            insert_cols=[
                "SaheliCardNumber", "AssessmentNumber", "AssessmentDate", "ConfidenceToJoin",
                "NumberOfHobbies", "CommunityInvolvement", "ServiceAwareness"
            ],
        )

        insert_if_missing(
            cur,
            "Assessment_HealthScreening",
            rows_healthscreen,
            key_cols=["SaheliCardNumber", "AssessmentNumber"],
            insert_cols=[
                "SaheliCardNumber", "AssessmentNumber", "AssessmentDate",
                "HasHealthCondition", "LastBpmeasurementDate", "BprecordedWithGp",
                "KnowledgeHealthyBp", "KnowledgeBprisk", "KnowledgeBpreduction",
                "SystolicBp", "DiastolicBp", "Bplevel", "HeartConditionTypes", "HeartRateBpm",
                "AtrialFibrillationResult", "HeartAge", "DoctorAdvisedNoExercise", "ChestPain",
                "ShortnessOfBreath", "DiabetesType", "DiabetesRisk", "GlucoseLevel", "HbA1c",
                "SugaryDrinkIntake", "HighCholesterol", "OtherHealthIssues", "BoneJointConditions",
                "TakesPrescribedMedication", "ReferredToDoctor", "RiskStratification", "HealthComments",
                "SelfManagementScore"
            ],
        )

        insert_if_missing(
            cur,
            "Assessment_Lifestyle",
            rows_lifestyle,
            key_cols=["SaheliCardNumber", "AssessmentNumber"],
            insert_cols=[
                "SaheliCardNumber", "AssessmentNumber", "AssessmentDate", "Nourishment", "Movement",
                "Connectedness", "SleepQuality", "HappySelf", "Resilience", "GreenBlueSpace",
                "ScreenTime", "SubstanceUse", "Purpose", "LifestyleComments"
            ],
        )

        insert_if_missing(
            cur,
            "Assessment_PhysicalActivity",
            rows_pa,
            key_cols=["SaheliCardNumber", "AssessmentNumber"],
            insert_cols=[
                "SaheliCardNumber", "AssessmentNumber", "AssessmentDate",
                "ActiveDaysPerWeek", "ActivityLevel", "ActivityComments"
            ],
        )

        insert_if_missing(
            cur,
            "Assessment_PreferredActivities",
            rows_pref,
            key_cols=["SaheliCardNumber", "AssessmentNumber"],
            insert_cols=[
                "SaheliCardNumber", "AssessmentNumber", "AssessmentDate",
                "PreferredActivities", "ActivityComments", "NextReviewDate"
            ],
        )

        insert_if_missing(
            cur,
            "Assessment_SocialIsolation",
            rows_social,
            key_cols=["SaheliCardNumber", "AssessmentNumber"],
            insert_cols=[
                "SaheliCardNumber", "AssessmentNumber", "AssessmentDate",
                "LackCompanionship", "FeelLeftOut", "FeelIsolated", "SocialIsolationComments"
            ],
        )

        insert_if_missing(
            cur,
            "Assessment_WEMWBS",
            rows_wem,
            key_cols=["SaheliCardNumber", "AssessmentNumber"],
            insert_cols=[
                "SaheliCardNumber", "AssessmentNumber", "AssessmentDate",
                "FeelingOptimistic", "FeelingUseful", "FeelingRelaxed", "FeelingInterestedInPeople",
                "EnergyToSpare", "DealingWithProblems", "ThinkingClearly", "FeelingGoodAboutSelf",
                "FeelingCloseToOthers", "FeelingConfident", "MakingOwnMindUp", "FeelingLoved",
                "InterestedInNewThings", "FeelingCheerful", "Wemwbscomments"
            ],
        )

        conn.commit()
        print("\n✅ SUCCESS: Missing rows inserted and existing rows skipped.")

    except Exception as e:
        conn.rollback()
        print("\n❌ ERROR: Transaction rolled back.")
        print(e)
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()