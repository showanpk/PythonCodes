# ============================================================
# Q FULL FILE 2: saheli_create_wide_output.py
# ------------------------------------------------------------
# Creates final WIDE Excel output by merging:
#   - Registrations_Cleaned.xlsx
#   - Healthassessments_Prepared.xlsx
#
# FIX INCLUDED:
#   - Safe Saheli key cleaning (no 1 -> 10 bug)
#   - Strong key normalization before merge
#
# Output:
#   - One row per Saheli Card Number
#   - "No of assessment completed"
#   - Registration fields
#   - 1st Assessment, 2nd Assessment, ... blocks
#   - Assessment date stored in "1st Assessment" / "2nd Assessment" columns
#
# Install:
#   pip install pandas openpyxl
# ============================================================

from pathlib import Path
import re
import pandas as pd


# =========================
# CONFIG
# =========================
REG_FILE = r"C:\Users\shonk\source\PythonCodes\New folder\Registrations_Cleaned.xlsx"
HEALTH_FILE = r"C:\Users\shonk\source\PythonCodes\New folder\Healthassessments_Prepared.xlsx"
OUTPUT_FILE = r"C:\Users\shonk\source\PythonCodes\New folder\Saheli_Master_Wide_Output.xlsx"

REG_SHEET_NAME = None
HEALTH_SHEET_NAME = None

MAX_ASSESSMENTS = 6


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


def keep_digits_only(v):
    """Safe cleaner for IDs (prevents 1.0 -> 10)."""
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


def safe_saheli_key_series(series: pd.Series) -> pd.Series:
    """
    Standardize Saheli keys across files so merge always matches:
    - clean digits
    - convert to Int64
    - convert to string
    """
    s = series.apply(keep_digits_only)
    s = pd.to_numeric(s, errors="coerce").astype("Int64")
    return s.astype("string")


def read_excel_flexible(path: str, sheet_name=None) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"File not found: {p}")
    return pd.read_excel(p) if sheet_name is None else pd.read_excel(p, sheet_name=sheet_name)


def write_excel(df: pd.DataFrame, out_path: str):
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out, engine="openpyxl", datetime_format="dd/mm/yyyy", date_format="dd/mm/yyyy") as writer:
        df.to_excel(writer, index=False)
    return out


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


def parse_date(series):
    dt = pd.to_datetime(series, errors="coerce", dayfirst=True)
    return dt.dt.date


def compute_age(dob_series, ref_series=None):
    dob = pd.to_datetime(dob_series, errors="coerce", dayfirst=True)
    if ref_series is None:
        ref = pd.Series([pd.Timestamp.today().normalize()] * len(dob), index=dob.index)
    else:
        ref = pd.to_datetime(ref_series, errors="coerce", dayfirst=True)

    age = ref.dt.year - dob.dt.year
    before_bday = (ref.dt.month < dob.dt.month) | ((ref.dt.month == dob.dt.month) & (ref.dt.day < dob.dt.day))
    return (age - before_bday.astype(int)).astype("Int64")


def ordinal(n):
    if 10 <= n % 100 <= 20:
        suf = "th"
    else:
        suf = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suf}"


# =========================
# OUTPUT LABELS
# =========================
REG_OUTPUT_LABELS = [
    "Registration Date",
    "Saheli Card Number",
    " Full Name:",
    " Date of Birth:",
    "AGE",
    " Address:",
    " Postcode:",
    " Email:",
    " Mobile/Home No:",
    " Emergency Contact Name:",
    " Emergency No:",
    " Emergency Relation To You:",
    " Gender:",
    " Is your gender the same as assigned at birth?",
    " Health Conditions/Disability:",
    " Ethnicity:",
    " Preferred spoken language:",
    " Religion:",
    " Relationship status:",
    " Caring responsibilities:",
    " Living alone:",
    " Sexuality:",
    " Occupation:",
    "Referral reason",
    " How heard about Saheli Hub?",
    "GP Surgery Name:",
    " Consent to store information:",
    " Health declaration:",
    " Permission to be added to Saheli WhatsApp group?",
    " Permission to be in photos and videos? (Media consent)",
    "Notes:",
    " Staff Member:",
    "Site:",
]

HEALTH_FIELDS = [
    " Weight (KG):",
    " Height (CM):",
    " BMI:",
    " BMI Results:",
    " Waist (CM):",
    " Hip (CM):",
    " Waist to Hip Ratio (CM):",
    " Body Fat Percentage Result:",
    " Body Fat Percentage Score:",
    " Visceral Fat Level Result:",
    " Visceral Fat Level Score:",
    " Skeletal Muscle Percentage:",
    " Skeletal Muscle Score:",
    " Resting Metabolism:",
    " Do You Have Any Health Condition?",
    "When did you last measure your blood pressure?",
    "Have you recorded your blood pressure measurement and registered it with a GP or Pharmacist? (Yes/No/Not sure)",
    "What is a healthy blood pressure for an adult?",
    "Why is a high blood pressure dangerous?",
    "How can you help reduce your blood pressure?",
    " Blood Pressure (Systolic/Diastolic):",
    " Blood Pressure Level:",
    " Do You Have a Heart Condition?",
    " Heart Rate (BPM):",
    " Atrial Fibrillation Result:",
    " Heart Age:",
    " Did Your Doctor Advise You Not to Exercise?",
    " Do You Feel Pain in Chest at Rest/During Activity?",
    " Do You Have Shortness of Breath?",
    " Do You Have Diabetes?",
    " Diabetes Risk:",
    " Glucose Level ( mg/dL):",
    " HbA1c:",
    " Do You Take Sugary Drinks, Including Chai?",
    " Do You Have High Cholesterol? (Total/HDL)",
    " Do You Experience The Following Health Issues?",
    " Do You Have a Bone / joint Condition?",
    " Do You Take Any Prescribed Medication?",
    " Referred to doctor for any concerning results?",
    " Risk Stratification Score",
    " Comments:",
    " How well do you manage your health/condition(s)? (Rating out of 10)",
    " In the past week, on how many days have you done a total of 30 mins or more of physical activity, which was enough to raise your breathing rate?",
    " Physical Activity Level:",
    "Comments:PA",
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
    "WEMBS",
    "Comments:2",
    " Nourishment: Rate the quality of the food you put into your body on a daily basis",
    " Movement: Rate how often and for how long you move your body on a daily basis",
    " Connectedness: Rate how well you stay connected with family, friends and your higher power",
    " Sleep: Rate the quality of your sleep",
    " Happy self: Rate how often and for how long you perform positive practices (gratitude, virtue awareness, meditation, prayer, etc.)",
    " Resilience: Rate how well you are able to manage stress in your life",
    " Green and Blue: Rate how often and how long you spend in nature or outdoors",
    " Screen time: Rate how happy you are with your current amount of screen time",
    " Substance use: Rate how comfortable you are with any current substance use (smoking, alcohol, drugs)",
    " Purpose: Rate how well you feel you are fulfilling your passion, purpose or vocation in life",
    "Comments:3",
    " How often do you feel that you lack companionship?",
    "How often do you feel left out?",
    " How often do you feel isolated from others?",
    "SOCIAL ISOLATION",
    "Comments:4",
    " How confident are you to join activities?",
    " How many hobbies and passions do you have?",
    " How involved you feel in your community?",
    " How much you know about local support/services?",
    "What are your aims & goals?",
    "Comments:5",
    " What reasons stop you from joining activities?",
    "Comments:6",
    "What are your preferred activities?",
    "Comments:7",
    " Date of next review appointment:",
]


# =========================
# MAIN
# =========================
def main():
    print(f"Reading registration file: {REG_FILE}")
    df_reg = read_excel_flexible(REG_FILE, REG_SHEET_NAME)

    print(f"Reading health file: {HEALTH_FILE}")
    df_health = read_excel_flexible(HEALTH_FILE, HEALTH_SHEET_NAME)

    reg_map = build_normalized_col_map(df_reg)
    health_map = build_normalized_col_map(df_health)

    # ---------- Key columns ----------
    reg_saheli_col = pick_col(reg_map, "Saheli Card No", "SaheliCardNo", "Saheli Card Number")
    health_saheli_col = pick_col(health_map, "Saheli Card No", "SaheliCardNo", "Saheli Card Number")
    completion_col = pick_col(health_map, "Completion time")

    if not reg_saheli_col:
        raise KeyError("Registration Saheli Card No column not found.")
    if not health_saheli_col:
        raise KeyError("Health Saheli Card No column not found.")
    if not completion_col:
        raise KeyError("Health Completion time column not found.")

    # ---------- Clean and standardize merge keys ----------
    df_reg["_SaheliKey"] = safe_saheli_key_series(df_reg[reg_saheli_col])
    df_health["_SaheliKey"] = safe_saheli_key_series(df_health[health_saheli_col])

    # Completion date in health
    df_health["_CompletionDate"] = parse_date(df_health[completion_col])

    # Debug check (important)
    print("\n[DEBUG] Registration Saheli sample:", df_reg["_SaheliKey"].dropna().head(10).tolist())
    print("[DEBUG] Health Saheli sample:", df_health["_SaheliKey"].dropna().head(10).tolist())

    # ---------- AssessmentNumber ----------
    assess_col = pick_col(health_map, "AssessmentNumber")
    if not assess_col:
        print("AssessmentNumber not found. Creating it...")
        tmp = df_health.copy()
        tmp["_CompletionSort"] = pd.to_datetime(tmp["_CompletionDate"], errors="coerce")
        tmp["_OriginalOrder"] = range(len(tmp))
        tmp = tmp.sort_values(["_SaheliKey", "_CompletionSort", "_OriginalOrder"], kind="mergesort").copy()
        tmp["AssessmentNumber"] = (tmp.groupby("_SaheliKey", dropna=False).cumcount() + 1).astype("Int64")
        tmp.loc[tmp["_SaheliKey"].isna(), "AssessmentNumber"] = pd.NA
        df_health = tmp.drop(columns=["_CompletionSort", "_OriginalOrder"])
        assess_col = "AssessmentNumber"
        health_map = build_normalized_col_map(df_health)
    else:
        df_health[assess_col] = pd.to_numeric(df_health[assess_col], errors="coerce").astype("Int64")

    # ---------- Derived totals ----------
    wem_items = [
        pick_col(health_map, "I’ve been feeling optimistic about the future"),
        pick_col(health_map, "I’ve been feeling useful"),
        pick_col(health_map, "I’ve been feeling relaxed"),
        pick_col(health_map, "I’ve been feeling interested in other people"),
        pick_col(health_map, "I’ve had energy to spare"),
        pick_col(health_map, "I’ve been dealing with problems well"),
        pick_col(health_map, "I’ve been thinking clearly"),
        pick_col(health_map, "I’ve been feeling good about myself"),
        pick_col(health_map, "I’ve been feeling close to other people"),
        pick_col(health_map, "I’ve been feeling confident"),
        pick_col(health_map, "I’ve been able to make up my own mind about things"),
        pick_col(health_map, "I’ve been feeling loved"),
        pick_col(health_map, "I’ve been interested in new things"),
        pick_col(health_map, "I’ve been feeling cheerful"),
    ]
    wem_items = [c for c in wem_items if c]
    for c in wem_items:
        df_health[c] = pd.to_numeric(df_health[c], errors="coerce")
    df_health["WEMBS"] = df_health[wem_items].sum(axis=1, min_count=1) if wem_items else pd.NA

    social_items = [
        pick_col(health_map, "How often do you feel that you lack companionship?"),
        pick_col(health_map, "How often do you feel left out?"),
        pick_col(health_map, "How often do you feel isolated from others?"),
    ]
    social_items = [c for c in social_items if c]
    for c in social_items:
        df_health[c] = pd.to_numeric(df_health[c], errors="coerce")
    df_health["SOCIAL ISOLATION"] = df_health[social_items].sum(axis=1, min_count=1) if social_items else pd.NA

    # ---------- Registration one-row-per-key ----------
    reg_date_col = pick_col(reg_map, "Date") or pick_col(reg_map, "Completion time") or pick_col(reg_map, "Start time")
    dob_col = pick_col(reg_map, "Date of Birth")
    age_col = pick_col(reg_map, "Age")

    if reg_date_col:
        df_reg["_RegDateParsed"] = parse_date(df_reg[reg_date_col])
        df_reg = df_reg.sort_values(["_SaheliKey", "_RegDateParsed"], kind="mergesort")
    else:
        df_reg["_RegDateParsed"] = pd.NaT

    df_reg_first = df_reg.drop_duplicates(subset=["_SaheliKey"], keep="first").copy()
    reg_first_map = build_normalized_col_map(df_reg_first)

    def reg_pick(*cands, occurrence=1):
        return pick_col(reg_first_map, *cands, occurrence=occurrence)

    reg_out = pd.DataFrame({"_SaheliKey": df_reg_first["_SaheliKey"]})
    reg_out["Registration Date"] = df_reg_first["_RegDateParsed"]
    reg_out["Saheli Card Number"] = df_reg_first["_SaheliKey"]

    reg_out[" Full Name:"] = df_reg_first[reg_pick("Full Name")] if reg_pick("Full Name") else pd.NA
    reg_out[" Date of Birth:"] = parse_date(df_reg_first[dob_col]) if dob_col else pd.NA

    if age_col:
        reg_out["AGE"] = pd.to_numeric(df_reg_first[age_col], errors="coerce").astype("Int64")
        if dob_col:
            miss = reg_out["AGE"].isna()
            calc_age = compute_age(df_reg_first[dob_col], df_reg_first["_RegDateParsed"])
            reg_out.loc[miss, "AGE"] = calc_age[miss]
    else:
        reg_out["AGE"] = compute_age(df_reg_first[dob_col], df_reg_first["_RegDateParsed"]) if dob_col else pd.NA

    reg_assignments = [
        (" Address:", reg_pick("Address")),
        (" Postcode:", reg_pick("Postcode")),
        (" Email:", reg_pick("Email", occurrence=2) or reg_pick("Email", occurrence=1)),
        (" Mobile/Home No:", reg_pick("Mobile/Home No")),
        (" Emergency Contact Name:", reg_pick("Emergency Contact Name")),
        (" Emergency No:", reg_pick("Emergency No")),
        (" Emergency Relation To You:", reg_pick("Emergency Relation To You")),
        (" Gender:", reg_pick("Gender")),
        (" Is your gender the same as assigned at birth?", reg_pick("Is your gender the same as assigned at birth")),
        (" Health Conditions/Disability:", reg_pick("Health Conditions/Disability")),
        (" Ethnicity:", reg_pick("Ethnicity")),
        (" Preferred spoken language:", reg_pick("Preferred spoken language")),
        (" Religion:", reg_pick("Religion")),
        (" Relationship status:", reg_pick("Relationship status")),
        (" Caring responsibilities:", reg_pick("Caring responsibilities")),
        (" Living alone:", reg_pick("Living alone")),
        (" Sexuality:", reg_pick("Sexuality")),
        (" Occupation:", reg_pick("Occupation")),
        ("Referral reason", reg_pick("Referral reason")),
        (" How heard about Saheli Hub?", reg_pick("How heard about Saheli Hub")),
        ("GP Surgery Name:", reg_pick("GP Surgery Name")),
        (" Consent to store information:", reg_pick("Consent to store information")),
        (" Health declaration:", reg_pick("Health declaration")),
        (" Permission to be added to Saheli WhatsApp group?", reg_pick("Permission to be added to Saheli WhatsApp group")),
        (" Permission to be in photos and videos? (Media consent)", reg_pick("Permission to be in photos and videos", "Media consent")),
        ("Notes:", reg_pick("Notes")),
        (" Staff Member:", reg_pick("Staff Member")),
        ("Site:", reg_pick("Site")),
    ]

    for out_label, src_col in reg_assignments:
        reg_out[out_label] = df_reg_first[src_col] if src_col else pd.NA

    # ---------- Health prep for wide ----------
    df_health = df_health[df_health[assess_col].notna()].copy()
    df_health[assess_col] = pd.to_numeric(df_health[assess_col], errors="coerce").astype("Int64")
    df_health = df_health[df_health[assess_col].notna()].copy()
    df_health[assess_col] = df_health[assess_col].astype(int)
    df_health = df_health[df_health[assess_col] <= MAX_ASSESSMENTS].copy()

    assess_counts = (
        df_health.dropna(subset=["_SaheliKey"])
        .groupby("_SaheliKey")[assess_col]
        .max()
        .rename("No of assessment completed")
        .astype("Int64")
    )

    def health_source_col(label):
        if label == " Comments:":
            return pick_col(health_map, "Comments", occurrence=1)
        if label == "Comments:PA":
            return pick_col(health_map, "Comments", occurrence=2) or pick_col(health_map, "Comments_2")
        if label == "Comments:2":
            return pick_col(health_map, "Comments2")
        if label == "Comments:3":
            return pick_col(health_map, "Comments3")
        if label == "Comments:4":
            return pick_col(health_map, "Comments4")
        if label == "Comments:5":
            return pick_col(health_map, "Comments5")
        if label == "Comments:6":
            return pick_col(health_map, "Comments6")
        if label == "Comments:7":
            return pick_col(health_map, "Comments7")
        if label == "WEMBS":
            return "WEMBS"
        if label == "SOCIAL ISOLATION":
            return "SOCIAL ISOLATION"

        return (
            pick_col(health_map, label)
            or pick_col(health_map, label.replace(":", ""))
            or pick_col(health_map, label.replace("?", ""))
        )

    # Include keys from both files
    all_keys = sorted(
        set(reg_out["_SaheliKey"].dropna().astype(str)).union(set(df_health["_SaheliKey"].dropna().astype(str)))
    )
    health_wide = pd.DataFrame({"_SaheliKey": pd.Series(all_keys, dtype="string")})

    # Build blocks
    for n in range(1, MAX_ASSESSMENTS + 1):
        block = f"{ordinal(n)} Assessment"
        h_n = df_health[df_health[assess_col] == n].copy()
        if h_n.empty:
            continue

        tmp = pd.DataFrame({"_SaheliKey": h_n["_SaheliKey"].astype("string").values})

        # Put assessment date into the block title column
        tmp[block] = h_n["_CompletionDate"].values

        for fld in HEALTH_FIELDS:
            src = health_source_col(fld)
            if src and src in h_n.columns:
                vals = h_n[src]
                if normalize_header(fld) == normalize_header("Date of next review appointment"):
                    vals = parse_date(vals)
                tmp[fld] = vals.values
            else:
                tmp[fld] = pd.NA

        # Rename to wide names
        rename_map = {}
        for c in tmp.columns:
            if c == "_SaheliKey":
                continue
            if c == block:
                rename_map[c] = c
            else:
                rename_map[c] = f"{block} {c}"
        tmp = tmp.rename(columns=rename_map)

        tmp = tmp.drop_duplicates(subset=["_SaheliKey"], keep="first")
        health_wide = health_wide.merge(tmp, on="_SaheliKey", how="left")

    # ---------- Final merge ----------
    final_df = reg_out.merge(health_wide, on="_SaheliKey", how="outer")
    final_df = final_df.merge(assess_counts.reset_index(), on="_SaheliKey", how="left")

    final_df["Saheli Card Number"] = final_df.get("Saheli Card Number", pd.Series(dtype="string"))
    final_df["Saheli Card Number"] = final_df["Saheli Card Number"].fillna(final_df["_SaheliKey"])

    # Put first columns in correct order
    for c in ["No of assessment completed"] + REG_OUTPUT_LABELS:
        if c not in final_df.columns:
            final_df[c] = pd.NA

    first_cols = ["No of assessment completed"] + REG_OUTPUT_LABELS
    remaining_cols = [c for c in final_df.columns if c not in first_cols + ["_SaheliKey"]]

    def assessment_sort_key(c):
        m = re.match(r"^(\d+)(st|nd|rd|th)\sAssessment(.*)$", str(c))
        if m:
            return (0, int(m.group(1)), m.group(3))
        return (1, 999, str(c))

    remaining_cols = sorted(remaining_cols, key=assessment_sort_key)
    final_df = final_df[first_cols + remaining_cols].copy()

    # Sort rows by numeric Saheli value
    final_df["_sort_num"] = pd.to_numeric(final_df["Saheli Card Number"], errors="coerce")
    final_df = final_df.sort_values(["_sort_num", "Saheli Card Number"], kind="mergesort").drop(columns=["_sort_num"])

    # Write file
    out = write_excel(final_df, OUTPUT_FILE)
    print(f"\n✅ Final wide file created: {out}")
    print(f"Rows: {len(final_df)} | Columns: {len(final_df.columns)}")

    # Preview
    preview_cols = [
        c for c in [
            "No of assessment completed",
            "Saheli Card Number",
            " Full Name:",
            "Registration Date",
            "1st Assessment",
            "1st Assessment  Weight (KG):",
            "2nd Assessment",
            "2nd Assessment  Weight (KG):",
        ] if c in final_df.columns
    ]
    if preview_cols:
        print("\nPreview:")
        print(final_df[preview_cols].head(12).to_string(index=False))


if __name__ == "__main__":
    main()
