# ============================================================
# Q FULL FILE 1: saheli_prepare_files.py
# ------------------------------------------------------------
# Prepares two separate Excel files:
#   1) Registrations.xlsx  -> Registrations_Cleaned.xlsx
#   2) Healthassessments.xlsx -> Healthassessments_Prepared.xlsx
#
# Registration file:
#   - Finds "Saheli Card No:" column (flexible header match)
#   - Keeps numbers only (FIXED: no 1 -> 10 bug)
#
# Healthassessment file:
#   - Finds "Completion time" and "Saheli Card No:" (flexible)
#   - Cleans Saheli Card No (FIXED)
#   - Converts Completion time to date
#   - Moves Saheli column next to Completion time
#   - Sorts by Saheli, Completion time
#   - Creates AssessmentNumber (1,2,3 per Saheli)
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
REG_FILE = r"C:\Users\shonk\Downloads\Main Registration Form(1-1143).xlsx"
HEALTH_FILE = r"C:\Users\shonk\Downloads\Saheli Hub Health Assessment(1-1477).xlsx"

REG_OUTPUT_FILE = r"C:\Users\shonk\source\PythonCodes\New folder\Registrations_Cleaned.xlsx"
HEALTH_OUTPUT_FILE = r"C:\Users\shonk\source\PythonCodes\New folder\Healthassessments_Prepared.xlsx"

# Set to sheet name string if needed, otherwise None uses first sheet
REG_SHEET_NAME = None
HEALTH_SHEET_NAME = None


# =========================
# HELPERS
# =========================
def normalize_header(h) -> str:
    """Normalize headers to match flexibly (spaces, colons, line breaks ignored)."""
    if h is None:
        return ""
    s = str(h)
    s = s.replace("\r", "").replace("\n", "")
    s = s.strip().lower()
    s = s.replace(" ", "").replace(":", "")
    return s


def keep_digits_only(v):
    """
    Keep only digits, but safely handle numeric Excel values.
    FIXES: 1.0 -> '1' (instead of wrong '10')
    """
    if pd.isna(v):
        return pd.NA

    # Numeric values first (Excel often reads whole numbers as floats like 14.0)
    if isinstance(v, (int, float)):
        try:
            fv = float(v)
            if fv.is_integer():
                return str(int(fv))
            # Rare non-integer case: strip non-digits from fixed-point string
            s_num = format(fv, "f")
            digits = re.sub(r"\D+", "", s_num)
            return digits if digits else pd.NA
        except Exception:
            pass

    s = str(v).strip()

    # Handle string "14.0", "27.000"
    if re.fullmatch(r"\d+\.0+", s):
        return s.split(".")[0]

    digits = re.sub(r"\D+", "", s)
    return digits if digits else pd.NA


def find_col_by_normalized(df: pd.DataFrame, target_normalized: str, required=True):
    for col in df.columns:
        if normalize_header(col) == target_normalized:
            return col
    if required:
        raise KeyError(
            f"Column not found for normalized='{target_normalized}'. "
            f"Available columns: {list(df.columns)}"
        )
    return None


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


# =========================
# 1) REGISTRATION PREP
# =========================
def prepare_registration_file(reg_file: str, reg_output_file: str, sheet_name=None):
    print(f"\n[Registration] Reading: {reg_file}")
    df = read_excel_flexible(reg_file, sheet_name=sheet_name)

    col_saheli = find_col_by_normalized(df, "sahelicardno", required=True)
    print(f"[Registration] Saheli column found: {col_saheli}")

    # Clean and force to integer values (nullable)
    df[col_saheli] = df[col_saheli].apply(keep_digits_only)
    df[col_saheli] = pd.to_numeric(df[col_saheli], errors="coerce").astype("Int64")

    out = write_excel(df, reg_output_file)
    print(f"[Registration] ✅ Saved: {out}")

    # Quick preview
    print("[Registration] Preview Saheli values:")
    print(df[[col_saheli]].head(10).to_string(index=False))

    return df


# =========================
# 2) HEALTHASSESSMENT PREP
# =========================
def prepare_healthassessment_file(health_file: str, health_output_file: str, sheet_name=None):
    print(f"\n[Health] Reading: {health_file}")
    df = read_excel_flexible(health_file, sheet_name=sheet_name)

    col_completion = find_col_by_normalized(df, "completiontime", required=True)
    col_saheli = find_col_by_normalized(df, "sahelicardno", required=True)

    print(f"[Health] Completion column found: {col_completion}")
    print(f"[Health] Saheli column found: {col_saheli}")

    # 1) Clean Saheli
    df[col_saheli] = df[col_saheli].apply(keep_digits_only)
    df[col_saheli] = pd.to_numeric(df[col_saheli], errors="coerce").astype("Int64")

    # 2) Convert Completion time to date
    completion_dt = pd.to_datetime(df[col_completion], errors="coerce", dayfirst=True)
    df[col_completion] = completion_dt.dt.date

    # 3) Move Saheli next to Completion time
    cols = list(df.columns)
    cols.remove(col_saheli)
    completion_idx = cols.index(col_completion)
    cols.insert(completion_idx + 1, col_saheli)
    df = df[cols].copy()

    # Re-find after move
    col_completion = find_col_by_normalized(df, "completiontime", required=True)
    col_saheli = find_col_by_normalized(df, "sahelicardno", required=True)

    # 4) Sort by Saheli, Completion
    df = df.sort_values(
        by=[col_saheli, col_completion],
        ascending=[True, True],
        na_position="last",
        kind="mergesort"
    ).reset_index(drop=True)

    # 5) Create AssessmentNumber
    assessment_num = df.groupby(col_saheli, dropna=False).cumcount() + 1
    assessment_num = assessment_num.where(df[col_saheli].notna(), pd.NA).astype("Int64")

    saheli_pos = df.columns.get_loc(col_saheli)
    df.insert(saheli_pos + 1, "AssessmentNumber", assessment_num)

    out = write_excel(df, health_output_file)
    print(f"[Health] ✅ Saved: {out}")

    # Quick preview
    preview_cols = [col_completion, col_saheli, "AssessmentNumber"]
    print("[Health] Preview:")
    print(df[preview_cols].head(20).to_string(index=False))

    return df


# =========================
# MAIN
# =========================
def main():
    prepare_registration_file(
        reg_file=REG_FILE,
        reg_output_file=REG_OUTPUT_FILE,
        sheet_name=REG_SHEET_NAME
    )

    prepare_healthassessment_file(
        health_file=HEALTH_FILE,
        health_output_file=HEALTH_OUTPUT_FILE,
        sheet_name=HEALTH_SHEET_NAME
    )

    print("\n🎉 Prep done")
    print(f"Registration output: {REG_OUTPUT_FILE}")
    print(f"Health output:       {HEALTH_OUTPUT_FILE}")


if __name__ == "__main__":
    main()
