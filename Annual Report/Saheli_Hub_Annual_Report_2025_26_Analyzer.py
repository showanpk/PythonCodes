#!/usr/bin/env python3
"""
Saheli Hub Annual Report 2025/26 Analyzer
-----------------------------------------
READ ONLY: this script only SELECTs data from SQL Server.

What it produces:
- One Excel workbook with:
  * Headline Metrics
  * Annual Attendee Summary
  * Location Summary
  * Monthly Summary
  * Top Activities
  * Gender - Annual Attendees
  * Age - Annual Attendees
  * Ethnicity Raw
  * Ethnicity Clean
  * Join Reasons
  * Heard About
  * CRM Membership
  * Source Audit
  * Attendance by Financial Year
  * Raw Annual Attendance (optional)
  * Canonical Participants (optional)

Important:
- It DOES NOT turn missing gender into Female.
- It DOES NOT inflate annual attendance to 21k+.
- It separately reports:
    annual confirmed attendance,
    current FULL + Lite CRM profile count,
    raw attendee identities,
    canonical deduplicated annual people.
- It audits possible extra source tables so you can see whether any
  delivery exists outside SessionAttendance.

Dependencies:
    pip install pyodbc pandas openpyxl

Use ODBC Driver 18 for SQL Server where possible.
"""

from __future__ import annotations

import os
import re
import sys
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import pyodbc


# ============================================================
# CONFIGURATION - EDIT THESE
# ============================================================

# OPTION 1 (recommended): set an environment variable:
#   SAHELI_SQL_CONNECTION_STRING
#
# OPTION 2: paste your connection string below.
CONNECTION_STRING = os.getenv(
    "SAHELI_SQL_CONNECTION_STRING",
    r"Driver={ODBC Driver 18 for SQL Server};"
    r"Server=tcp:sahelihub.database.windows.net,1433;"
    r"Database=SaheliHubCRM;"
    r"Uid=sahelihubadmin;"
    r"Pwd=W7WZ7ZaG1YbMZ71gh%2xSFuR;"
    r"Encrypt=yes;"
    r"TrustServerCertificate=no;"
    r"Connection Timeout=30;"
)

REPORT_START = "2025-04-01"
REPORT_END_EXCLUSIVE = "2026-04-01"
AGE_AT_DATE = pd.Timestamp("2026-03-31")

OUTPUT_XLSX = Path("Saheli_Hub_Annual_Report_2025_26_COMPLETE.xlsx")
BRANDED_OUTPUT_XLSX = Path(
    "Saheli_Hub_Annual_Report_2025_26_COMPLETE_BRANDED.xlsx"
)
PROJECT_ROOT = Path(__file__).resolve().parent.parent

LAST_YEAR = {
    "Total confirmed attendance": 21777,
    "Canonical unique people": 1897,
    "New FULL registrations during 2025/26": 598,
    "Female % among recorded Female/Male": 0.85,
    "Male % among recorded Female/Male": 0.15,
    "Ethnically diverse percentage": 0.95,
    "IMD percentage": 0.77,
}

# Set False if you do not want names/phones/DOB/postcodes in the workbook.
EXPORT_RAW_SENSITIVE_DATA = True

# Optional: if you later have a postcode-level IMD lookup CSV, set its path.
# Expected columns can vary, so the script does not require it.
IMD_POSTCODE_CSV = None


# ============================================================
# HELPERS
# ============================================================

def fail_if_placeholder_connection_string() -> None:
    markers = ("YOUR_SERVER", "YOUR_USERNAME", "YOUR_PASSWORD")
    if any(x in CONNECTION_STRING for x in markers):
        raise SystemExit(
            "\nPlease edit CONNECTION_STRING at the top of the script "
            "or set SAHELI_SQL_CONNECTION_STRING first.\n"
        )


def sql_df(conn: pyodbc.Connection, query: str, params=None) -> pd.DataFrame:
    return pd.read_sql_query(query, conn, params=params)


def table_exists(conn: pyodbc.Connection, table_name: str) -> bool:
    q = """
    SELECT CASE WHEN OBJECT_ID(?, 'U') IS NOT NULL THEN 1 ELSE 0 END AS ExistsFlag
    """
    return bool(sql_df(conn, q, [f"dbo.{table_name}"]).iloc[0, 0])


def normalize_name(v) -> str:
    if pd.isna(v):
        return ""
    s = str(v).strip().lower()
    # Match the SQL audit logic used previously:
    s = s.replace(" ", "").replace("-", "").replace("'", "")
    return s


def normalize_postcode(v) -> str:
    if pd.isna(v):
        return ""
    return str(v).strip().upper().replace(" ", "")


def normalize_phone(v) -> str:
    if pd.isna(v):
        return ""
    s = str(v).strip()
    for ch in (" ", "-", "(", ")", "+", "."):
        s = s.replace(ch, "")
    return s


def clean_gender(v):
    if pd.isna(v):
        return None
    s = str(v).strip().lower()
    if not s:
        return None
    if s in {"female", "f", "woman", "women"}:
        return "Female"
    if s in {"male", "m", "man", "men"}:
        return "Male"
    return "Other"


def age_at(dob, at_date: pd.Timestamp):
    if pd.isna(dob):
        return None
    dob = pd.Timestamp(dob)
    if dob > at_date or dob < pd.Timestamp("1900-01-01"):
        return None
    return (
        at_date.year
        - dob.year
        - ((at_date.month, at_date.day) < (dob.month, dob.day))
    )


def age_band(age):
    if age is None or pd.isna(age):
        return "Unknown"
    age = int(age)
    if age < 16:
        return "Under 16"
    if age <= 25:
        return "16-25"
    if age <= 35:
        return "26-35"
    if age <= 45:
        return "36-45"
    if age <= 55:
        return "46-55"
    if age <= 65:
        return "56-65"
    if age <= 75:
        return "66-75"
    return "76+"


def clean_ethnicity(v):
    if pd.isna(v) or not str(v).strip():
        return "Not recorded"

    s = str(v).strip().lower()

    asian = {
        "asian - pakistani", "pakistani", "british pakistani",
        "asian - indian", "indian", "asian - bangladeshi",
        "asian - other asian background", "asian - chinese",
        "kashmiri", "guiyanese indian", "indonesian",
    }
    black = {
        "black - caribbean", "black carribean",
        "black - african", "black - somalian",
        "black - other black background", "black british",
        "african", "east african", "eirtria",
        "somalian", "somanian",
    }
    arab = {"other ethnicity - arab", "arab"}
    white = {"white - british", "white - irish", "white uk"}
    mixed = {
        "mixed - other mixed background",
        "mixed - white and asian",
        "mixed - white and black african",
        "mixed - white and black caribbean",
        "mixed background",
    }
    other = {
        "iranian", "other ethnicity - iranian",
        "kurdish", "south american", "other",
    }

    if s in asian:
        return "Asian / Asian British"
    if s in black:
        return "Black / Black British"
    if s in arab:
        return "Arab"
    if s in white:
        return "White"
    if s in mixed:
        return "Mixed"
    if s in other:
        return "Other ethnic background"
    return "Unclear / review"


def clean_heard_about(v):
    if pd.isna(v) or not str(v).strip():
        return "Not recorded"
    s = str(v).strip().lower()

    if s in {"word of mouth", "word-of-mouth", "wordofmouth"}:
        return "Word of mouth"
    if s == "gp" or s.startswith("gp "):
        return "GP"
    if s in {
        "workwell", "workwell programme", "work well",
        "work well programme"
    }:
        return "WorkWell"
    if s == "social media":
        return "Social media"
    if s == "website":
        return "Website"
    return "Other"


def clean_referral_reason(v):
    if pd.isna(v):
        return None
    s = " ".join(str(v).strip().lower().split())
    mapping = {
        "increase exercise or mobility": "Increase exercise or mobility",
        "weight management": "Weight management",
        "long term health condition": "Long-term health condition",
        "long-term health condition": "Long-term health condition",
        "healthy eating or nutrition": "Healthy eating or nutrition",
        "mild to moderate depression or anxiety":
            "Mild/moderate depression or anxiety",
        "mild/moderate depression or anxiety":
            "Mild/moderate depression or anxiety",
        "isolation or loneliness": "Isolation or loneliness",
        "learning, training or employment":
            "Learning/training/employment",
        "learning/training/employment":
            "Learning/training/employment",
        "challenging social circumstances":
            "Challenging social circumstances",
    }
    return mapping.get(s, str(v).strip())


def financial_year_label(ts):
    ts = pd.Timestamp(ts)
    if ts.month >= 4:
        return f"{ts.year}/{str(ts.year + 1)[-2:]}"
    return f"{ts.year - 1}/{str(ts.year)[-2:]}"


# ============================================================
# LOAD CORE DATA
# ============================================================

def load_data(conn):
    sessions = sql_df(
        conn,
        """
        SELECT
            SessionId, SessionDate, VenueName, ActivityName,
            StartTime, EndTime, IsCancelled
        FROM dbo.Sessions
        WHERE SessionDate >= ?
          AND SessionDate < ?
        """,
        [REPORT_START, REPORT_END_EXCLUSIVE],
    )

    attendance = sql_df(
        conn,
        """
        SELECT
            AttendanceId, SessionId, ParticipantId, LiteMemberId,
            SaheliCardNumber, MemberDisplayId, MemberName, Phone,
            Attended, CreatedAtUtc
        FROM dbo.SessionAttendance
        WHERE Attended = 1
          AND SessionId IN
          (
              SELECT SessionId
              FROM dbo.Sessions
              WHERE SessionDate >= ?
                AND SessionDate < ?
          )
        """,
        [REPORT_START, REPORT_END_EXCLUSIVE],
    )

    participants = sql_df(
        conn,
        """
        SELECT
            ParticipantID, SaheliCardNumber, FullName, MobileNumber,
            Gender, DateOfBirth, Ethnicity, Postcode,
            RegistrationDate, ReferralReason, HeardAboutSaheli
        FROM dbo.Participants
        """,
    )

    lite = sql_df(
        conn,
        """
        SELECT
            Id, MembershipId, FirstName, LastName, Phone,
            Gender, DateOfBirth, Ethnicity, Postcode, CreatedAtUtc
        FROM dbo.LiteMembers
        """,
    )

    return sessions, attendance, participants, lite


# ============================================================
# CANONICAL FULL/LITE DEDUP
# ============================================================

def build_canonical_data(sessions, attendance, participants, lite):
    sessions = sessions.copy()
    attendance = attendance.copy()
    participants = participants.copy()
    lite = lite.copy()

    sessions["SessionDate"] = pd.to_datetime(sessions["SessionDate"])
    annual = attendance.merge(
        sessions[[
            "SessionId", "SessionDate", "VenueName", "ActivityName",
            "StartTime", "EndTime",
        ]],
        on="SessionId",
        how="left",
        validate="many_to_one",
    )

    annual["raw_identity_key"] = annual.apply(
        lambda r:
            f"FULL:{int(r.ParticipantId)}"
            if pd.notna(r.ParticipantId)
            else f"LITE:{r.LiteMemberId}",
        axis=1,
    )

    annual_full_ids = set(
        int(x) for x in annual["ParticipantId"].dropna().unique()
    )
    annual_lite_ids = set(
        str(x) for x in annual["LiteMemberId"].dropna().unique()
    )

    # Full normalisation
    participants["ParticipantID"] = participants["ParticipantID"].astype(int)
    participants["name_key"] = participants["FullName"].map(normalize_name)
    participants["postcode_key"] = participants["Postcode"].map(normalize_postcode)
    participants["phone_key"] = participants["MobileNumber"].map(normalize_phone)
    participants["card_key"] = (
        participants["SaheliCardNumber"].astype(str).str.strip()
    )

    full_by_id = participants.set_index("ParticipantID", drop=False)

    phone_to_full = defaultdict(set)
    namepost_to_full = defaultdict(set)
    card_to_full = defaultdict(set)
    name_to_full = defaultdict(set)

    for _, r in participants.iterrows():
        pid = int(r["ParticipantID"])
        if r["phone_key"] and len(r["phone_key"]) >= 10:
            phone_to_full[r["phone_key"]].add(pid)
        if r["name_key"] and r["postcode_key"]:
            namepost_to_full[
                (r["name_key"], r["postcode_key"])
            ].add(pid)
        if r["card_key"] and r["card_key"].lower() != "nan":
            card_to_full[r["card_key"]].add(pid)
        if r["name_key"]:
            name_to_full[r["name_key"]].add(pid)

    # Current Lite profiles used by annual identities
    lite["Id_str"] = lite["Id"].astype(str)
    lite["FullName"] = (
        lite["FirstName"].fillna("").astype(str).str.strip()
        + " "
        + lite["LastName"].fillna("").astype(str).str.strip()
    ).str.strip()
    lite["name_key"] = lite["FullName"].map(normalize_name)
    lite["postcode_key"] = lite["Postcode"].map(normalize_postcode)
    lite["phone_key"] = lite["Phone"].map(normalize_phone)

    lite_by_id = lite.set_index("Id_str", drop=False)

    annual_lite = annual[annual["LiteMemberId"].notna()].copy()
    annual_lite["LiteMemberId_str"] = annual_lite["LiteMemberId"].astype(str)

    # all historical names / phones by Lite id
    hist_names = defaultdict(set)
    hist_phones = defaultdict(set)

    for lid, group in annual_lite.groupby("LiteMemberId_str"):
        for x in group["MemberName"].dropna():
            s = str(x).strip()
            if s:
                hist_names[lid].add(s)
        for x in group["Phone"].dropna():
            s = normalize_phone(x)
            if s:
                hist_phones[lid].add(s)

    # Add current Lite values into evidence pools
    for lid in annual_lite_ids:
        if lid in lite_by_id.index:
            lr = lite_by_id.loc[lid]
            if isinstance(lr, pd.DataFrame):
                lr = lr.iloc[0]
            if lr["FullName"]:
                hist_names[lid].add(lr["FullName"])
            if lr["phone_key"]:
                hist_phones[lid].add(lr["phone_key"])

    evidence_candidates = defaultdict(lambda: defaultdict(set))

    for lid in annual_lite_ids:
        # A. historical "Card ####"
        for raw_name in hist_names.get(lid, set()):
            m = re.match(r"^\s*card\s+(\d+)\s*$", raw_name, re.I)
            if m:
                for pid in card_to_full.get(m.group(1), set()):
                    evidence_candidates[lid]["Historical Saheli Card"].add(pid)

        # B. exact phone
        for ph in hist_phones.get(lid, set()):
            if len(ph) >= 10:
                for pid in phone_to_full.get(ph, set()):
                    evidence_candidates[lid]["Exact phone"].add(pid)

        # C. exact current name + postcode
        if lid in lite_by_id.index:
            lr = lite_by_id.loc[lid]
            if isinstance(lr, pd.DataFrame):
                lr = lr.iloc[0]
            if lr["name_key"] and lr["postcode_key"]:
                for pid in namepost_to_full.get(
                    (lr["name_key"], lr["postcode_key"]), set()
                ):
                    evidence_candidates[lid][
                        "Exact name + postcode"
                    ].add(pid)

    resolved_lite_to_full = {}
    conflicting_lite = {}

    for lid, typed in evidence_candidates.items():
        candidates = set()
        for ids in typed.values():
            candidates.update(ids)

        if len(candidates) == 1:
            resolved_lite_to_full[lid] = next(iter(candidates))
        elif len(candidates) > 1:
            conflicting_lite[lid] = sorted(candidates)

    # Canonical person key on each annual attendance row
    def canonical_key(row):
        if pd.notna(row["ParticipantId"]):
            return f"FULL:{int(row['ParticipantId'])}"
        lid = str(row["LiteMemberId"])
        if lid in resolved_lite_to_full:
            return f"FULL:{resolved_lite_to_full[lid]}"
        return f"LITE:{lid}"

    annual["CanonicalPersonKey"] = annual.apply(canonical_key, axis=1)

    # Canonical key -> associated lite ids
    canonical_lite_ids = defaultdict(set)
    for _, r in annual[annual["LiteMemberId"].notna()].iterrows():
        canonical_lite_ids[r["CanonicalPersonKey"]].add(
            str(r["LiteMemberId"])
        )

    # latest historical Lite name / phone
    latest_hist = {}
    if not annual_lite.empty:
        temp = annual_lite.sort_values(
            ["LiteMemberId_str", "SessionDate", "AttendanceId"]
        )
        for lid, group in temp.groupby("LiteMemberId_str"):
            last = group.iloc[-1]
            latest_hist[lid] = {
                "MemberName": last.get("MemberName"),
                "Phone": last.get("Phone"),
            }

    # Precompute report-only gender evidence
    strong_gender = {}
    for lid, typed in evidence_candidates.items():
        candidates = set()
        for ids in typed.values():
            candidates.update(ids)
        genders = set()
        for pid in candidates:
            if pid in full_by_id.index:
                g = clean_gender(full_by_id.loc[pid, "Gender"])
                if g:
                    genders.add(g)
        if len(genders) == 1:
            strong_gender[lid] = next(iter(genders))

    # Blank FULL profile -> other FULL exact-name gender agreement
    full_name_gender = {}
    for _, fr in participants.iterrows():
        pid = int(fr["ParticipantID"])
        own_gender = clean_gender(fr["Gender"])
        if own_gender or not fr["name_key"]:
            continue
        genders = set()
        for other_pid in name_to_full.get(fr["name_key"], set()):
            if other_pid == pid:
                continue
            g = clean_gender(full_by_id.loc[other_pid, "Gender"])
            if g:
                genders.add(g)
        if len(genders) == 1:
            full_name_gender[pid] = next(iter(genders))

    # Lite/historical exact-name gender agreement
    lite_name_gender = {}
    for lid in annual_lite_ids:
        genders = set()
        for raw_name in hist_names.get(lid, set()):
            # exclude placeholders
            low = raw_name.strip().lower()
            if (
                "unknown" in low
                or "not provided" in low
                or low.startswith("card ")
                or low == "#n/a"
            ):
                continue
            nk = normalize_name(raw_name)
            for pid in name_to_full.get(nk, set()):
                g = clean_gender(full_by_id.loc[pid, "Gender"])
                if g:
                    genders.add(g)
        if len(genders) == 1:
            lite_name_gender[lid] = next(iter(genders))

    # Canonical participant table
    rows = []
    for key, group in annual.groupby("CanonicalPersonKey"):
        full_id = None
        if key.startswith("FULL:"):
            full_id = int(key.split(":", 1)[1])

        associated_lites = sorted(canonical_lite_ids.get(key, set()))
        chosen_lite = associated_lites[0] if associated_lites else None

        full_row = None
        if full_id is not None and full_id in full_by_id.index:
            full_row = full_by_id.loc[full_id]
            if isinstance(full_row, pd.DataFrame):
                full_row = full_row.iloc[0]

        lite_row = None
        if chosen_lite and chosen_lite in lite_by_id.index:
            lite_row = lite_by_id.loc[chosen_lite]
            if isinstance(lite_row, pd.DataFrame):
                lite_row = lite_row.iloc[0]

        hist = latest_hist.get(chosen_lite or "", {})

        def first_nonempty(*vals):
            for v in vals:
                if pd.notna(v) and str(v).strip():
                    return v
            return None

        member_number = first_nonempty(
            full_row["SaheliCardNumber"] if full_row is not None else None,
            lite_row["MembershipId"] if lite_row is not None else None,
        )

        full_name = first_nonempty(
            full_row["FullName"] if full_row is not None else None,
            lite_row["FullName"] if lite_row is not None else None,
            hist.get("MemberName"),
        )

        phone = first_nonempty(
            full_row["MobileNumber"] if full_row is not None else None,
            lite_row["Phone"] if lite_row is not None else None,
            hist.get("Phone"),
        )

        raw_gender = first_nonempty(
            full_row["Gender"] if full_row is not None else None,
            lite_row["Gender"] if lite_row is not None else None,
        )

        report_gender = clean_gender(raw_gender)
        gender_source = None

        if report_gender:
            gender_source = (
                "Own FULL profile"
                if full_row is not None
                and clean_gender(full_row["Gender"])
                else "Own Lite profile"
            )
        else:
            # strong evidence from associated lite ids
            candidates = {
                strong_gender[lid]
                for lid in associated_lites
                if lid in strong_gender
            }
            if len(candidates) == 1:
                report_gender = next(iter(candidates))
                gender_source = "Strong identity evidence"
            elif (
                full_id is not None
                and full_id in full_name_gender
            ):
                report_gender = full_name_gender[full_id]
                gender_source = "Exact FULL name - gender agrees"
            else:
                candidates = {
                    lite_name_gender[lid]
                    for lid in associated_lites
                    if lid in lite_name_gender
                }
                if len(candidates) == 1:
                    report_gender = next(iter(candidates))
                    gender_source = (
                        "Exact Lite/historical name - gender agrees"
                    )

        if not gender_source:
            gender_source = "Not resolved"

        dob = first_nonempty(
            full_row["DateOfBirth"] if full_row is not None else None,
            lite_row["DateOfBirth"] if lite_row is not None else None,
        )
        ethnicity = first_nonempty(
            full_row["Ethnicity"] if full_row is not None else None,
            lite_row["Ethnicity"] if lite_row is not None else None,
        )
        postcode = first_nonempty(
            full_row["Postcode"] if full_row is not None else None,
            lite_row["Postcode"] if lite_row is not None else None,
        )

        age = age_at(dob, AGE_AT_DATE)

        rows.append(
            {
                "CanonicalPersonKey": key,
                "CanonicalMemberType":
                    "FULL" if full_id is not None else "LITE",
                "ParticipantId": full_id,
                "LiteMemberId": chosen_lite,
                "MemberNumber": member_number,
                "FullName": full_name,
                "Phone": phone,
                "RawGender": raw_gender,
                "ReportGender": report_gender
                    if report_gender else "Not recorded",
                "GenderResolutionSource": gender_source,
                "DateOfBirth": dob,
                "AgeAt31Mar2026": age,
                "AgeBand": age_band(age),
                "Ethnicity": ethnicity,
                "EthnicityGroup": clean_ethnicity(ethnicity),
                "Postcode": postcode,
                "FirstAttendance": group["SessionDate"].min(),
                "LastAttendance": group["SessionDate"].max(),
                "AttendanceRecords": len(group),
            }
        )

    canonical_people = pd.DataFrame(rows)

    return (
        annual,
        canonical_people,
        resolved_lite_to_full,
        conflicting_lite,
        evidence_candidates,
    )


# ============================================================
# SUMMARIES
# ============================================================

def make_summaries(
    conn,
    sessions,
    annual,
    participants,
    lite,
    canonical_people,
    resolved_lite_to_full,
    conflicting_lite,
):
    # Headline metrics
    annual_attendance = len(annual)
    annual_sessions = annual["SessionId"].nunique()
    raw_unique = annual["raw_identity_key"].nunique()
    canonical_unique = canonical_people["CanonicalPersonKey"].nunique()

    new_full_mask = (
        pd.to_datetime(participants["RegistrationDate"], errors="coerce")
        .between(
            pd.Timestamp(REPORT_START),
            pd.Timestamp(REPORT_END_EXCLUSIVE),
            inclusive="left",
        )
    )
    new_full = participants[new_full_mask].copy()

    current_full = len(participants)
    current_lite = len(lite)

    headline = pd.DataFrame(
        [
            ["Annual confirmed attendances", annual_attendance],
            ["Annual attended sessions", annual_sessions],
            ["Raw FULL/Lite attendee identities", raw_unique],
            ["Canonical annual people", canonical_unique],
            ["Current FULL CRM profiles", current_full],
            ["Current Lite CRM profiles", current_lite],
            ["Current FULL + Lite CRM profiles",
             current_full + current_lite],
            ["New FULL registrations in 2025/26", len(new_full)],
            ["Strong Lite->FULL mappings",
             len(resolved_lite_to_full)],
            ["Conflicting strong Lite mappings",
             len(conflicting_lite)],
        ],
        columns=["Metric", "Value"],
    )

    # Annual location summary using canonical people
    location = (
        annual.groupby("VenueName", dropna=False)
        .agg(
            Attendances=("AttendanceId", "size"),
            Sessions=("SessionId", "nunique"),
            CanonicalUniquePeople=("CanonicalPersonKey", "nunique"),
        )
        .reset_index()
        .rename(columns={"VenueName": "Location"})
        .sort_values("Attendances", ascending=False)
    )
    location["AveragePerSession"] = (
        location["Attendances"] / location["Sessions"]
    ).round(2)
    location["AttendanceSharePct"] = (
        100 * location["Attendances"] / annual_attendance
    ).round(2)

    # Monthly
    annual["Month"] = annual["SessionDate"].dt.to_period("M").astype(str)
    monthly = (
        annual.groupby(["Month", "VenueName"], dropna=False)
        .agg(
            Attendances=("AttendanceId", "size"),
            Sessions=("SessionId", "nunique"),
            CanonicalUniquePeople=("CanonicalPersonKey", "nunique"),
        )
        .reset_index()
        .rename(columns={"VenueName": "Location"})
        .sort_values(["Month", "Attendances"], ascending=[True, False])
    )

    # Activities
    activities = (
        annual.groupby("ActivityName", dropna=False)
        .agg(
            Attendances=("AttendanceId", "size"),
            Sessions=("SessionId", "nunique"),
            CanonicalUniquePeople=("CanonicalPersonKey", "nunique"),
        )
        .reset_index()
        .rename(columns={"ActivityName": "Activity"})
        .sort_values("Attendances", ascending=False)
    )
    activities["AveragePerSession"] = (
        activities["Attendances"] / activities["Sessions"]
    ).round(2)
    top10 = activities.head(10).copy()

    # Gender - canonical
    gender = (
        canonical_people["ReportGender"]
        .fillna("Not recorded")
        .value_counts(dropna=False)
        .rename_axis("Gender")
        .reset_index(name="Participants")
    )
    gender["PercentageOfCanonicalPeople"] = (
        100 * gender["Participants"] / canonical_unique
    ).round(2)

    binary = canonical_people[
        canonical_people["ReportGender"].isin(["Female", "Male"])
    ]["ReportGender"].value_counts().rename_axis("Gender").reset_index(
        name="Participants"
    )
    binary["PercentageOfFemaleMaleRecorded"] = (
        100 * binary["Participants"] / binary["Participants"].sum()
    ).round(2)

    gender_source = (
        canonical_people["GenderResolutionSource"]
        .value_counts()
        .rename_axis("ResolutionSource")
        .reset_index(name="Participants")
    )

    # Age
    age_order = [
        "Under 16", "16-25", "26-35", "36-45",
        "46-55", "56-65", "66-75", "76+", "Unknown"
    ]
    age_summary = (
        canonical_people["AgeBand"]
        .value_counts()
        .reindex(age_order, fill_value=0)
        .rename_axis("AgeBand")
        .reset_index(name="Participants")
    )
    age_summary["PercentageOfCanonicalPeople"] = (
        100 * age_summary["Participants"] / canonical_unique
    ).round(2)

    valid_age = canonical_people["AgeAt31Mar2026"].notna().sum()
    age_summary["PercentageOfValidAge"] = age_summary.apply(
        lambda r:
            round(100 * r["Participants"] / valid_age, 2)
            if valid_age and r["AgeBand"] != "Unknown"
            else None,
        axis=1,
    )

    # Ethnicity
    eth_raw = (
        canonical_people["Ethnicity"]
        .fillna("Not recorded")
        .replace("", "Not recorded")
        .value_counts()
        .rename_axis("Ethnicity")
        .reset_index(name="Participants")
    )

    eth_clean = (
        canonical_people["EthnicityGroup"]
        .value_counts()
        .rename_axis("EthnicityGroup")
        .reset_index(name="Participants")
    )
    eth_clean["PercentageOfCanonicalPeople"] = (
        100 * eth_clean["Participants"] / canonical_unique
    ).round(2)

    usable_eth = eth_clean[
        ~eth_clean["EthnicityGroup"].isin(
            ["Not recorded", "Unclear / review"]
        )
    ].copy()
    usable_total = usable_eth["Participants"].sum()
    usable_eth["PercentageOfUsableEthnicity"] = (
        100 * usable_eth["Participants"] / usable_total
    ).round(2)

    # Referral reasons - new FULL registrations
    reason_rows = []
    for _, r in new_full.iterrows():
        raw = r.get("ReferralReason")
        if pd.isna(raw):
            continue
        seen = set()
        for part in str(raw).split(";"):
            cleaned = clean_referral_reason(part)
            if cleaned and cleaned not in seen:
                reason_rows.append(
                    {
                        "ParticipantID": r["ParticipantID"],
                        "Reason": cleaned,
                    }
                )
                seen.add(cleaned)

    reasons = pd.DataFrame(reason_rows)
    if reasons.empty:
        reasons_summary = pd.DataFrame(
            columns=[
                "Reason", "Participants",
                "PercentageOfNewFullRegistrations"
            ]
        )
    else:
        reasons_summary = (
            reasons.groupby("Reason")["ParticipantID"]
            .nunique()
            .sort_values(ascending=False)
            .rename("Participants")
            .reset_index()
        )
        reasons_summary["PercentageOfNewFullRegistrations"] = (
            100 * reasons_summary["Participants"] / len(new_full)
        ).round(2)

    # Heard about Saheli
    heard = new_full["HeardAboutSaheli"].map(clean_heard_about)
    heard_summary = (
        heard.value_counts()
        .rename_axis("HeardAboutSaheli")
        .reset_index(name="Participants")
    )
    if len(new_full):
        heard_summary["Percentage"] = (
            100 * heard_summary["Participants"] / len(new_full)
        ).round(2)

    heard_raw = (
        new_full["HeardAboutSaheli"]
        .fillna("Not recorded")
        .replace("", "Not recorded")
        .value_counts()
        .rename_axis("RawHeardAboutSaheli")
        .reset_index(name="Participants")
    )

    # Current CRM profile demographics, because the CEO may use
    # FULL + Lite registrations as the "participant" headline.
    current_profile_rows = []

    for _, r in participants.iterrows():
        current_profile_rows.append(
            {
                "MemberType": "FULL",
                "MemberId": r["ParticipantID"],
                "Gender": clean_gender(r["Gender"])
                    or "Not recorded",
                "DateOfBirth": r["DateOfBirth"],
                "Ethnicity": r["Ethnicity"],
                "Postcode": r["Postcode"],
            }
        )

    for _, r in lite.iterrows():
        current_profile_rows.append(
            {
                "MemberType": "LITE",
                "MemberId": r["Id"],
                "Gender": clean_gender(r["Gender"])
                    or "Not recorded",
                "DateOfBirth": r["DateOfBirth"],
                "Ethnicity": r["Ethnicity"],
                "Postcode": r["Postcode"],
            }
        )

    current_profiles = pd.DataFrame(current_profile_rows)

    current_gender = (
        current_profiles["Gender"]
        .value_counts()
        .rename_axis("Gender")
        .reset_index(name="Profiles")
    )
    current_gender["PercentageOfCurrentProfiles"] = (
        100 * current_gender["Profiles"] / len(current_profiles)
    ).round(2)

    service_metrics = {}
    if table_exists(conn, "Assessment_Master"):
        assessment = sql_df(
            conn,
            """
            SELECT
                COUNT(DISTINCT AssessmentID) AS Assessments,
                COUNT(DISTINCT NULLIF(LTRIM(RTRIM(SaheliCardNumber)), '')) AS PeopleAssessed,
                COUNT(DISTINCT CASE WHEN AssessmentNumber > 1 THEN AssessmentID END) AS FollowUpAssessments,
                COUNT(DISTINCT CASE WHEN AssessmentNumber > 1
                    THEN NULLIF(LTRIM(RTRIM(SaheliCardNumber)), '') END) AS PeopleWithFollowUp
            FROM dbo.Assessment_Master
            WHERE AssessmentDate >= ? AND AssessmentDate < ?
            """,
            [REPORT_START, REPORT_END_EXCLUSIVE],
        ).iloc[0]
        service_metrics = {
            "Health assessments": int(assessment["Assessments"] or 0),
            "People receiving health assessments": int(assessment["PeopleAssessed"] or 0),
            "Follow-up assessments": int(assessment["FollowUpAssessments"] or 0),
            "People receiving follow-up assessments": int(assessment["PeopleWithFollowUp"] or 0),
        }

    # Attendance by financial year (all-time)
    alltime = sql_df(
        conn,
        """
        SELECT s.SessionDate, sa.AttendanceId, sa.SessionId
        FROM dbo.SessionAttendance sa
        INNER JOIN dbo.Sessions s
            ON s.SessionId = sa.SessionId
        WHERE sa.Attended = 1
        """
    )
    alltime["SessionDate"] = pd.to_datetime(alltime["SessionDate"])
    alltime["FinancialYear"] = alltime["SessionDate"].map(
        financial_year_label
    )
    fy = (
        alltime.groupby("FinancialYear")
        .agg(
            AttendanceRows=("AttendanceId", "size"),
            Sessions=("SessionId", "nunique"),
        )
        .reset_index()
        .sort_values("FinancialYear")
    )

    # Period comparison - useful when trying to understand 21k+ claim
    latest_date = alltime["SessionDate"].max()
    from_apr_2025_to_latest = alltime[
        alltime["SessionDate"] >= pd.Timestamp(REPORT_START)
    ]
    period_options = pd.DataFrame(
        [
            [
                "FY 2025/26 confirmed attendance",
                annual_attendance,
                REPORT_START,
                "2026-03-31",
            ],
            [
                "From 1 Apr 2025 to latest CRM attendance",
                len(from_apr_2025_to_latest),
                REPORT_START,
                latest_date.date() if pd.notna(latest_date) else None,
            ],
            [
                "All-time confirmed SessionAttendance",
                len(alltime),
                alltime["SessionDate"].min().date()
                    if not alltime.empty else None,
                latest_date.date() if pd.notna(latest_date) else None,
            ],
        ],
        columns=["PeriodDefinition", "AttendanceRows", "Start", "End"],
    )

    return {
        "Headline Metrics": headline,
        "Location Summary": location,
        "Monthly Summary": monthly,
        "Top Activities": top10,
        "All Activities": activities,
        "Gender Annual": gender,
        "Gender F-M Split": binary,
        "Gender Sources": gender_source,
        "Age Annual": age_summary,
        "Ethnicity Raw": eth_raw,
        "Ethnicity Clean": eth_clean,
        "Ethnicity Usable": usable_eth,
        "Join Reasons": reasons_summary,
        "Heard About": heard_summary,
        "Heard About Raw": heard_raw,
        "CRM Current Gender": current_gender,
        "Attendance by FY": fy,
        "Attendance Period Options": period_options,
        "New FULL Registrations": new_full,
        "Service Metrics": service_metrics,
    }


# ============================================================
# EXTRA-SOURCE AUDIT
# ============================================================

def normalize_match_text(v) -> str:
    if pd.isna(v):
        return ""
    return re.sub(r"[^a-z0-9]+", "", str(v).strip().lower())


def _first_present(row, *names):
    for name in names:
        if name in row.index and pd.notna(row[name]) and str(row[name]).strip():
            return row[name]
    return None


def load_migration_source_evidence():
    """Load committed migration evidence and verify its original workbooks/sheets."""
    configs = [
        ("ARCC", "ARCC MIgrations", "arcc_migration_commit_20260909_211459.csv"),
        ("Calthorpe", "Calthorpe Migrate", "calthorpe_migration_commit_20260908_162750.csv"),
        ("Handsworth", "Handworth Migration", "handsworth_migration_commit_20260909_211932.csv"),
        ("Omnia", "Omnia Migration", "omnia_migration_commit_20260910_145900.csv"),
        ("Men's Services", "Mens Migration", "mens_migration_commit_20260910_135305.csv"),
        ("Tennis", "Tennis Migration", "tennis_migration_commit_20260910_185641.csv"),
        ("Cycling", "Cycling Migration", "bike_giveaway_migration_commit_20260910_234126.csv"),
    ]
    attendance_actions = {
        "NEW_ATTENDANCE", "CREATE_ATTENDANCE", "ALREADY_IN_CRM",
        "SKIP_EXISTING_ATTENDANCE",
    }
    evidence_rows = []
    audit_rows = []
    file_sheet_refs = defaultdict(set)

    for source, folder, filename in configs:
        log_path = PROJECT_ROOT / folder / filename
        if not log_path.exists():
            audit_rows.append({
                "Source": source, "SourceFile": filename, "SourceSheet": None,
                "Status": "REVIEW", "Detail": "Committed migration audit file is missing.",
            })
            continue
        df = pd.read_csv(log_path, dtype=str, keep_default_na=False)
        action_col = "Action" if "Action" in df.columns else "action"
        date_col = "Date" if "Date" in df.columns else "session_date"
        parsed_dates = pd.to_datetime(df[date_col], errors="coerce")
        period = df[
            parsed_dates.between(
                pd.Timestamp(REPORT_START), pd.Timestamp(REPORT_END_EXCLUSIVE),
                inclusive="left",
            )
        ].copy()
        period["_date"] = pd.to_datetime(period[date_col], errors="coerce")
        period["_action"] = period[action_col].astype(str).str.upper().str.strip()
        for idx, row in period.iterrows():
            action = row["_action"]
            if action not in attendance_actions:
                continue
            source_file = _first_present(row, "SourceFile", "source_file")
            source_sheet = _first_present(row, "SourceSheet", "sheet")
            if source == "Cycling":
                source_file = source_file or "Cycling Register 2026.xlsx"
                source_sheet = source_sheet or "Bike Giveaway"
            source_ref = _first_present(
                row, "SourceRow", "source_ref", "SourceSessionRef"
            )
            member_ref = _first_present(row, "MemberRef", "member_display_id")
            participant_id = None
            lite_id = None
            if member_ref and str(member_ref).upper().startswith("FULL:"):
                participant_id = str(member_ref).split(":", 1)[1]
            elif member_ref and str(member_ref).upper().startswith("LITE:"):
                lite_id = str(member_ref).split(":", 1)[1]
            source_path = PROJECT_ROOT / folder / str(source_file or "")
            file_sheet_refs[str(source_path)].add(str(source_sheet or ""))
            evidence_rows.append({
                "Source": source,
                "SourceFile": str(source_file or ""),
                "SourceSheet": str(source_sheet or ""),
                "SourceRow": str(source_ref or idx + 2),
                "Location": _first_present(row, "Venue", "venue") or (
                    "Various Locations" if source == "Cycling" else source
                ),
                "Activity": _first_present(row, "Activity", "activity") or (
                    "Bike Giveaway" if source == "Cycling" else source
                ),
                "SessionDate": row["_date"],
                "StartTime": _first_present(row, "start_time", "StartTime"),
                "EndTime": _first_present(row, "end_time", "EndTime"),
                "ParticipantName": _first_present(
                    row, "SourceName", "source_name", "SourceRawIdentity"
                ),
                "SaheliCardNumber": _first_present(row, "SourceCard", "source_card"),
                "Phone": None,
                "Postcode": None,
                "CRMParticipantId": participant_id,
                "CRMLiteMemberId": lite_id,
                "SourceSessionId": _first_present(row, "SessionId", "session_id"),
                "MigrationAction": action,
                "AttendanceEvidence": (
                    f"Committed migration audit: {filename}; action={action}"
                ),
                "_SourcePath": str(source_path),
            })

    # OCF attendance is held in its own migration evidence and uses UUID identities.
    ocf_folder = PROJECT_ROOT / "OCF Migrations"
    ocf_marks_path = ocf_folder / "ocf_session_attendance_commit_20260913_220537_marks.csv"
    ocf_sessions_path = ocf_folder / "ocf_session_attendance_commit_20260913_220537_sessions.csv"
    if ocf_marks_path.exists() and ocf_sessions_path.exists():
        marks = pd.read_csv(ocf_marks_path, dtype=str, keep_default_na=False)
        session_evidence = pd.read_csv(ocf_sessions_path, dtype=str, keep_default_na=False)
        marks["SessionDate"] = pd.to_datetime(marks["session_date"], errors="coerce")
        marks = marks[
            marks["SessionDate"].between(
                pd.Timestamp(REPORT_START), pd.Timestamp(REPORT_END_EXCLUSIVE),
                inclusive="left",
            ) & marks["attendance_action"].eq("CREATE")
        ].copy()
        lookup = session_evidence.set_index("session_id", drop=False)
        for idx, row in marks.iterrows():
            sr = lookup.loc[row["session_id"]] if row["session_id"] in lookup.index else None
            if isinstance(sr, pd.DataFrame):
                sr = sr.iloc[0]
            source_files = sr["source_files"] if sr is not None else ""
            source_headers = sr["source_headers"] if sr is not None else ""
            for source_file in [x.strip() for x in str(source_files).split("|") if x.strip()] or [""]:
                source_path = ocf_folder / "Session Excels" / source_file
                file_sheet_refs[str(source_path)].add("")
            evidence_rows.append({
                "Source": "OCF",
                "SourceFile": source_files,
                "SourceSheet": "Workbook attendance grid",
                "SourceRow": source_headers or str(idx + 2),
                "Location": "Our Community Foundation",
                "Activity": row["canonical_activity"],
                "SessionDate": row["SessionDate"],
                "StartTime": sr["start_time"] if sr is not None else None,
                "EndTime": sr["end_time"] if sr is not None else None,
                "ParticipantName": row["participant_name"],
                "SaheliCardNumber": row["ocf_id"],
                "Phone": None,
                "Postcode": None,
                "CRMParticipantId": None,
                "CRMLiteMemberId": None,
                "SourceSessionId": row["session_id"],
                "MigrationAction": "CREATE",
                "AttendanceEvidence": (
                    "Committed OCF attendance migration; source mark="
                    + str(row["source_mark"])
                ),
                "_SourcePath": str(ocf_folder / "Session Excels" / source_files),
            })

    # Open the original workbooks and verify each referenced sheet exists.
    from openpyxl import load_workbook
    verified_files = set()
    verified_sheets = set()
    for path_text, sheets in sorted(file_sheet_refs.items()):
        path = Path(path_text)
        if not path.exists():
            audit_rows.append({
                "Source": "Source workbook", "SourceFile": path.name,
                "SourceSheet": None, "Status": "REVIEW",
                "Detail": f"Referenced original source file not found: {path}",
            })
            continue
        try:
            wb = load_workbook(path, read_only=True, data_only=True)
            verified_files.add(str(path.resolve()))
            requested = {s for s in sheets if s}
            inspect_sheets = requested or set(wb.sheetnames)
            for sheet in sorted(inspect_sheets):
                if sheet in wb.sheetnames:
                    ws = wb[sheet]
                    # Force a real read of source content, not just workbook metadata.
                    next(ws.iter_rows(min_row=1, max_row=min(ws.max_row, 5), values_only=True), None)
                    verified_sheets.add((str(path.resolve()), sheet))
                    audit_rows.append({
                        "Source": "Source workbook", "SourceFile": path.name,
                        "SourceSheet": sheet, "Status": "INSPECTED",
                        "Detail": f"Original workbook opened; dimensions {ws.max_row}x{ws.max_column}.",
                    })
                else:
                    audit_rows.append({
                        "Source": "Source workbook", "SourceFile": path.name,
                        "SourceSheet": sheet, "Status": "REVIEW",
                        "Detail": "Referenced sheet not found in original workbook.",
                    })
            wb.close()
        except Exception as exc:
            audit_rows.append({
                "Source": "Source workbook", "SourceFile": path.name,
                "SourceSheet": None, "Status": "REVIEW",
                "Detail": f"Could not inspect original workbook: {exc}",
            })

    evidence = pd.DataFrame(evidence_rows)
    if not evidence.empty:
        evidence["SourceFileVerified"] = evidence["_SourcePath"].map(
            lambda p: str(Path(p).resolve()) in verified_files
        )
    return evidence, pd.DataFrame(audit_rows), verified_files, verified_sheets


def reconcile_source_evidence(annual, source_rows):
    """Classify source attendance against live CRM using conservative match keys."""
    if source_rows.empty:
        return source_rows.copy()
    crm = annual.copy()
    crm["_date"] = pd.to_datetime(crm["SessionDate"]).dt.date
    crm["_activity"] = crm["ActivityName"].map(normalize_match_text)
    crm["_location"] = crm["VenueName"].map(normalize_match_text)
    crm["_name"] = crm["MemberName"].map(normalize_name)
    crm["_phone"] = crm["Phone"].map(normalize_phone)
    crm["_card"] = crm["SaheliCardNumber"].fillna("").astype(str).str.strip().str.lower()

    by_session_full = set()
    by_session_lite = set()
    by_date_full = set()
    by_date_lite = set()
    by_date_card = set()
    by_date_name = set()
    for _, r in crm.iterrows():
        session = str(r["SessionId"])
        d = r["_date"]
        activity = r["_activity"]
        if pd.notna(r["ParticipantId"]):
            pid = str(int(r["ParticipantId"]))
            by_session_full.add((session, pid))
            by_date_full.add((d, activity, pid))
        if pd.notna(r["LiteMemberId"]):
            lid = str(r["LiteMemberId"]).lower()
            by_session_lite.add((session, lid))
            by_date_lite.add((d, activity, lid))
        if r["_card"]:
            by_date_card.add((d, activity, r["_card"]))
        if r["_name"]:
            by_date_name.add((d, activity, r["_name"]))

    out = source_rows.copy()
    classifications = []
    in_crm_values = []
    match_notes = []
    seen = set()

    def scalar_text(value):
        return "" if pd.isna(value) else str(value).strip()

    for _, r in out.iterrows():
        d = pd.Timestamp(r["SessionDate"]).date()
        activity = normalize_match_text(r["Activity"])
        session = scalar_text(r.get("SourceSessionId"))
        pid = scalar_text(r.get("CRMParticipantId")).split(".0")[0]
        lid = scalar_text(r.get("CRMLiteMemberId")).lower()
        card = scalar_text(r.get("SaheliCardNumber")).lower()
        name = normalize_name(r.get("ParticipantName"))
        person_key = pid or lid or card or name
        dedupe_key = (
            r["Source"], d, activity, normalize_match_text(r.get("Location")),
            scalar_text(r.get("StartTime")), person_key,
        )
        if dedupe_key in seen:
            classifications.append("DUPLICATE_SOURCE")
            in_crm_values.append(False)
            match_notes.append("Duplicate source identity/session evidence.")
            continue
        seen.add(dedupe_key)
        matched = False
        note = ""
        if pid and ((session, pid) in by_session_full or (d, activity, pid) in by_date_full):
            matched, note = True, "Matched CRM by FULL participant plus session/date/activity."
        elif lid and ((session, lid) in by_session_lite or (d, activity, lid) in by_date_lite):
            matched, note = True, "Matched CRM by Lite member plus session/date/activity."
        elif card and (d, activity, card) in by_date_card:
            matched, note = True, "Matched CRM by card plus date/activity."
        elif name and (d, activity, name) in by_date_name:
            matched, note = True, "Matched CRM by exact normalized name plus date/activity."
        if matched:
            classifications.append("CRM_CONFIRMED")
            in_crm_values.append(True)
            match_notes.append(note)
        elif bool(r.get("SourceFileVerified")):
            classifications.append("SOURCE_ONLY_VERIFIED")
            in_crm_values.append(False)
            match_notes.append("Committed attendance evidence found in an inspected original workbook.")
        else:
            classifications.append("REVIEW")
            in_crm_values.append(False)
            match_notes.append("Original workbook/sheet could not be verified.")
    out["InCRM"] = in_crm_values
    out["Classification"] = classifications
    out["ReconciliationNote"] = match_notes
    return out


def build_verified_delivery_ledger(annual, reconciled_sources):
    crm = pd.DataFrame({
        "Source": "CRM",
        "SourceFile": "dbo.SessionAttendance",
        "SourceSheet": "dbo.Sessions",
        "SourceRow": annual["AttendanceId"],
        "Location": annual["VenueName"],
        "Activity": annual["ActivityName"],
        "SessionDate": annual["SessionDate"],
        "StartTime": annual.get("StartTime"),
        "EndTime": annual.get("EndTime"),
        "ParticipantName": annual["MemberName"],
        "SaheliCardNumber": annual["SaheliCardNumber"],
        "Phone": annual["Phone"],
        "Postcode": None,
        "CRMParticipantId": annual["ParticipantId"],
        "CRMLiteMemberId": annual["LiteMemberId"],
        "InCRM": True,
        "AttendanceEvidence": "dbo.SessionAttendance.Attended = 1",
        "Classification": "CRM_CONFIRMED",
        "SessionKey": annual["SessionId"].map(lambda x: f"CRM:{x}"),
    })
    source_only = reconciled_sources[
        reconciled_sources["Classification"] == "SOURCE_ONLY_VERIFIED"
    ].copy()
    source_only["SessionKey"] = source_only.apply(
        lambda r: "SOURCE:" + "|".join([
            str(r.get("Source")), str(pd.Timestamp(r.get("SessionDate")).date()),
            normalize_match_text(r.get("Location")), normalize_match_text(r.get("Activity")),
            str(r.get("StartTime") or ""), str(r.get("SourceSessionId") or ""),
        ]), axis=1,
    )
    wanted = [
        "Source", "SourceFile", "SourceSheet", "SourceRow", "Location", "Activity",
        "SessionDate", "StartTime", "EndTime", "ParticipantName", "SaheliCardNumber",
        "Phone", "Postcode", "CRMParticipantId", "CRMLiteMemberId", "InCRM",
        "AttendanceEvidence", "Classification", "SessionKey",
    ]
    verified = pd.concat([crm[wanted], source_only[wanted]], ignore_index=True)
    verified["SessionDate"] = pd.to_datetime(verified["SessionDate"], errors="coerce")
    return verified

def source_audit(conn) -> pd.DataFrame:
    """Inventory every plausible service-delivery source without counting it as attendance."""
    inventory = sql_df(
        conn,
        """
        SELECT t.name AS TableName, c.name AS ColumnName
        FROM sys.tables t
        INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
        INNER JOIN sys.columns c ON c.object_id = t.object_id
        WHERE s.name = 'dbo'
        ORDER BY t.name, c.column_id
        """,
    )
    terms = re.compile(
        r"attendance|session|register|bellboat|cycling|migrat|histor|delivery|activity|assessment",
        re.I,
    )
    date_candidates = (
        "SessionDate", "AttendanceDate", "RegisterDate", "ActivityDate",
        "DeliveryDate", "AssessmentDate", "Date", "CreatedAtUtc", "CreatedAt",
    )
    rows = []
    for table_name, group in inventory.groupby("TableName", sort=True):
        columns = [str(x) for x in group["ColumnName"]]
        if not terms.search(table_name + " " + " ".join(columns)):
            continue
        quoted_table = table_name.replace("]", "]]" )
        total = int(sql_df(
            conn, f"SELECT COUNT_BIG(*) AS N FROM dbo.[{quoted_table}]"
        ).iloc[0]["N"])
        date_col = next((c for c in date_candidates if c in columns), None)
        period_rows = None
        if table_name == "SessionAttendance":
            period_rows = int(sql_df(
                conn,
                """SELECT COUNT_BIG(*) AS N
                   FROM dbo.SessionAttendance sa
                   INNER JOIN dbo.Sessions s ON s.SessionId = sa.SessionId
                   WHERE s.SessionDate >= ? AND s.SessionDate < ?
                     AND sa.Attended = 1""",
                [REPORT_START, REPORT_END_EXCLUSIVE],
            ).iloc[0]["N"])
            date_col = "Sessions.SessionDate"
        elif date_col:
            quoted_col = date_col.replace("]", "]]" )
            period_rows = int(sql_df(
                conn,
                f"""SELECT COUNT_BIG(*) AS N FROM dbo.[{quoted_table}]
                    WHERE TRY_CONVERT(date, [{quoted_col}]) >= ?
                      AND TRY_CONVERT(date, [{quoted_col}]) < ?""",
                [REPORT_START, REPORT_END_EXCLUSIVE],
            ).iloc[0]["N"])

        lower_cols = {c.lower() for c in columns}
        has_attended = any(c in lower_cols for c in (
            "attended", "isattended", "attendanceconfirmed", "present"
        ))
        if table_name == "SessionAttendance":
            classification = "CORE CONFIRMED ATTENDANCE"
        elif has_attended:
            classification = "POSSIBLE ATTENDANCE - REVIEW FOR OVERLAP"
        elif re.search(r"booking|assignment|registration", table_name, re.I):
            classification = "NON-ATTENDANCE REGISTRATION/BOOKING"
        else:
            classification = "POSSIBLE DELIVERY SOURCE - SEMANTICS REQUIRE REVIEW"
        rows.append({
            "Source": table_name,
            "TotalRows": total,
            "RowsIn2025_26": period_rows,
            "DateColumn": date_col,
            "HasExplicitAttendanceField": has_attended,
            "Classification": classification,
            "Columns": ", ".join(columns),
        })
    return pd.DataFrame(rows)


# ============================================================
# OPTIONAL IMD MATCHING
# ============================================================

def add_imd_if_available(canonical_people):
    if not IMD_POSTCODE_CSV:
        return None
    path = Path(IMD_POSTCODE_CSV)
    if not path.exists():
        print(f"IMD file not found: {path}")
        return None

    imd = pd.read_csv(path, dtype=str)
    cols = {c.lower().strip(): c for c in imd.columns}

    postcode_col = None
    for candidate in (
        "postcode", "pcds", "pcd", "postcodeclean"
    ):
        if candidate in cols:
            postcode_col = cols[candidate]
            break

    decile_col = None
    for c in imd.columns:
        lc = c.lower()
        if "index of multiple deprivation" in lc and "decile" in lc:
            decile_col = c
            break
        if lc in {"imd decile", "imd_decile"}:
            decile_col = c
            break

    if not postcode_col or not decile_col:
        print(
            "IMD CSV loaded, but postcode/IMD decile columns "
            "could not be detected."
        )
        return None

    imd = imd[[postcode_col, decile_col]].copy()
    imd["postcode_key"] = imd[postcode_col].map(normalize_postcode)
    imd["IMDDecile"] = pd.to_numeric(
        imd[decile_col], errors="coerce"
    )

    cp = canonical_people.copy()
    cp["postcode_key"] = cp["Postcode"].map(normalize_postcode)
    matched = cp.merge(
        imd[["postcode_key", "IMDDecile"]].drop_duplicates("postcode_key"),
        on="postcode_key",
        how="left",
    )

    summary = (
        matched["IMDDecile"]
        .dropna()
        .astype(int)
        .value_counts()
        .sort_index()
        .rename_axis("IMDDecile")
        .reset_index(name="Participants")
    )

    if not summary.empty:
        matched_n = summary["Participants"].sum()
        summary["PercentageOfMatched"] = (
            100 * summary["Participants"] / matched_n
        ).round(2)

    return matched, summary


# ============================================================
# EXCEL OUTPUT
# ============================================================

def write_excel(
    summaries,
    source_audit_df,
    annual,
    canonical_people,
    imd_result=None,
):
    from openpyxl.styles import Font, PatternFill

    headline = summaries["Headline Metrics"].iloc[:8].copy()
    headline["Metric"] = [
        "Total confirmed attendance", "Total attended sessions",
        "Raw unique attendance identities", "Canonical unique people",
        "Current FULL members", "Current Lite members",
        "FULL + Lite registered members",
        "New FULL registrations during 2025/26",
    ]

    gender_order = ["Female", "Male", "Other", "Not recorded"]
    gender_counts = canonical_people["ReportGender"].fillna("Not recorded").value_counts()
    gender = pd.DataFrame({
        "Gender": gender_order,
        "Participants": [int(gender_counts.get(x, 0)) for x in gender_order],
    })
    gender["Percentage"] = gender["Participants"] / max(len(canonical_people), 1)
    fm_total = int(gender_counts.get("Female", 0) + gender_counts.get("Male", 0))
    female_fm = int(gender_counts.get("Female", 0)) / fm_total if fm_total else None
    male_fm = int(gender_counts.get("Male", 0)) / fm_total if fm_total else None

    age = summaries["Age Annual"].rename(columns={
        "AgeBand": "Age Band",
        "PercentageOfCanonicalPeople": "% of all participants",
        "PercentageOfValidAge": "% of participants with valid DOB",
    }).copy()
    for col in ("% of all participants", "% of participants with valid DOB"):
        age[col] = pd.to_numeric(age[col], errors="coerce") / 100

    eth_raw = summaries["Ethnicity Raw"].rename(columns={"Ethnicity": "Raw Ethnicity"})
    eth_order = [
        "Asian / Asian British", "Black / Black British", "Arab", "White",
        "Mixed", "Other ethnic background", "Unclear / review", "Not recorded",
    ]
    eth_counts = canonical_people["EthnicityGroup"].value_counts()
    eth_clean = pd.DataFrame({
        "Ethnicity Group": eth_order,
        "Participants": [int(eth_counts.get(x, 0)) for x in eth_order],
    })
    eth_clean["Percentage"] = eth_clean["Participants"] / max(len(canonical_people), 1)
    usable_mask = ~canonical_people["EthnicityGroup"].isin(["Not recorded", "Unclear / review"])
    usable_ethnicity = int(usable_mask.sum())
    diverse_participants = int((usable_mask & (canonical_people["EthnicityGroup"] != "White")).sum())
    diverse_pct = diverse_participants / usable_ethnicity if usable_ethnicity else None

    postcode_recorded = int(canonical_people["Postcode"].fillna("").astype(str).str.strip().ne("").sum())
    postcode_missing = len(canonical_people) - postcode_recorded
    postcode_coverage = postcode_recorded / max(len(canonical_people), 1)

    reasons = summaries["Join Reasons"].rename(columns={
        "PercentageOfNewFullRegistrations": "Percentage of new FULL registrations"
    }).copy()
    if "Percentage of new FULL registrations" in reasons:
        reasons["Percentage of new FULL registrations"] /= 100
    heard = summaries["Heard About"].rename(columns={
        "HeardAboutSaheli": "Source"
    }).copy()
    if "Percentage" in heard:
        heard["Percentage"] /= 100

    top10 = summaries["Top Activities"][[
        "Activity", "Attendances", "Sessions", "CanonicalUniquePeople"
    ]].rename(columns={"CanonicalUniquePeople": "Canonical Unique Participants"})

    annual_export = annual[[
        "CanonicalPersonKey", "AttendanceId", "SessionId", "SessionDate",
        "VenueName", "ActivityName", "ParticipantId", "LiteMemberId",
        "MemberDisplayId", "MemberName", "Phone", "Attended",
    ]].copy()
    person_detail = canonical_people[[
        "CanonicalPersonKey", "MemberNumber", "FullName", "ReportGender",
        "DateOfBirth", "Ethnicity", "Postcode",
    ]].rename(columns={"ReportGender": "Gender"})
    annual_export = annual_export.merge(person_detail, on="CanonicalPersonKey", how="left")
    annual_export = annual_export.rename(columns={"MemberDisplayId": "MemberNumber_Attendance"})
    annual_export["MemberNumber"] = annual_export["MemberNumber"].fillna(
        annual_export["MemberNumber_Attendance"]
    )
    annual_export = annual_export[[
        "CanonicalPersonKey", "AttendanceId", "SessionId", "SessionDate",
        "VenueName", "ActivityName", "ParticipantId", "LiteMemberId",
        "MemberNumber", "MemberName", "Phone", "Gender", "DateOfBirth",
        "Ethnicity", "Postcode", "Attended",
    ]]

    raw_people = canonical_people.rename(columns={
        "CanonicalMemberType": "MemberType", "ReportGender": "Gender",
        "EthnicityGroup": "CleanEthnicityGroup",
    })[[
        "CanonicalPersonKey", "MemberType", "ParticipantId", "LiteMemberId",
        "MemberNumber", "FullName", "Phone", "Gender", "GenderResolutionSource",
        "DateOfBirth", "AgeAt31Mar2026", "AgeBand", "Ethnicity",
        "CleanEthnicityGroup", "Postcode", "FirstAttendance", "LastAttendance",
        "AttendanceRecords",
    ]]

    audit_candidates = source_audit_df[
        (source_audit_df["Source"] != "SessionAttendance")
        & source_audit_df["RowsIn2025_26"].fillna(0).gt(0)
    ]
    backup_mask = audit_candidates["Source"].str.contains(
        r"backup|cleanup|test", case=False, regex=True, na=False
    )
    backup_rows = int(audit_candidates.loc[backup_mask, "RowsIn2025_26"].fillna(0).sum())
    explicit = audit_candidates[
        audit_candidates["HasExplicitAttendanceField"] & ~backup_mask
    ]
    staging = audit_candidates[audit_candidates["Source"] == "ActivityRegisterImport"]
    staging_rows = int(staging["RowsIn2025_26"].fillna(0).sum())
    if explicit.empty:
        audit_note = (
            "No live separate source with an explicit attendance field was proven safe to add. "
            f"ActivityRegisterImport has {staging_rows:,} period rows but no attended flag, so it "
            f"was treated as staging/register data. Cleanup/test backup tables contain {backup_rows:,} "
            "period rows and were excluded as duplicate/test-history records."
        )
    else:
        audit_note = (
            "Attendance-like rows exist outside SessionAttendance in: "
            + ", ".join(explicit["Source"].astype(str))
            + ". They were not added because overlap/attendance semantics are not proven."
        )

    current_metrics = {
        "Total confirmed attendance": len(annual),
        "Total attended sessions": annual["SessionId"].nunique(),
        "Raw unique attendance identities": annual["raw_identity_key"].nunique(),
        "Canonical unique people": len(canonical_people),
        "Current FULL members": int(headline.iloc[4]["Value"]),
        "Current Lite members": int(headline.iloc[5]["Value"]),
        "FULL + Lite registered members": int(headline.iloc[6]["Value"]),
        "New FULL registrations during 2025/26": int(headline.iloc[7]["Value"]),
        "Female % among recorded Female/Male": female_fm,
        "Male % among recorded Female/Male": male_fm,
        "Ethnically diverse percentage": diverse_pct,
        "Postcode coverage percentage": postcode_coverage,
        "Participants attending more than once": int(canonical_people["AttendanceRecords"].gt(1).sum()),
        "Participants with valid DOB": int(canonical_people["AgeAt31Mar2026"].notna().sum()),
    }
    current_metrics.update(summaries.get("Service Metrics", {}))
    if imd_result is not None:
        imd_people, _ = imd_result
        matched = pd.to_numeric(imd_people["IMDDecile"], errors="coerce").notna()
        current_metrics["IMD percentage"] = matched.sum() / max(len(imd_people), 1)

    yoy_rows = []
    for metric, this_year in current_metrics.items():
        last_year = LAST_YEAR.get(metric)
        if last_year is None or this_year is None:
            difference = None
            change = None
            status = "NOT DIRECTLY COMPARABLE"
            note = "No equivalent published prior-year figure supplied."
        else:
            difference = this_year - last_year
            change = difference / last_year if last_year else None
            status = "HIGHER" if difference > 0 else "LOWER" if difference < 0 else "NOT DIRECTLY COMPARABLE"
            if status == "HIGHER":
                note = "HIGHER THAN LAST YEAR"
            elif status == "LOWER":
                note = "LOWER THAN LAST YEAR - REVIEW METHODOLOGY / SOURCE COVERAGE"
            else:
                note = "No change."
            if metric == "Total confirmed attendance":
                note += " " + audit_note
            elif metric == "Canonical unique people":
                note += " Current year uses conservative canonical FULL/Lite deduplication; prior definition may differ."
            elif "%" in metric or "percentage" in metric.lower():
                note += " Difference is percentage points; % Change is relative change."
        yoy_rows.append([metric, last_year, this_year, difference, change, status, note])
    yoy = pd.DataFrame(yoy_rows, columns=[
        "Metric", "Last Year", "This Year", "Difference", "% Change", "Status", "Notes"
    ])

    with pd.ExcelWriter(
        OUTPUT_XLSX,
        engine="openpyxl",
    ) as writer:
        headline.to_excel(writer, sheet_name="SUMMARY", index=False)

        gender.to_excel(writer, sheet_name="DEMOGRAPHICS", index=False, startrow=1)
        binary_display = pd.DataFrame([
            ["Female % among recorded Female/Male", female_fm],
            ["Male % among recorded Female/Male", male_fm],
        ], columns=["Recorded Female/Male measure", "Percentage"])
        binary_display.to_excel(writer, sheet_name="DEMOGRAPHICS", index=False, startrow=8)
        age.to_excel(writer, sheet_name="DEMOGRAPHICS", index=False, startrow=13)
        eth_raw.to_excel(writer, sheet_name="DEMOGRAPHICS", index=False, startrow=26)
        eth_start = 29 + len(eth_raw)
        eth_clean.to_excel(writer, sheet_name="DEMOGRAPHICS", index=False, startrow=eth_start)
        eth_metrics = pd.DataFrame([
            ["Ethnically diverse participants", diverse_participants],
            ["Usable ethnicity records", usable_ethnicity],
            ["Ethnically diverse percentage", diverse_pct],
        ], columns=["Ethnicity measure", "Value"])
        eth_metrics.to_excel(writer, sheet_name="DEMOGRAPHICS", index=False, startrow=eth_start + 11)
        postcode = pd.DataFrame([
            ["Postcode recorded", postcode_recorded],
            ["Postcode missing", postcode_missing],
            ["Postcode coverage %", postcode_coverage],
        ], columns=["Postcode measure", "Value"])
        postcode.to_excel(writer, sheet_name="DEMOGRAPHICS", index=False, startrow=eth_start + 17)

        reasons.to_excel(writer, sheet_name="REGISTRATION INSIGHTS", index=False, startrow=1)
        heard.to_excel(writer, sheet_name="REGISTRATION INSIGHTS", index=False, startrow=4 + len(reasons))
        top10.to_excel(writer, sheet_name="TOP ACTIVITIES", index=False)
        annual_export.to_excel(writer, sheet_name="RAW ATTENDANCE", index=False)
        raw_people.to_excel(writer, sheet_name="RAW PARTICIPANTS", index=False)
        yoy.to_excel(writer, sheet_name="YEAR ON YEAR", index=False)

        # Basic presentation formatting
        wb = writer.book
        for ws in wb.worksheets:
            ws.freeze_panes = "A2"
            ws.auto_filter.ref = ws.dimensions
            ws.sheet_view.showGridLines = False
            for row in ws.iter_rows():
                for cell in row:
                    if cell.value in {
                        "Metric", "Gender", "Recorded Female/Male measure", "Age Band",
                        "Raw Ethnicity", "Ethnicity Group", "Ethnicity measure",
                        "Postcode measure", "Reason", "Source", "Activity",
                        "CanonicalPersonKey",
                    }:
                        cell.font = Font(bold=True, color="FFFFFF")
                        cell.fill = PatternFill("solid", fgColor="1F4E78")
            for column_cells in ws.columns:
                max_length = 0
                col_letter = column_cells[0].column_letter
                for cell in column_cells[:500]:
                    try:
                        val = "" if cell.value is None else str(cell.value)
                        max_length = max(max_length, len(val))
                    except Exception:
                        pass
                ws.column_dimensions[col_letter].width = min(
                    max(max_length + 2, 12), 45
                )
            for row in ws.iter_rows():
                for cell in row:
                    if isinstance(cell.value, (date, datetime, pd.Timestamp)):
                        cell.number_format = "DD/MM/YYYY"
            for cell in ws[1]:
                cell.font = Font(bold=True, color="FFFFFF")
                cell.fill = PatternFill("solid", fgColor="1F4E78")

        demo_ws = wb["DEMOGRAPHICS"]
        for row in range(3, 7):
            demo_ws.cell(row, 3).number_format = "0.0%"
        for row in range(10, 12):
            demo_ws.cell(row, 2).number_format = "0.0%"
        for row in range(15, 24):
            demo_ws.cell(row, 3).number_format = "0.0%"
            demo_ws.cell(row, 4).number_format = "0.0%"
        for row in range(eth_start + 2, eth_start + 10):
            demo_ws.cell(row, 3).number_format = "0.0%"
        demo_ws.cell(eth_start + 15, 2).number_format = "0.0%"
        demo_ws.cell(eth_start + 21, 2).number_format = "0.0%"

        reg_ws = wb["REGISTRATION INSIGHTS"]
        for row in range(3, 3 + len(reasons)):
            reg_ws.cell(row, 3).number_format = "0.0%"
        heard_header_row = 5 + len(reasons)
        for row in range(heard_header_row + 1, heard_header_row + 1 + len(heard)):
            reg_ws.cell(row, 3).number_format = "0.0%"

        yoy_ws = wb["YEAR ON YEAR"]
        for row in range(2, yoy_ws.max_row + 1):
            yoy_ws.cell(row, 5).number_format = "0.0%"
            metric = str(yoy_ws.cell(row, 1).value or "").lower()
            if "%" in metric or "percentage" in metric:
                for col in range(2, 5):
                    yoy_ws.cell(row, col).number_format = "0.0%"
            if yoy_ws.cell(row, 6).value == "HIGHER":
                yoy_ws.cell(row, 6).fill = PatternFill("solid", fgColor="C6EFCE")
            elif yoy_ws.cell(row, 6).value == "LOWER":
                yoy_ws.cell(row, 6).fill = PatternFill("solid", fgColor="FFC7CE")


def write_complete_excel(
    summaries,
    verified_delivery,
    reconciled_sources,
    inspection_audit,
    canonical_people,
    participants,
    lite,
):
    """Write the requested seven sheets using the established report styling."""
    from openpyxl.styles import Font, PatternFill

    crm_count = int((verified_delivery["Classification"] == "CRM_CONFIRMED").sum())
    source_only_count = int(
        (verified_delivery["Classification"] == "SOURCE_ONLY_VERIFIED").sum()
    )
    combined = len(verified_delivery)
    new_full = summaries["New FULL Registrations"]
    summary = pd.DataFrame([
        ["Total verified annual attendance", combined, 21777, combined - 21777],
        ["CRM attendance portion", crm_count, None, None],
        ["Verified source-only additional attendance", source_only_count, None, None],
        ["REGISTERED PARTICIPANTS", len(participants) + len(lite), 1897,
         len(participants) + len(lite) - 1897],
        ["Current FULL members", len(participants), None, None],
        ["Current Lite members", len(lite), None, None],
        ["Canonical annual attendees", len(canonical_people), None, None],
        ["New FULL registrations", len(new_full), 598, len(new_full) - 598],
        ["Sessions delivered", verified_delivery["SessionKey"].nunique(), None, None],
    ], columns=["Metric", "This Year", "Last Year", "Difference"])

    gender_order = ["Female", "Male", "Other", "Not recorded"]
    gender_counts = canonical_people["ReportGender"].fillna("Not recorded").value_counts()
    gender = pd.DataFrame({
        "Gender": gender_order,
        "Participants": [int(gender_counts.get(x, 0)) for x in gender_order],
    })
    gender["Percentage"] = gender["Participants"] / max(len(canonical_people), 1)
    fm_total = int(gender_counts.get("Female", 0) + gender_counts.get("Male", 0))
    fm = pd.DataFrame([
        ["Female % among recorded Female/Male",
         int(gender_counts.get("Female", 0)) / fm_total if fm_total else None],
        ["Male % among recorded Female/Male",
         int(gender_counts.get("Male", 0)) / fm_total if fm_total else None],
    ], columns=["Recorded Female/Male measure", "Percentage"])

    age = summaries["Age Annual"].rename(columns={
        "AgeBand": "Age Band",
        "PercentageOfCanonicalPeople": "% of all participants",
        "PercentageOfValidAge": "% of participants with valid DOB",
    }).copy()
    for col in ("% of all participants", "% of participants with valid DOB"):
        age[col] = pd.to_numeric(age[col], errors="coerce") / 100

    eth_raw = summaries["Ethnicity Raw"].rename(columns={"Ethnicity": "Raw Ethnicity"})
    eth_clean = summaries["Ethnicity Clean"].rename(columns={
        "EthnicityGroup": "Ethnicity Group",
        "PercentageOfCanonicalPeople": "Percentage",
    }).copy()
    eth_clean["Percentage"] = pd.to_numeric(eth_clean["Percentage"], errors="coerce") / 100
    postcode_recorded = int(
        canonical_people["Postcode"].fillna("").astype(str).str.strip().ne("").sum()
    )
    postcode = pd.DataFrame([
        ["Postcode recorded", postcode_recorded],
        ["Postcode missing", len(canonical_people) - postcode_recorded],
        ["Postcode coverage %", postcode_recorded / max(len(canonical_people), 1)],
    ], columns=["Postcode / IMD measure", "Value"])

    reasons = summaries["Join Reasons"].rename(columns={
        "PercentageOfNewFullRegistrations": "Percentage of new FULL registrations"
    }).copy()
    if "Percentage of new FULL registrations" in reasons:
        reasons["Percentage of new FULL registrations"] /= 100
    heard = summaries["Heard About"].rename(columns={"HeardAboutSaheli": "Source"}).copy()
    if "Percentage" in heard:
        heard["Percentage"] /= 100

    top = (
        verified_delivery.groupby("Activity", dropna=False)
        .agg(
            Attendances=("Classification", "size"),
            Sessions=("SessionKey", "nunique"),
            UniqueParticipants=("ParticipantName", lambda s: s.map(normalize_name).replace("", pd.NA).nunique()),
        )
        .reset_index()
        .sort_values("Attendances", ascending=False)
        .head(10)
        .rename(columns={"UniqueParticipants": "Canonical Unique Participants"})
    )

    class_summary = (
        reconciled_sources["Classification"].value_counts()
        .rename_axis("Classification").reset_index(name="Rows")
    )
    audit_detail = reconciled_sources[[
        "Source", "SourceFile", "SourceSheet", "SourceRow", "Location", "Activity",
        "SessionDate", "StartTime", "EndTime", "ParticipantName", "SaheliCardNumber",
        "CRMParticipantId", "CRMLiteMemberId", "InCRM", "MigrationAction",
        "Classification", "AttendanceEvidence", "ReconciliationNote",
    ]].copy()
    raw_delivery = verified_delivery.drop(columns=["SessionKey"]).copy()
    raw_people = canonical_people.rename(columns={
        "CanonicalMemberType": "MemberType", "ReportGender": "Gender",
        "EthnicityGroup": "CleanEthnicityGroup",
    })[[
        "CanonicalPersonKey", "MemberType", "ParticipantId", "LiteMemberId",
        "MemberNumber", "FullName", "Phone", "Gender", "GenderResolutionSource",
        "DateOfBirth", "AgeAt31Mar2026", "AgeBand", "Ethnicity",
        "CleanEthnicityGroup", "Postcode", "FirstAttendance", "LastAttendance",
        "AttendanceRecords",
    ]]

    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
        summary.to_excel(writer, sheet_name="SUMMARY", index=False)
        gender.to_excel(writer, sheet_name="DEMOGRAPHICS", index=False, startrow=1)
        fm.to_excel(writer, sheet_name="DEMOGRAPHICS", index=False, startrow=8)
        age.to_excel(writer, sheet_name="DEMOGRAPHICS", index=False, startrow=13)
        eth_raw.to_excel(writer, sheet_name="DEMOGRAPHICS", index=False, startrow=26)
        eth_start = 29 + len(eth_raw)
        eth_clean.to_excel(writer, sheet_name="DEMOGRAPHICS", index=False, startrow=eth_start)
        postcode.to_excel(
            writer, sheet_name="DEMOGRAPHICS", index=False,
            startrow=eth_start + len(eth_clean) + 3,
        )
        top.to_excel(writer, sheet_name="TOP ACTIVITIES", index=False)
        reasons.to_excel(writer, sheet_name="REGISTRATION INSIGHTS", index=False, startrow=1)
        heard.to_excel(
            writer, sheet_name="REGISTRATION INSIGHTS", index=False,
            startrow=4 + len(reasons),
        )
        inspection_audit.to_excel(
            writer, sheet_name="DELIVERY SOURCE AUDIT", index=False, startrow=1
        )
        class_start = 4 + len(inspection_audit)
        class_summary.to_excel(
            writer, sheet_name="DELIVERY SOURCE AUDIT", index=False, startrow=class_start
        )
        audit_detail.to_excel(
            writer, sheet_name="DELIVERY SOURCE AUDIT", index=False,
            startrow=class_start + len(class_summary) + 3,
        )
        raw_delivery.to_excel(writer, sheet_name="RAW VERIFIED DELIVERY", index=False)
        raw_people.to_excel(writer, sheet_name="RAW PARTICIPANTS", index=False)

        # Preserve the analyzer's established simple professional formatting.
        wb = writer.book
        header_labels = {
            "Metric", "Gender", "Recorded Female/Male measure", "Age Band",
            "Raw Ethnicity", "Ethnicity Group", "Postcode / IMD measure",
            "Activity", "Reason", "Source", "Classification", "CanonicalPersonKey",
        }
        for ws in wb.worksheets:
            ws.freeze_panes = "A2"
            ws.auto_filter.ref = ws.dimensions
            ws.sheet_view.showGridLines = False
            for row in ws.iter_rows():
                for cell in row:
                    if cell.value in header_labels:
                        cell.font = Font(bold=True, color="FFFFFF")
                        cell.fill = PatternFill("solid", fgColor="1F4E78")
                    if isinstance(cell.value, (date, datetime, pd.Timestamp)):
                        cell.number_format = "DD/MM/YYYY"
            for cell in ws[1]:
                cell.font = Font(bold=True, color="FFFFFF")
                cell.fill = PatternFill("solid", fgColor="1F4E78")
            for column_cells in ws.columns:
                max_length = max(
                    (len(str(cell.value)) if cell.value is not None else 0)
                    for cell in column_cells[:500]
                )
                ws.column_dimensions[column_cells[0].column_letter].width = min(
                    max(max_length + 2, 12), 45
                )

        demo = wb["DEMOGRAPHICS"]
        for row in range(3, 7):
            demo.cell(row, 3).number_format = "0.0%"
        for row in range(10, 12):
            demo.cell(row, 2).number_format = "0.0%"
        for row in range(15, 24):
            demo.cell(row, 3).number_format = "0.0%"
            demo.cell(row, 4).number_format = "0.0%"
        for row in range(eth_start + 2, eth_start + 2 + len(eth_clean)):
            demo.cell(row, 3).number_format = "0.0%"
        demo.cell(eth_start + len(eth_clean) + 7, 2).number_format = "0.0%"
        reg = wb["REGISTRATION INSIGHTS"]
        for row in range(3, 3 + len(reasons)):
            reg.cell(row, 3).number_format = "0.0%"
        heard_header = 5 + len(reasons)
        for row in range(heard_header + 1, heard_header + 1 + len(heard)):
            reg.cell(row, 3).number_format = "0.0%"


def apply_saheli_branding(input_path, output_path):
    """Presentation-only pass: brand, chart, and add documented Bellboat reporting."""
    from copy import copy
    from openpyxl import load_workbook
    from openpyxl.chart import BarChart, DoughnutChart, Reference
    from openpyxl.chart.label import DataLabelList
    from openpyxl.chart.marker import DataPoint
    from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
    from openpyxl.worksheet.table import Table, TableStyleInfo

    magenta = "C2185B"
    pink = "E91E63"
    pale_pink = "FCE4EC"
    yellow = "FFC107"
    orange = "F59E0B"
    dark = "2B2B2B"
    grey = "6B7280"
    white = "FFFFFF"
    light_grey = "F3F4F6"
    thin_grey = Side(style="thin", color="D1D5DB")

    wb = load_workbook(input_path)

    def title_band(ws, title, subtitle, end_col):
        ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=end_col)
        ws["A1"] = "SAHELI HUB  |  " + title
        ws["A1"].font = Font(name="Aptos Display", size=20, bold=True, color=white)
        ws["A1"].fill = PatternFill("solid", fgColor=magenta)
        ws["A1"].alignment = Alignment(vertical="center")
        ws.row_dimensions[1].height = 34
        ws.merge_cells(start_row=2, start_column=1, end_row=2, end_column=end_col)
        ws["A2"] = subtitle
        ws["A2"].font = Font(name="Aptos", size=10, color=dark, italic=True)
        ws["A2"].fill = PatternFill("solid", fgColor=yellow)
        ws["A2"].alignment = Alignment(vertical="center")
        ws.row_dimensions[2].height = 22
        ws.sheet_view.showGridLines = False

    def style_header(ws, row, start_col, end_col):
        for col in range(start_col, end_col + 1):
            cell = ws.cell(row, col)
            cell.font = Font(name="Aptos", bold=True, color=white)
            cell.fill = PatternFill("solid", fgColor=magenta)
            cell.alignment = Alignment(vertical="center", wrap_text=True)
            cell.border = Border(bottom=Side(style="medium", color=yellow))
        ws.row_dimensions[row].height = 28

    def section_label(ws, cell_ref, text, end_col=None):
        cell = ws[cell_ref]
        cell.value = text
        if end_col:
            ws.merge_cells(
                start_row=cell.row, start_column=cell.column,
                end_row=cell.row, end_column=end_col,
            )
        cell.font = Font(name="Aptos Display", size=12, bold=True, color=white)
        cell.fill = PatternFill("solid", fgColor=magenta)
        cell.alignment = Alignment(vertical="center")
        ws.row_dimensions[cell.row].height = 24

    def style_chart(chart, title, horizontal=False):
        chart.title = title
        chart.style = 10
        chart.height = 7.5
        chart.width = 12.5
        chart.legend = None if horizontal else chart.legend
        chart.graphical_properties = None
        if chart.series:
            chart.series[0].graphicalProperties.solidFill = magenta
            chart.series[0].graphicalProperties.line.solidFill = magenta
        chart.dLbls = DataLabelList()
        chart.dLbls.showVal = True

    def color_donut(chart, colors):
        if not chart.series:
            return
        points = []
        for idx, color in enumerate(colors):
            point = DataPoint(idx=idx)
            point.graphicalProperties.solidFill = color
            point.graphicalProperties.line.solidFill = white
            points.append(point)
        chart.series[0].dPt = points

    def add_table(ws, ref, name, style="TableStyleMedium2"):
        if not any(t.ref == ref for t in ws.tables.values()):
            table = Table(displayName=name, ref=ref)
            table.tableStyleInfo = TableStyleInfo(
                name=style, showFirstColumn=False, showLastColumn=False,
                showRowStripes=True, showColumnStripes=False,
            )
            ws.add_table(table)

    # SUMMARY dashboard -------------------------------------------------
    ws = wb["SUMMARY"]
    source_values = {
        ws.cell(r, 1).value: ws.cell(r, 2).value
        for r in range(2, ws.max_row + 1)
    }
    last_values = {
        ws.cell(r, 1).value: ws.cell(r, 3).value
        for r in range(2, ws.max_row + 1)
    }
    ws.delete_rows(1, ws.max_row)
    title_band(
        ws, "ANNUAL REPORT 2025/26",
        "Verified delivery, reach and registration overview  |  1 April 2025 – 31 March 2026",
        16,
    )
    kpis = [
        ("Total verified attendance", source_values["Total verified annual attendance"]),
        ("CRM attendance", source_values["CRM attendance portion"]),
        ("Verified source-only", source_values["Verified source-only additional attendance"]),
        ("Registered participants", source_values["REGISTERED PARTICIPANTS"]),
        ("New FULL registrations", source_values["New FULL registrations"]),
        ("Sessions delivered", source_values["Sessions delivered"]),
    ]
    card_positions = [(1, 4), (6, 4), (11, 4), (1, 8), (6, 8), (11, 8)]
    for (label, value), (col, row) in zip(kpis, card_positions):
        ws.merge_cells(start_row=row, start_column=col, end_row=row, end_column=col + 3)
        ws.merge_cells(start_row=row + 1, start_column=col, end_row=row + 2, end_column=col + 3)
        label_cell = ws.cell(row, col)
        value_cell = ws.cell(row + 1, col)
        label_cell.value = label
        value_cell.value = value
        label_cell.fill = PatternFill("solid", fgColor=magenta)
        label_cell.font = Font(name="Aptos", bold=True, color=white, size=10)
        label_cell.alignment = Alignment(horizontal="center", vertical="center")
        value_cell.fill = PatternFill("solid", fgColor=pale_pink)
        value_cell.font = Font(name="Aptos Display", bold=True, color=magenta, size=24)
        value_cell.number_format = "#,##0"
        value_cell.alignment = Alignment(horizontal="center", vertical="center")
        for r in range(row, row + 3):
            for c in range(col, col + 4):
                ws.cell(r, c).border = Border(
                    left=thin_grey, right=thin_grey, top=thin_grey, bottom=thin_grey
                )

    section_label(ws, "A13", "CURRENT YEAR VS PREVIOUS PUBLISHED FIGURES", 4)
    comparison = [
        ["Metric", "2025/26", "Previous published", "Difference"],
        ["Attendance", source_values["Total verified annual attendance"], 21777,
         source_values["Total verified annual attendance"] - 21777],
        ["Registered participants*", source_values["REGISTERED PARTICIPANTS"], 1897,
         source_values["REGISTERED PARTICIPANTS"] - 1897],
        ["New registrations", source_values["New FULL registrations"], 598,
         source_values["New FULL registrations"] - 598],
    ]
    for r_idx, row in enumerate(comparison, 14):
        for c_idx, value in enumerate(row, 1):
            ws.cell(r_idx, c_idx, value)
    style_header(ws, 14, 1, 4)
    ws["A18"] = "*Current registered participants are FULL + Lite CRM profiles; the prior published participant definition may differ."
    ws.merge_cells("A18:D19")
    ws["A18"].alignment = Alignment(wrap_text=True, vertical="top")
    ws["A18"].font = Font(name="Aptos", size=9, italic=True, color=grey)

    section_label(ws, "A21", "ATTENDANCE EVIDENCE", 2)
    evidence = [
        ["Evidence", "Attendances"],
        ["CRM attendance", source_values["CRM attendance portion"]],
        ["Verified source-only", source_values["Verified source-only additional attendance"]],
    ]
    for r_idx, row in enumerate(evidence, 22):
        for c_idx, value in enumerate(row, 1):
            ws.cell(r_idx, c_idx, value)
    style_header(ws, 22, 1, 2)
    ws["A27"] = "Bellboat: 114 documented working participation records are reported separately and are not added to the organisation-wide total."
    ws.merge_cells("A27:P28")
    ws["A27"].fill = PatternFill("solid", fgColor=yellow)
    ws["A27"].font = Font(name="Aptos", bold=True, color=dark)
    ws["A27"].alignment = Alignment(wrap_text=True, vertical="center")

    chart = BarChart()
    chart.type = "col"
    chart.add_data(Reference(ws, min_col=2, max_col=3, min_row=14, max_row=17), titles_from_data=True)
    chart.set_categories(Reference(ws, min_col=1, min_row=15, max_row=17))
    style_chart(chart, "Current year vs previous published figures")
    chart.series[0].graphicalProperties.solidFill = magenta
    chart.series[1].graphicalProperties.solidFill = yellow
    chart.legend.position = "b"
    ws.add_chart(chart, "F13")

    donut = DoughnutChart()
    donut.add_data(Reference(ws, min_col=2, min_row=22, max_row=24), titles_from_data=True)
    donut.set_categories(Reference(ws, min_col=1, min_row=23, max_row=24))
    donut.holeSize = 58
    style_chart(donut, "How verified attendance was constructed")
    donut.legend.position = "b"
    color_donut(donut, [magenta, yellow])
    ws.add_chart(donut, "L13")
    ws.freeze_panes = "A4"
    for col in range(1, 17):
        ws.column_dimensions[ws.cell(1, col).column_letter].width = 13
    ws.column_dimensions["A"].width = 29

    # DEMOGRAPHICS ------------------------------------------------------
    ws = wb["DEMOGRAPHICS"]
    ws.insert_rows(1, 4)
    title_band(ws, "DEMOGRAPHICS", "Canonical annual attendees; missing values remain explicitly recorded", 15)
    section_label(ws, "A4", "GENDER", 3)
    gender_header = 6
    style_header(ws, gender_header, 1, 3)
    section_label(ws, "A13", "RECORDED FEMALE / MALE", 2)
    style_header(ws, 14, 1, 2)
    section_label(ws, "A18", "AGE PROFILE", 4)
    style_header(ws, 19, 1, 4)
    gender_chart = DoughnutChart()
    gender_chart.add_data(Reference(ws, min_col=2, min_row=6, max_row=10), titles_from_data=True)
    gender_chart.set_categories(Reference(ws, min_col=1, min_row=7, max_row=10))
    gender_chart.holeSize = 58
    style_chart(gender_chart, "Gender of annual attendees")
    gender_chart.legend.position = "b"
    color_donut(gender_chart, [magenta, yellow, orange, "BDBDBD"])
    ws.add_chart(gender_chart, "F4")

    age_chart = BarChart()
    age_chart.type = "bar"
    age_chart.add_data(Reference(ws, min_col=4, min_row=19, max_row=27), titles_from_data=True)
    age_chart.set_categories(Reference(ws, min_col=1, min_row=20, max_row=27))
    style_chart(age_chart, "Age profile (% of participants with valid DOB)", horizontal=True)
    age_chart.x_axis.numFmt = "0%"
    age_chart.height = 8.5
    ws.add_chart(age_chart, "F19")

    # Find ethnicity table after row insertion by its header text.
    eth_header = next(
        cell.row for row in ws.iter_rows() for cell in row
        if cell.value == "Ethnicity Group"
    )
    section_label(ws, f"A{eth_header - 1}", "CLEANED ETHNICITY", 3)
    style_header(ws, eth_header, 1, 3)
    eth_end = eth_header
    while eth_end + 1 <= ws.max_row and ws.cell(eth_end + 1, 1).value not in (None, ""):
        eth_end += 1
    eth_chart = DoughnutChart()
    eth_chart.add_data(Reference(ws, min_col=2, min_row=eth_header, max_row=eth_end), titles_from_data=True)
    eth_chart.set_categories(Reference(ws, min_col=1, min_row=eth_header + 1, max_row=eth_end))
    eth_chart.holeSize = 58
    style_chart(eth_chart, "Cleaned ethnicity groups")
    eth_chart.legend.position = "r"
    color_donut(eth_chart, [magenta, pink, yellow, orange, "8E24AA", "6D4C41", "BDBDBD", "E0E0E0"])
    ws.add_chart(eth_chart, f"F{eth_header - 1}")
    headline_row = eth_end + 2
    ws.cell(headline_row, 1, "97% ethnically diverse")
    ws.merge_cells(start_row=headline_row, start_column=1, end_row=headline_row, end_column=3)
    ws.cell(headline_row, 1).font = Font(name="Aptos Display", size=16, bold=True, color=magenta)
    ws.cell(headline_row, 1).fill = PatternFill("solid", fgColor=pale_pink)
    ws.cell(headline_row, 1).alignment = Alignment(horizontal="center")
    ws.cell(headline_row + 1, 1, "Based on usable recorded ethnicity.")
    ws.merge_cells(start_row=headline_row + 1, start_column=1, end_row=headline_row + 1, end_column=3)
    ws.cell(headline_row + 1, 1).alignment = Alignment(horizontal="center")
    ws.cell(headline_row + 3, 1, "IMD data not available from current loaded sources.")
    ws.merge_cells(start_row=headline_row + 3, start_column=1, end_row=headline_row + 3, end_column=4)
    ws.cell(headline_row + 3, 1).font = Font(italic=True, color=grey)
    ws.freeze_panes = "A6"
    ws.column_dimensions["A"].width = 34
    ws.column_dimensions["B"].width = 16
    ws.column_dimensions["C"].width = 22
    ws.column_dimensions["D"].width = 28

    # TOP ACTIVITIES ----------------------------------------------------
    ws = wb["TOP ACTIVITIES"]
    ws.insert_rows(1, 4)
    title_band(ws, "TOP ACTIVITIES", "Top 10 activities from the combined verified-delivery ledger", 16)
    style_header(ws, 5, 1, 4)
    activity_chart = BarChart()
    activity_chart.type = "bar"
    activity_chart.add_data(Reference(ws, min_col=2, min_row=5, max_row=15), titles_from_data=True)
    activity_chart.set_categories(Reference(ws, min_col=1, min_row=6, max_row=15))
    style_chart(activity_chart, "Top 10 activities by verified attendance", horizontal=True)
    activity_chart.height = 10
    activity_chart.width = 16
    ws.add_chart(activity_chart, "F5")
    ws["A18"] = "Bellboat working participation is reported on the BELLBOAT sheet and is not silently added to verified attendance."
    ws.merge_cells("A18:D20")
    ws["A18"].fill = PatternFill("solid", fgColor=yellow)
    ws["A18"].alignment = Alignment(wrap_text=True, vertical="center")
    ws.freeze_panes = "A6"
    ws.column_dimensions["A"].width = 38

    # REGISTRATION INSIGHTS -------------------------------------------
    ws = wb["REGISTRATION INSIGHTS"]
    ws.insert_rows(1, 4)
    title_band(ws, "REGISTRATION INSIGHTS", "Why new FULL members joined and how they heard about Saheli", 15)
    section_label(ws, "A4", "TOP REASONS PEOPLE JOIN", 3)
    style_header(ws, 6, 1, 3)
    heard_header = next(
        cell.row for row in ws.iter_rows() for cell in row
        if cell.value == "Source"
    )
    section_label(ws, f"A{heard_header - 1}", "HOW PEOPLE HEARD ABOUT SAHELI", 3)
    style_header(ws, heard_header, 1, 3)
    reason_end = heard_header - 3
    reason_chart = BarChart()
    reason_chart.type = "bar"
    reason_chart.add_data(Reference(ws, min_col=3, min_row=6, max_row=reason_end), titles_from_data=True)
    reason_chart.set_categories(Reference(ws, min_col=1, min_row=7, max_row=reason_end))
    style_chart(reason_chart, "Top reasons people join", horizontal=True)
    reason_chart.x_axis.numFmt = "0%"
    reason_chart.height = 9
    ws.add_chart(reason_chart, "E5")
    heard_end = heard_header
    while heard_end + 1 <= ws.max_row and ws.cell(heard_end + 1, 1).value not in (None, ""):
        heard_end += 1
    heard_chart = DoughnutChart()
    heard_chart.add_data(Reference(ws, min_col=2, min_row=heard_header, max_row=heard_end), titles_from_data=True)
    heard_chart.set_categories(Reference(ws, min_col=1, min_row=heard_header + 1, max_row=heard_end))
    heard_chart.holeSize = 58
    style_chart(heard_chart, "How people heard about Saheli")
    heard_chart.legend.position = "r"
    color_donut(heard_chart, [magenta, pink, yellow, orange, "8E24AA", "6D4C41", "BDBDBD"])
    ws.add_chart(heard_chart, f"E{heard_header - 1}")
    ws.freeze_panes = "A6"
    ws.column_dimensions["A"].width = 42

    # BELLBOAT ---------------------------------------------------------
    if "BELLBOAT" in wb.sheetnames:
        del wb["BELLBOAT"]
    ws = wb.create_sheet("BELLBOAT", 4)
    title_band(
        ws, "BELLBOAT REPORT",
        "Documented working participation — data available from current sources",
        15,
    )
    bell_kpis = [
        ("Working participation", 114), ("Unique named participants", 83),
        ("Sessions / events", 11), ("Unique detailed profiles", 52),
    ]
    for (label, value), col in zip(bell_kpis, (1, 5, 9, 13)):
        ws.merge_cells(start_row=4, start_column=col, end_row=4, end_column=col + 2)
        ws.merge_cells(start_row=5, start_column=col, end_row=6, end_column=col + 2)
        ws.cell(4, col, label)
        ws.cell(5, col, value)
        ws.cell(4, col).fill = PatternFill("solid", fgColor=magenta)
        ws.cell(4, col).font = Font(bold=True, color=white)
        ws.cell(4, col).alignment = Alignment(horizontal="center")
        ws.cell(5, col).fill = PatternFill("solid", fgColor=pale_pink)
        ws.cell(5, col).font = Font(size=22, bold=True, color=magenta)
        ws.cell(5, col).alignment = Alignment(horizontal="center", vertical="center")
        ws.cell(5, col).number_format = "#,##0"
    note = (
        "Bellboat working participation is based on documented delivery records. "
        "The source file does not contain a separate Attended Yes/No field, so the "
        "114 participation figure remains a documented working participation total."
    )
    ws.merge_cells("A8:O10")
    ws["A8"] = note
    ws["A8"].fill = PatternFill("solid", fgColor=yellow)
    ws["A8"].font = Font(bold=True, color=dark)
    ws["A8"].alignment = Alignment(wrap_text=True, vertical="center")

    monthly = [("Month", "Participation"), ("May 2025", 46), ("Jun 2025", 8),
               ("Jul 2025", 10), ("Sep 2025", 50)]
    delivery = [("Delivery type", "Participation", "Sessions"),
                ("Bellboating", 73, 9),
                ("Bellboating & Kayaking Combined", 36, 1), ("Kayaking", 5, 1)]
    bell_gender = [("Gender", "Profiles"), ("Female", 46), ("Male", 6)]
    bell_age = [("Age band", "Profiles"), ("Under 16", 15), ("16-25", 6),
                ("26-35", 4), ("36-45", 5), ("46-55", 8), ("56-65", 2),
                ("66-75", 1), ("Unknown", 11)]
    for start_row, rows in ((13, monthly), (29, delivery), (44, bell_gender), (58, bell_age)):
        for r_off, row in enumerate(rows):
            for c_off, value in enumerate(row):
                ws.cell(start_row + r_off, 1 + c_off, value)
        style_header(ws, start_row, 1, len(rows[0]))

    chart = BarChart()
    chart.type = "col"
    chart.add_data(Reference(ws, min_col=2, min_row=13, max_row=17), titles_from_data=True)
    chart.set_categories(Reference(ws, min_col=1, min_row=14, max_row=17))
    style_chart(chart, "Bellboat participation by month")
    ws.add_chart(chart, "D13")
    chart = BarChart()
    chart.type = "bar"
    chart.add_data(Reference(ws, min_col=2, min_row=29, max_row=32), titles_from_data=True)
    chart.set_categories(Reference(ws, min_col=1, min_row=30, max_row=32))
    style_chart(chart, "Bellboat and kayaking delivery", horizontal=True)
    ws.add_chart(chart, "E28")
    chart = DoughnutChart()
    chart.add_data(Reference(ws, min_col=2, min_row=44, max_row=46), titles_from_data=True)
    chart.set_categories(Reference(ws, min_col=1, min_row=45, max_row=46))
    chart.holeSize = 58
    style_chart(chart, "Bellboat profile gender")
    chart.legend.position = "b"
    color_donut(chart, [magenta, yellow])
    ws.add_chart(chart, "D43")
    chart = BarChart()
    chart.type = "bar"
    chart.add_data(Reference(ws, min_col=2, min_row=58, max_row=66), titles_from_data=True)
    chart.set_categories(Reference(ws, min_col=1, min_row=59, max_row=66))
    style_chart(chart, "Bellboat age profile", horizontal=True)
    ws.add_chart(chart, "D57")
    ws["A69"] = "The Bellboat 114 is not included in the organisation-wide verified attendance total because source-only reconciliation is not proven."
    ws.merge_cells("A69:O70")
    ws["A69"].font = Font(italic=True, color=grey)
    ws["A69"].alignment = Alignment(wrap_text=True)
    ws.freeze_panes = "A4"
    ws.column_dimensions["A"].width = 38
    ws.column_dimensions["B"].width = 18
    ws.column_dimensions["C"].width = 14
    for col in range(4, 16):
        ws.column_dimensions[ws.cell(1, col).column_letter].width = 12

    # AUDIT and raw sheets --------------------------------------------
    audit = wb["DELIVERY SOURCE AUDIT"]
    audit.insert_rows(1, 4)
    title_band(
        audit, "DELIVERY SOURCE AUDIT",
        "Inspected source workbooks, reconciliation classifications and row-level evidence",
        18,
    )
    audit.freeze_panes = "A6"
    for row in audit.iter_rows():
        if row[0].value in {"Source", "Classification"}:
            style_header(audit, row[0].row, 1, 18 if row[0].value == "Source" else 2)
    audit.column_dimensions["A"].width = 22
    audit.column_dimensions["B"].width = 42
    audit.column_dimensions["C"].width = 30
    audit.column_dimensions["D"].width = 22
    for col in (17, 18):
        letter = audit.cell(1, col).column_letter
        audit.column_dimensions[letter].width = 52
    for row in audit.iter_rows(min_row=5, max_row=min(audit.max_row, 250)):
        for cell in row:
            cell.alignment = copy(cell.alignment)
            cell.alignment = Alignment(
                horizontal=cell.alignment.horizontal,
                vertical="top", wrap_text=cell.column in (2, 3, 17, 18),
            )

    for sheet_name, table_name in (
        ("RAW VERIFIED DELIVERY", "RawVerifiedDeliveryTable"),
        ("RAW PARTICIPANTS", "RawParticipantsTable"),
    ):
        raw = wb[sheet_name]
        raw.insert_rows(1, 3)
        title_band(raw, sheet_name.replace("RAW ", "RAW DATA — "),
                   "Filtered supporting data for authorised reporting use", raw.max_column)
        style_header(raw, 4, 1, raw.max_column)
        add_table(raw, f"A4:{raw.cell(raw.max_row, raw.max_column).coordinate}", table_name)
        raw.freeze_panes = "A5"
        raw.auto_filter.ref = f"A4:{raw.cell(raw.max_row, raw.max_column).coordinate}"
        for col in range(1, raw.max_column + 1):
            header = str(raw.cell(4, col).value or "")
            width = 16
            if any(x in header for x in ("Evidence", "Name", "Activity", "Location")):
                width = 28
            if header in ("AttendanceEvidence",):
                width = 45
            raw.column_dimensions[raw.cell(4, col).column_letter].width = width
        for row in raw.iter_rows(min_row=5, max_row=min(raw.max_row, 300)):
            for cell in row:
                if isinstance(cell.value, (date, datetime, pd.Timestamp)):
                    cell.number_format = "DD/MM/YYYY"

    # Consistent print/page settings for presentation sheets.
    for sheet_name in ("SUMMARY", "DEMOGRAPHICS", "TOP ACTIVITIES", "REGISTRATION INSIGHTS", "BELLBOAT"):
        sheet = wb[sheet_name]
        sheet.sheet_properties.pageSetUpPr.fitToPage = True
        sheet.page_setup.fitToWidth = 1
        sheet.page_setup.fitToHeight = 0
        sheet.page_margins.left = 0.25
        sheet.page_margins.right = 0.25
        sheet.page_margins.top = 0.4
        sheet.page_margins.bottom = 0.4

    wb.save(output_path)


# ============================================================
# MAIN
# ============================================================

def main():
    fail_if_placeholder_connection_string()

    print("Connecting to Saheli CRM...")
    conn = pyodbc.connect(CONNECTION_STRING)
    print("Connected.")

    try:
        print("Loading core data...")
        sessions, attendance, participants, lite = load_data(conn)

        print("Building canonical FULL/Lite participant mapping...")
        (
            annual,
            canonical_people,
            resolved_lite_to_full,
            conflicting_lite,
            evidence_candidates,
        ) = build_canonical_data(
            sessions, attendance, participants, lite
        )

        print("Calculating annual report summaries...")
        summaries = make_summaries(
            conn,
            sessions,
            annual,
            participants,
            lite,
            canonical_people,
            resolved_lite_to_full,
            conflicting_lite,
        )

        print("Opening original migration workbooks and attendance evidence...")
        source_rows, inspection_audit, verified_files, verified_sheets = (
            load_migration_source_evidence()
        )
        print("Reconciling source attendance against dbo.SessionAttendance...")
        reconciled_sources = reconcile_source_evidence(annual, source_rows)
        verified_delivery = build_verified_delivery_ledger(
            annual, reconciled_sources
        )

        print(f"Writing {OUTPUT_XLSX} ...")
        write_complete_excel(
            summaries,
            verified_delivery,
            reconciled_sources,
            inspection_audit,
            canonical_people,
            participants,
            lite,
        )
        print("Applying Saheli branding, dashboard charts and Bellboat report...")
        apply_saheli_branding(OUTPUT_XLSX, BRANDED_OUTPUT_XLSX)

        already_in_crm = int(
            (reconciled_sources["Classification"] == "CRM_CONFIRMED").sum()
        )
        source_only = int(
            (reconciled_sources["Classification"] == "SOURCE_ONLY_VERIFIED").sum()
        )
        duplicates = int(
            (reconciled_sources["Classification"] == "DUPLICATE_SOURCE").sum()
        )
        review = int((reconciled_sources["Classification"] == "REVIEW").sum())
        combined = len(annual) + source_only
        print("\n============================================================")
        print("VERIFIED DELIVERY RECONCILIATION")
        print("============================================================")
        print(f"CRM attendance: {len(annual):,}")
        print(f"Verified source-only attendance: {source_only:,}")
        print(f"Combined verified attendance: {combined:,}")
        print(f"Difference vs 21,777: {combined - 21777:+,}")
        print(f"\nFULL registered: {len(participants):,}")
        print(f"Lite registered: {len(lite):,}")
        print(f"FULL + Lite registered: {len(participants) + len(lite):,}")
        print(f"\nNew FULL registrations: {len(summaries['New FULL Registrations']):,}")
        print(f"\nFiles inspected: {len(verified_files):,}")
        print(f"Sheets inspected: {len(verified_sheets):,}")
        print(f"Source attendance rows checked: {len(reconciled_sources):,}")
        print(f"Rows already in CRM: {already_in_crm:,}")
        print(f"Verified source-only rows: {source_only:,}")
        print(f"Duplicates excluded: {duplicates:,}")
        print(f"Rows requiring review: {review:,}")
        print(f"\nExcel saved to: {BRANDED_OUTPUT_XLSX.resolve()}")
        if EXPORT_RAW_SENSITIVE_DATA:
            print(
                "WARNING: workbook contains sensitive raw participant data."
            )

    finally:
        conn.close()


if __name__ == "__main__":
    main()
