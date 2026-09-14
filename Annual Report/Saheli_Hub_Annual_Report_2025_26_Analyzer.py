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
from datetime import date
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

OUTPUT_XLSX = Path("Saheli_Hub_Annual_Report_2025_26_Analysis.xlsx")

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
        sessions[["SessionId", "SessionDate", "VenueName", "ActivityName"]],
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
    }


# ============================================================
# EXTRA-SOURCE AUDIT
# ============================================================

def source_audit(conn) -> pd.DataFrame:
    rows = []

    def add_row(source, total_rows=None, period_rows=None, note=""):
        rows.append(
            {
                "Source": source,
                "TotalRows": total_rows,
                "RowsIn2025_26": period_rows,
                "Note": note,
            }
        )

    # SessionAttendance
    total = sql_df(
        conn,
        "SELECT COUNT(*) AS N FROM dbo.SessionAttendance"
    ).iloc[0]["N"]
    period = sql_df(
        conn,
        """
        SELECT COUNT(*) AS N
        FROM dbo.SessionAttendance sa
        INNER JOIN dbo.Sessions s
            ON s.SessionId = sa.SessionId
        WHERE s.SessionDate >= ?
          AND s.SessionDate < ?
          AND sa.Attended = 1
        """,
        [REPORT_START, REPORT_END_EXCLUSIVE],
    ).iloc[0]["N"]
    add_row(
        "SessionAttendance",
        int(total),
        int(period),
        "Core confirmed attendance source",
    )

    # ActivityRegisterImport
    if table_exists(conn, "ActivityRegisterImport"):
        total = sql_df(
            conn,
            "SELECT COUNT(*) AS N FROM dbo.ActivityRegisterImport"
        ).iloc[0]["N"]
        period = sql_df(
            conn,
            """
            SELECT COUNT(*) AS N
            FROM dbo.ActivityRegisterImport
            WHERE TRY_CONVERT(date, SessionDate) >= ?
              AND TRY_CONVERT(date, SessionDate) < ?
            """,
            [REPORT_START, REPORT_END_EXCLUSIVE],
        ).iloc[0]["N"]
        add_row(
            "ActivityRegisterImport",
            int(total),
            int(period),
            "Import/source staging rows; do NOT add automatically",
        )

    # Bellboat assignments
    if table_exists(conn, "BellboatSessionAssignments"):
        total = sql_df(
            conn,
            "SELECT COUNT(*) AS N FROM dbo.BellboatSessionAssignments"
        ).iloc[0]["N"]
        period = sql_df(
            conn,
            """
            SELECT COUNT(*) AS N
            FROM dbo.BellboatSessionAssignments bsa
            INNER JOIN dbo.BellboatSessions bs
                ON bs.BellboatSessionId = bsa.BellboatSessionId
            WHERE bs.SessionDate >= ?
              AND bs.SessionDate < ?
            """,
            [REPORT_START, REPORT_END_EXCLUSIVE],
        ).iloc[0]["N"]
        add_row(
            "BellboatSessionAssignments",
            int(total),
            int(period),
            "Booking/assignment rows; verify attendance before adding",
        )

    # Cycling registrations
    if table_exists(conn, "CyclingRegistrations"):
        total = sql_df(
            conn,
            "SELECT COUNT(*) AS N FROM dbo.CyclingRegistrations"
        ).iloc[0]["N"]
        period = sql_df(
            conn,
            """
            SELECT COUNT(*) AS N
            FROM dbo.CyclingRegistrations cr
            INNER JOIN dbo.Sessions s
                ON s.SessionId = cr.SessionId
            WHERE s.SessionDate >= ?
              AND s.SessionDate < ?
            """,
            [REPORT_START, REPORT_END_EXCLUSIVE],
        ).iloc[0]["N"]
        add_row(
            "CyclingRegistrations",
            int(total),
            int(period),
            "Registration rows; not equivalent to attendance",
        )

    # Bellboat register
    if table_exists(conn, "BellboatingRegisterEntries"):
        total = sql_df(
            conn,
            "SELECT COUNT(*) AS N FROM dbo.BellboatingRegisterEntries"
        ).iloc[0]["N"]
        period = sql_df(
            conn,
            """
            SELECT COUNT(*) AS N
            FROM dbo.BellboatingRegisterEntries
            WHERE RegisterDate >= ?
              AND RegisterDate < ?
            """,
            [REPORT_START, REPORT_END_EXCLUSIVE],
        ).iloc[0]["N"]
        add_row(
            "BellboatingRegisterEntries",
            int(total),
            int(period),
            "Register rows; verify semantics before adding",
        )

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
    with pd.ExcelWriter(
        OUTPUT_XLSX,
        engine="openpyxl",
    ) as writer:

        for name, df in summaries.items():
            # Excel sheet name max = 31 chars
            sheet = name[:31]
            df.to_excel(writer, sheet_name=sheet, index=False)

        source_audit_df.to_excel(
            writer,
            sheet_name="Source Audit",
            index=False,
        )

        if imd_result is not None:
            imd_people, imd_summary = imd_result
            imd_summary.to_excel(
                writer, sheet_name="IMD Summary", index=False
            )
            if EXPORT_RAW_SENSITIVE_DATA:
                imd_people.to_excel(
                    writer, sheet_name="IMD Participant Detail", index=False
                )

        if EXPORT_RAW_SENSITIVE_DATA:
            annual_export = annual[
                [
                    "CanonicalPersonKey",
                    "AttendanceId",
                    "SessionId",
                    "SessionDate",
                    "VenueName",
                    "ActivityName",
                    "ParticipantId",
                    "LiteMemberId",
                    "MemberDisplayId",
                    "MemberName",
                    "Phone",
                    "Attended",
                ]
            ].copy()

            annual_export.to_excel(
                writer,
                sheet_name="Raw Annual Attendance",
                index=False,
            )

            canonical_people.to_excel(
                writer,
                sheet_name="Canonical Participants",
                index=False,
            )

        # Small methodology sheet
        methodology = pd.DataFrame(
            [
                [
                    "Annual period",
                    "1 April 2025 to 31 March 2026",
                ],
                [
                    "Annual attendance",
                    "Confirmed SessionAttendance rows where Attended=1",
                ],
                [
                    "Raw attendee identities",
                    "Distinct FULL and Lite attendance identities",
                ],
                [
                    "Canonical annual people",
                    "Lite identities collapse to FULL only when strong "
                    "evidence resolves to exactly one FULL record",
                ],
                [
                    "Current CRM profiles",
                    "FULL + Lite rows currently stored in CRM; this is "
                    "not the same as unique annual attendees",
                ],
                [
                    "Missing gender",
                    "Never converted to Female without evidence",
                ],
                [
                    "Raw sensitive tabs",
                    "Remove before sharing outside authorised staff",
                ],
            ],
            columns=["Topic", "Definition"],
        )
        methodology.to_excel(
            writer,
            sheet_name="Methodology",
            index=False,
        )

        # Basic presentation formatting
        wb = writer.book
        for ws in wb.worksheets:
            ws.freeze_panes = "A2"
            ws.auto_filter.ref = ws.dimensions
            for cell in ws[1]:
                cell.font = cell.font.copy(bold=True)
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

        print("Auditing possible non-SessionAttendance sources...")
        audit = source_audit(conn)

        imd_result = add_imd_if_available(canonical_people)

        print(f"Writing {OUTPUT_XLSX} ...")
        write_excel(
            summaries,
            audit,
            annual,
            canonical_people,
            imd_result,
        )

        # Console headline
        print("\n============================================================")
        print("ANNUAL REPORT HEADLINE CHECK")
        print("============================================================")
        print(f"Confirmed annual attendance: {len(annual):,}")
        print(
            "Annual attended sessions: "
            f"{annual['SessionId'].nunique():,}"
        )
        print(
            "Raw attendee identities: "
            f"{annual['raw_identity_key'].nunique():,}"
        )
        print(
            "Canonical annual people: "
            f"{canonical_people['CanonicalPersonKey'].nunique():,}"
        )
        print(f"Current FULL profiles: {len(participants):,}")
        print(f"Current Lite profiles: {len(lite):,}")
        print(
            "Current FULL + Lite profiles: "
            f"{len(participants) + len(lite):,}"
        )

        new_full = summaries["New FULL Registrations"]
        print(
            "New FULL registrations in FY: "
            f"{len(new_full):,}"
        )

        print("\nGender - canonical annual people:")
        print(
            summaries["Gender Annual"]
            .to_string(index=False)
        )

        print("\nTop 10 activities:")
        print(
            summaries["Top Activities"][
                ["Activity", "Attendances", "Sessions"]
            ].to_string(index=False)
        )

        print("\nPossible extra delivery sources:")
        print(audit.to_string(index=False))

        print(f"\nDONE: {OUTPUT_XLSX.resolve()}")
        if EXPORT_RAW_SENSITIVE_DATA:
            print(
                "WARNING: workbook contains sensitive raw participant data."
            )

    finally:
        conn.close()


if __name__ == "__main__":
    main()
