#!/usr/bin/env python3
"""
Saheli CRM - Ambiguous Source Attendance Investigation
======================================================

Purpose
-------
Investigate the rows previously classified as:

    SESSION_AMBIGUOUS_PERSON_UNRESOLVED

This is a READ-ONLY second-pass analysis. It does not migrate anything
and does not change the CRM.

The script:
1. Reads Saheli_SourceOnly_CRM_CrossCheck.xlsx
2. Reconnects to the live CRM
3. Looks at all CRM sessions on the same date
4. Scores activity / venue / time similarity
5. Checks whether the source person's name/card/phone appears in
   attendance on the candidate CRM session or elsewhere on the same date
6. Produces an Excel workbook showing where the 3,501 ambiguous rows sit

Output
------
Saheli_Ambiguous_3501_Breakdown.xlsx

Important
---------
This script is intentionally conservative. Similar names/activity names
are investigation evidence only, not proof of a duplicate attendance.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from collections import defaultdict
from difflib import SequenceMatcher

import pandas as pd
import pyodbc


# ============================================================
# CONFIG
# ============================================================

BASE = Path(__file__).resolve().parent

INPUT_XLSX = BASE / "Saheli_SourceOnly_CRM_CrossCheck.xlsx"
INPUT_SHEET = "ROW DETAIL"

OUTPUT_XLSX = BASE / "Saheli_Ambiguous_3501_Breakdown.xlsx"

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

TARGET_STATUS = "SESSION_AMBIGUOUS_PERSON_UNRESOLVED"


# ============================================================
# HELPERS
# ============================================================

def fail_if_placeholder_connection_string():
    markers = ("YOUR_SERVER", "YOUR_USERNAME", "YOUR_PASSWORD")
    if any(x in CONNECTION_STRING for x in markers):
        raise SystemExit(
            "\nPlease set SAHELI_SQL_CONNECTION_STRING or edit CONNECTION_STRING.\n"
        )

def sql_df(conn, query, params=None):
    return pd.read_sql_query(query, conn, params=params)

def clean_text(v):
    if pd.isna(v):
        return ""
    s = str(v).strip().lower()
    s = s.replace("&", " and ")
    s = re.sub(r"[^a-z0-9\s]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def compact(v):
    return re.sub(r"[^a-z0-9]+", "", clean_text(v))

def norm_phone(v):
    if pd.isna(v):
        return ""
    return re.sub(r"\D+", "", str(v))

def norm_card(v):
    if pd.isna(v):
        return ""
    return re.sub(r"\.0$", "", str(v).strip()).lower()

def parse_date(v):
    return pd.to_datetime(v, errors="coerce").normalize()

def time_key(v):
    if pd.isna(v) or v == "":
        return ""
    try:
        t = pd.to_datetime(v).time()
        return f"{t.hour:02d}:{t.minute:02d}"
    except Exception:
        s = str(v).strip()
        m = re.search(r"(\d{1,2})[:\.](\d{2})", s)
        if m:
            return f"{int(m.group(1)):02d}:{int(m.group(2)):02d}"
    return ""

def token_set(s):
    return set(clean_text(s).split())

def text_similarity(a, b):
    """
    Conservative 0..1 similarity using:
    - token overlap
    - sequence similarity

    Exact normalized equality = 1.
    """
    a = clean_text(a)
    b = clean_text(b)

    if not a or not b:
        return 0.0

    if a == b:
        return 1.0

    ta = set(a.split())
    tb = set(b.split())

    union = ta | tb
    token_jaccard = (len(ta & tb) / len(union)) if union else 0.0
    sequence = SequenceMatcher(None, a, b).ratio()

    # Slightly prefer token overlap for phrases like:
    # "Chair Exercise" vs "Chair Based Exercise"
    return max(
        sequence,
        (0.65 * token_jaccard) + (0.35 * sequence)
    )

def score_session(src_activity, src_location, src_start,
                  crm_activity, crm_location, crm_start):
    """
    Weighted session similarity.
    Only fields actually present in the source contribute to denominator.
    """
    components = []
    weights = []

    if clean_text(src_activity):
        components.append(text_similarity(src_activity, crm_activity))
        weights.append(0.55)

    if clean_text(src_location):
        components.append(text_similarity(src_location, crm_location))
        weights.append(0.30)

    if time_key(src_start):
        src_t = time_key(src_start)
        crm_t = time_key(crm_start)
        if src_t and crm_t:
            components.append(1.0 if src_t == crm_t else 0.0)
            weights.append(0.15)

    if not weights:
        return 0.0

    return sum(c * w for c, w in zip(components, weights)) / sum(weights)

def unique_nonempty(values):
    return sorted({str(v) for v in values if pd.notna(v) and str(v).strip()})

def safe_sheet_name(name):
    return re.sub(r"[\[\]:*?/\\]", "_", name)[:31]


# ============================================================
# LOAD PRIOR CROSS-CHECK
# ============================================================

if not INPUT_XLSX.exists():
    raise SystemExit(
        f"Input workbook not found:\n{INPUT_XLSX}\n\n"
        "Run Saheli_SourceOnly_CRM_CrossCheck.py first."
    )

print(f"Reading prior cross-check: {INPUT_XLSX}")

df = pd.read_excel(INPUT_XLSX, sheet_name=INPUT_SHEET)

required = [
    "CrossCheckStatus", "SessionDate", "Activity",
    "Location", "ParticipantName"
]

missing = [c for c in required if c not in df.columns]
if missing:
    raise SystemExit(f"Missing required columns: {missing}")

amb = df[df["CrossCheckStatus"] == TARGET_STATUS].copy()

print(f"Ambiguous rows selected: {len(amb):,}")

if amb.empty:
    raise SystemExit(f"No rows found with status {TARGET_STATUS}.")


# ============================================================
# CONNECT TO CRM
# ============================================================

fail_if_placeholder_connection_string()

print("Connecting to live CRM...")
conn = pyodbc.connect(CONNECTION_STRING)
print("Connected.")

sessions = sql_df(
    conn,
    """
    SELECT
        SessionId,
        SessionDate,
        VenueName,
        ActivityName,
        StartTime,
        EndTime
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
        sa.AttendanceId,
        sa.SessionId,
        sa.ParticipantId,
        sa.LiteMemberId,
        sa.SaheliCardNumber,
        sa.MemberDisplayId,
        sa.MemberName,
        sa.Phone,
        sa.Attended,
        s.SessionDate,
        s.VenueName,
        s.ActivityName,
        s.StartTime
    FROM dbo.SessionAttendance sa
    INNER JOIN dbo.Sessions s
        ON s.SessionId = sa.SessionId
    WHERE s.SessionDate >= ?
      AND s.SessionDate < ?
      AND sa.Attended = 1
    """,
    [REPORT_START, REPORT_END_EXCLUSIVE],
)

conn.close()


# ============================================================
# NORMALISE CRM DATA
# ============================================================

sessions["date_key"] = pd.to_datetime(
    sessions["SessionDate"], errors="coerce"
).dt.normalize()

sessions["activity_clean"] = sessions["ActivityName"].map(clean_text)
sessions["location_clean"] = sessions["VenueName"].map(clean_text)
sessions["start_key"] = sessions["StartTime"].map(time_key)

attendance["date_key"] = pd.to_datetime(
    attendance["SessionDate"], errors="coerce"
).dt.normalize()

attendance["name_clean"] = attendance["MemberName"].map(compact)
attendance["phone_clean"] = attendance["Phone"].map(norm_phone)
attendance["card_clean"] = attendance["SaheliCardNumber"].map(norm_card)

sessions_by_date = {
    d: g.copy()
    for d, g in sessions.groupby("date_key")
    if pd.notna(d)
}

att_by_date = {
    d: g.copy()
    for d, g in attendance.groupby("date_key")
    if pd.notna(d)
}

att_by_session = {
    int(sid): g.copy()
    for sid, g in attendance.groupby("SessionId")
}


# ============================================================
# SECOND PASS
# ============================================================

rows = []

for idx, r in amb.iterrows():

    d = parse_date(r["SessionDate"])

    src_activity = r.get("Activity", "")
    src_location = r.get("Location", "")
    src_start = r.get("StartTime", "")

    src_name = r.get("ParticipantName", "")
    src_card = r.get("SaheliCardNumber", "")
    src_phone = r.get("Phone", "")

    src_name_key = compact(src_name)
    src_card_key = norm_card(src_card)
    src_phone_key = norm_phone(src_phone)

    same_date_sessions = sessions_by_date.get(d, pd.DataFrame()).copy()

    candidate_rows = []

    if not same_date_sessions.empty:
        for _, s in same_date_sessions.iterrows():
            score = score_session(
                src_activity,
                src_location,
                src_start,
                s["ActivityName"],
                s["VenueName"],
                s["StartTime"],
            )

            candidate_rows.append({
                "SessionId": int(s["SessionId"]),
                "ActivityName": s["ActivityName"],
                "VenueName": s["VenueName"],
                "StartTime": s["StartTime"],
                "Score": score,
                "ActivitySimilarity": text_similarity(
                    src_activity, s["ActivityName"]
                ) if clean_text(src_activity) else None,
                "VenueSimilarity": text_similarity(
                    src_location, s["VenueName"]
                ) if clean_text(src_location) else None,
                "ExactStart": (
                    bool(time_key(src_start))
                    and time_key(src_start) == time_key(s["StartTime"])
                ),
            })

    candidate_rows = sorted(
        candidate_rows,
        key=lambda x: x["Score"],
        reverse=True
    )

    top = candidate_rows[0] if candidate_rows else None
    second = candidate_rows[1] if len(candidate_rows) > 1 else None

    top_score = top["Score"] if top else 0.0
    second_score = second["Score"] if second else 0.0
    margin = top_score - second_score

    # Strong unique candidate:
    # > .80 and meaningfully better than second option
    strong_unique_session = (
        top is not None
        and top_score >= 0.80
        and (len(candidate_rows) == 1 or margin >= 0.12)
    )

    top_session_id = top["SessionId"] if top else None
    top_att = (
        att_by_session.get(top_session_id, pd.DataFrame()).copy()
        if top_session_id is not None
        else pd.DataFrame()
    )

    # --------------------------------------------------------
    # Find person in top candidate session
    # --------------------------------------------------------

    top_person_match_type = ""
    top_person_match_rows = 0

    if not top_att.empty:

        masks = []

        if src_card_key:
            m = top_att["card_clean"] == src_card_key
            if m.any():
                top_person_match_type = "EXACT CARD"
                top_person_match_rows = int(m.sum())

        if not top_person_match_type and len(src_phone_key) >= 10:
            m = top_att["phone_clean"] == src_phone_key
            if m.any():
                top_person_match_type = "EXACT PHONE"
                top_person_match_rows = int(m.sum())

        if not top_person_match_type and src_name_key:
            m = top_att["name_clean"] == src_name_key
            if m.any():
                top_person_match_type = "EXACT NAME"
                top_person_match_rows = int(m.sum())

    # --------------------------------------------------------
    # Find same person anywhere in CRM attendance on same date
    # --------------------------------------------------------

    day_att = att_by_date.get(d, pd.DataFrame()).copy()

    same_day_match_type = ""
    same_day_match_count = 0
    same_day_session_ids = []

    if not day_att.empty:

        if src_card_key:
            m = day_att["card_clean"] == src_card_key
            if m.any():
                same_day_match_type = "EXACT CARD"
                same_day_match_count = int(m.sum())
                same_day_session_ids = unique_nonempty(
                    day_att.loc[m, "SessionId"]
                )

        if not same_day_match_type and len(src_phone_key) >= 10:
            m = day_att["phone_clean"] == src_phone_key
            if m.any():
                same_day_match_type = "EXACT PHONE"
                same_day_match_count = int(m.sum())
                same_day_session_ids = unique_nonempty(
                    day_att.loc[m, "SessionId"]
                )

        if not same_day_match_type and src_name_key:
            m = day_att["name_clean"] == src_name_key
            if m.any():
                same_day_match_type = "EXACT NAME"
                same_day_match_count = int(m.sum())
                same_day_session_ids = unique_nonempty(
                    day_att.loc[m, "SessionId"]
                )

    # --------------------------------------------------------
    # Investigation classification
    # --------------------------------------------------------

    # Exact card / phone are strongest.
    if (
        strong_unique_session
        and top_person_match_type in {"EXACT CARD", "EXACT PHONE"}
    ):
        investigation_status = "STRONG_ALREADY_IN_CRM"

    # Exact same name + strong session = likely duplicate but review.
    elif (
        strong_unique_session
        and top_person_match_type == "EXACT NAME"
    ):
        investigation_status = "LIKELY_ALREADY_IN_CRM_REVIEW"

    # Same person appears elsewhere on the same date.
    elif same_day_match_type in {"EXACT CARD", "EXACT PHONE"}:
        investigation_status = "PERSON_ATTENDED_OTHER_SESSION_SAME_DAY"

    elif same_day_match_type == "EXACT NAME":
        investigation_status = "NAME_FOUND_OTHER_SESSION_SAME_DAY_REVIEW"

    # Session can be identified well but person isn't in it.
    elif strong_unique_session:
        investigation_status = "LIKELY_SESSION_EXISTS_ATTENDANCE_MISSING"

    # There are same-date sessions but no clear mapping.
    elif top is not None:
        investigation_status = "SESSION_STILL_AMBIGUOUS"

    else:
        investigation_status = "NO_SAME_DATE_SESSION"

    out = r.to_dict()

    out.update({
        "InvestigationStatus": investigation_status,

        "SameDateSessionCount": len(candidate_rows),

        "TopCandidateSessionId": top_session_id,
        "TopCandidateActivity": top["ActivityName"] if top else None,
        "TopCandidateVenue": top["VenueName"] if top else None,
        "TopCandidateStartTime": top["StartTime"] if top else None,

        "TopCandidateScore": round(top_score, 4),
        "SecondCandidateScore": round(second_score, 4),
        "ScoreMargin": round(margin, 4),

        "TopActivitySimilarity": (
            round(top["ActivitySimilarity"], 4)
            if top and top["ActivitySimilarity"] is not None
            else None
        ),
        "TopVenueSimilarity": (
            round(top["VenueSimilarity"], 4)
            if top and top["VenueSimilarity"] is not None
            else None
        ),
        "TopExactStart": top["ExactStart"] if top else None,

        "StrongUniqueSessionCandidate": strong_unique_session,

        "TopSessionPersonMatch": top_person_match_type,
        "TopSessionPersonMatchRows": top_person_match_rows,

        "SameDayPersonMatch": same_day_match_type,
        "SameDayPersonMatchCount": same_day_match_count,
        "SameDayMatchedSessionIds": ",".join(same_day_session_ids),

        "Top3Candidates": " || ".join(
            f"{x['SessionId']} | {x['ActivityName']} | "
            f"{x['VenueName']} | {x['StartTime']} | {x['Score']:.3f}"
            for x in candidate_rows[:3]
        )
    })

    rows.append(out)

result = pd.DataFrame(rows)


# ============================================================
# SUMMARY TABLES
# ============================================================

summary = (
    result["InvestigationStatus"]
    .value_counts()
    .rename_axis("InvestigationStatus")
    .reset_index(name="Rows")
)

summary["Percentage"] = (
    100 * summary["Rows"] / len(result)
).round(2)


# Source file / activity / location breakdown
source_group_cols = [
    c for c in ["SourceFile", "SourceSheet", "Activity", "Location"]
    if c in result.columns
]

by_source = (
    result.groupby(
        source_group_cols + ["InvestigationStatus"],
        dropna=False
    )
    .size()
    .reset_index(name="Rows")
    .sort_values("Rows", ascending=False)
)


# Source activity -> best CRM activity mapping
activity_map = (
    result.groupby(
        ["Activity", "TopCandidateActivity", "InvestigationStatus"],
        dropna=False
    )
    .agg(
        Rows=("InvestigationStatus", "size"),
        AvgScore=("TopCandidateScore", "mean"),
    )
    .reset_index()
    .sort_values(["Rows", "AvgScore"], ascending=[False, False])
)

activity_map["AvgScore"] = activity_map["AvgScore"].round(3)


# Source venue -> best CRM venue mapping
venue_map = (
    result.groupby(
        ["Location", "TopCandidateVenue", "InvestigationStatus"],
        dropna=False
    )
    .agg(
        Rows=("InvestigationStatus", "size"),
        AvgScore=("TopCandidateScore", "mean"),
    )
    .reset_index()
    .sort_values(["Rows", "AvgScore"], ascending=[False, False])
)

venue_map["AvgScore"] = venue_map["AvgScore"].round(3)


# Month breakdown
result["Month"] = pd.to_datetime(
    result["SessionDate"], errors="coerce"
).dt.to_period("M").astype(str)

by_month = (
    result.groupby(
        ["Month", "InvestigationStatus"],
        dropna=False
    )
    .size()
    .reset_index(name="Rows")
    .sort_values(["Month", "Rows"], ascending=[True, False])
)


# High-value review rows:
review_statuses = [
    "STRONG_ALREADY_IN_CRM",
    "LIKELY_ALREADY_IN_CRM_REVIEW",
    "PERSON_ATTENDED_OTHER_SESSION_SAME_DAY",
    "NAME_FOUND_OTHER_SESSION_SAME_DAY_REVIEW",
    "LIKELY_SESSION_EXISTS_ATTENDANCE_MISSING",
]

priority_review = result[
    result["InvestigationStatus"].isin(review_statuses)
].copy()

priority_review = priority_review.sort_values(
    ["InvestigationStatus", "TopCandidateScore"],
    ascending=[True, False]
)


# ============================================================
# WRITE OUTPUT
# ============================================================

with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:

    summary.to_excel(
        writer,
        sheet_name="SUMMARY",
        index=False
    )

    by_source.to_excel(
        writer,
        sheet_name="BY SOURCE",
        index=False
    )

    activity_map.to_excel(
        writer,
        sheet_name="ACTIVITY MAP",
        index=False
    )

    venue_map.to_excel(
        writer,
        sheet_name="VENUE MAP",
        index=False
    )

    by_month.to_excel(
        writer,
        sheet_name="BY MONTH",
        index=False
    )

    priority_review.to_excel(
        writer,
        sheet_name="PRIORITY REVIEW",
        index=False
    )

    result.to_excel(
        writer,
        sheet_name="ROW DETAIL",
        index=False
    )

    # Separate sheets for key statuses
    for status in summary["InvestigationStatus"].tolist():
        subset = result[
            result["InvestigationStatus"] == status
        ]
        if not subset.empty:
            subset.to_excel(
                writer,
                sheet_name=safe_sheet_name(status),
                index=False
            )

    wb = writer.book

    for ws in wb.worksheets:
        ws.freeze_panes = "A2"
        ws.auto_filter.ref = ws.dimensions

        for cell in ws[1]:
            cell.font = cell.font.copy(bold=True)

        # Reasonable widths without scanning every row
        for col_cells in ws.columns:
            letter = col_cells[0].column_letter
            max_len = 0

            for cell in col_cells[:500]:
                value = "" if cell.value is None else str(cell.value)
                max_len = max(max_len, len(value))

            ws.column_dimensions[letter].width = min(
                max(max_len + 2, 12),
                50
            )


# ============================================================
# TERMINAL SUMMARY
# ============================================================

print("\n" + "=" * 76)
print("AMBIGUOUS 3,501 SECOND-PASS ANALYSIS COMPLETE")
print("=" * 76)
print(f"Rows investigated: {len(result):,}\n")

for _, r in summary.iterrows():
    print(
        f"{r['InvestigationStatus']}: "
        f"{int(r['Rows']):,} ({r['Percentage']:.2f}%)"
    )

strong_dup = int(
    summary.loc[
        summary["InvestigationStatus"] == "STRONG_ALREADY_IN_CRM",
        "Rows"
    ].sum()
)

likely_dup = int(
    summary.loc[
        summary["InvestigationStatus"] == "LIKELY_ALREADY_IN_CRM_REVIEW",
        "Rows"
    ].sum()
)

session_missing_att = int(
    summary.loc[
        summary["InvestigationStatus"]
        == "LIKELY_SESSION_EXISTS_ATTENDANCE_MISSING",
        "Rows"
    ].sum()
)

still_amb = int(
    summary.loc[
        summary["InvestigationStatus"] == "SESSION_STILL_AMBIGUOUS",
        "Rows"
    ].sum()
)

print("\nKey investigation totals")
print("------------------------")
print(f"Strong evidence already in CRM: {strong_dup:,}")
print(f"Likely already in CRM - review: {likely_dup:,}")
print(f"Likely existing session but attendance missing: {session_missing_att:,}")
print(f"Still session ambiguous: {still_amb:,}")

print(f"\nExcel saved to:\n{OUTPUT_XLSX}")
print("\nREAD ONLY: no CRM data was changed.")
