#!/usr/bin/env python3
"""
Saheli CRM Cross-Check: VERIFIED SOURCE-ONLY ATTENDANCE
======================================================

Purpose
-------
Re-check the rows previously classified as SOURCE_ONLY_VERIFIED against
the LIVE Saheli CRM and determine whether they are actually present under
a different participant/session identity.

READ ONLY:
- Does not INSERT / UPDATE / DELETE anything.
- Only reads the Excel evidence workbook and SQL Server.

Outputs
-------
1) Saheli_SourceOnly_CRM_CrossCheck.xlsx
2) Terminal summary with:
   - source-only rows checked
   - already in CRM
   - session exists but attendance missing
   - participant exists but session missing
   - both participant and session missing
   - unresolved / ambiguous

Dependencies
------------
pip install pandas pyodbc openpyxl
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from collections import defaultdict

import pandas as pd
import pyodbc

BASE = Path(__file__).resolve().parent

INPUT_XLSX = BASE / "Saheli_Hub_Annual_Report_2025_26_COMPLETE.xlsx"
RAW_SHEET = "RAW VERIFIED DELIVERY"
OUTPUT_XLSX = BASE / "Saheli_SourceOnly_CRM_CrossCheck.xlsx"

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


def fail_if_placeholder_connection_string():
    markers = ("YOUR_SERVER", "YOUR_USERNAME", "YOUR_PASSWORD")
    if any(x in CONNECTION_STRING for x in markers):
        raise SystemExit(
            "\nPlease set SAHELI_SQL_CONNECTION_STRING or edit CONNECTION_STRING.\n"
        )

def sql_df(conn, query, params=None):
    return pd.read_sql_query(query, conn, params=params)

def norm_text(v):
    if pd.isna(v):
        return ""
    return re.sub(r"[^a-z0-9]+", "", str(v).strip().lower())

def norm_name(v):
    return norm_text(v)

def norm_postcode(v):
    if pd.isna(v):
        return ""
    return re.sub(r"\s+", "", str(v).upper().strip())

def norm_phone(v):
    if pd.isna(v):
        return ""
    return re.sub(r"\D+", "", str(v))

def norm_card(v):
    if pd.isna(v):
        return ""
    return re.sub(r"\.0$", "", str(v).strip())

def to_date(v):
    if pd.isna(v):
        return pd.NaT
    return pd.to_datetime(v, errors="coerce").normalize()

def to_time_key(v):
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

def find_col(df, aliases, required=False):
    lookup = {str(c).strip().lower(): c for c in df.columns}
    for alias in aliases:
        if alias.lower() in lookup:
            return lookup[alias.lower()]
    for c in df.columns:
        low = str(c).lower().replace("_", " ").strip()
        for alias in aliases:
            a = alias.lower().replace("_", " ").strip()
            if a in low or low in a:
                return c
    if required:
        raise KeyError(f"Could not find any of columns: {aliases}")
    return None

def unique_or_none(items):
    items = {x for x in items if x is not None}
    return next(iter(items)) if len(items) == 1 else None


if not INPUT_XLSX.exists():
    raise SystemExit(f"Workbook not found:\n{INPUT_XLSX}")

print(f"Reading: {INPUT_XLSX}")
xl = pd.ExcelFile(INPUT_XLSX)

if RAW_SHEET not in xl.sheet_names:
    raise SystemExit(
        f"Sheet '{RAW_SHEET}' not found.\nAvailable sheets:\n" +
        "\n".join(xl.sheet_names)
    )

raw = pd.read_excel(INPUT_XLSX, sheet_name=RAW_SHEET)
raw = raw.dropna(how="all").copy()
raw = raw.loc[:, ~raw.columns.astype(str).str.startswith("Unnamed")]

print(f"Raw rows loaded: {len(raw):,}")

c_status = find_col(raw, [
    "Classification", "ReconciliationStatus", "Status",
    "CRM Status", "InCRM", "AttendanceEvidence"
])
c_source = find_col(raw, ["Source", "SourceType"])
c_source_file = find_col(raw, ["SourceFile", "Source File", "File"])
c_source_sheet = find_col(raw, ["SourceSheet", "Source Sheet", "Sheet"])
c_source_row = find_col(raw, ["SourceRow", "Source Row", "Row"])
c_location = find_col(raw, ["Location", "VenueName", "Venue"])
c_activity = find_col(raw, ["Activity", "ActivityName", "Source Activity"])
c_date = find_col(raw, ["SessionDate", "Session Date", "Date"], required=True)
c_start = find_col(raw, ["StartTime", "Start Time", "Time"])
c_name = find_col(raw, ["ParticipantName", "MemberName", "FullName", "Name"])
c_card = find_col(raw, ["SaheliCardNumber", "Saheli Card", "CardNumber", "MemberNumber"])
c_phone = find_col(raw, ["Phone", "Mobile", "MobileNumber"])
c_postcode = find_col(raw, ["Postcode", "Post Code"])
c_pid = find_col(raw, ["CRMParticipantId", "ParticipantId", "ParticipantID"])
c_lid = find_col(raw, ["CRMLiteMemberId", "LiteMemberId", "LiteMemberID"])

print("\nDetected source columns:")
for label, col in [
    ("status", c_status), ("source", c_source), ("source file", c_source_file),
    ("source sheet", c_source_sheet), ("date", c_date), ("location", c_location),
    ("activity", c_activity), ("name", c_name), ("card", c_card),
    ("phone", c_phone), ("postcode", c_postcode),
    ("participant id", c_pid), ("lite id", c_lid),
]:
    print(f"  {label:15s}: {col}")

if c_status:
    status_text = raw[c_status].fillna("").astype(str).str.upper()
    mask = (
        status_text.str.contains("SOURCE_ONLY_VERIFIED", regex=False)
        | status_text.str.contains("SOURCE ONLY VERIFIED", regex=False)
        | status_text.str.contains("SOURCE-ONLY VERIFIED", regex=False)
    )
    if mask.sum() == 0:
        mask = (
            status_text.str.contains("SOURCE", regex=False)
            & status_text.str.contains("VERIFIED", regex=False)
            & ~status_text.str.contains("CRM_CONFIRMED", regex=False)
        )
    source_only = raw[mask].copy()
else:
    print("\nWARNING: No classification column detected; checking all rows.\n")
    source_only = raw.copy()

print(f"\nRows selected for live CRM re-check: {len(source_only):,}")
if source_only.empty:
    raise SystemExit("No SOURCE_ONLY_VERIFIED rows detected.")

fail_if_placeholder_connection_string()

print("\nConnecting to Saheli CRM...")
conn = pyodbc.connect(CONNECTION_STRING)
print("Connected.")

sessions = sql_df(
    conn,
    """
    SELECT SessionId, SessionDate, VenueName, ActivityName, StartTime, EndTime
    FROM dbo.Sessions
    WHERE SessionDate >= ? AND SessionDate < ?
    """,
    [REPORT_START, REPORT_END_EXCLUSIVE],
)

attendance = sql_df(
    conn,
    """
    SELECT
        sa.AttendanceId, sa.SessionId, sa.ParticipantId, sa.LiteMemberId,
        sa.SaheliCardNumber, sa.MemberDisplayId, sa.MemberName, sa.Phone,
        sa.Attended
    FROM dbo.SessionAttendance sa
    INNER JOIN dbo.Sessions s ON s.SessionId = sa.SessionId
    WHERE s.SessionDate >= ? AND s.SessionDate < ? AND sa.Attended = 1
    """,
    [REPORT_START, REPORT_END_EXCLUSIVE],
)

participants = sql_df(
    conn,
    """
    SELECT ParticipantID, SaheliCardNumber, FullName, MobileNumber, Postcode
    FROM dbo.Participants
    """
)

lite = sql_df(
    conn,
    """
    SELECT Id, MembershipId, FirstName, LastName, Phone, Postcode
    FROM dbo.LiteMembers
    """
)

conn.close()

sessions["date_key"] = pd.to_datetime(sessions["SessionDate"], errors="coerce").dt.normalize()
sessions["location_key"] = sessions["VenueName"].map(norm_text)
sessions["activity_key"] = sessions["ActivityName"].map(norm_text)
sessions["start_key"] = sessions["StartTime"].map(to_time_key)

participants["ParticipantID"] = pd.to_numeric(
    participants["ParticipantID"], errors="coerce"
).astype("Int64")
participants["card_key"] = participants["SaheliCardNumber"].map(norm_card)
participants["name_key"] = participants["FullName"].map(norm_name)
participants["phone_key"] = participants["MobileNumber"].map(norm_phone)
participants["postcode_key"] = participants["Postcode"].map(norm_postcode)

lite["Id_str"] = lite["Id"].astype(str).str.lower()
lite["FullName"] = (
    lite["FirstName"].fillna("").astype(str).str.strip()
    + " "
    + lite["LastName"].fillna("").astype(str).str.strip()
).str.strip()
lite["name_key"] = lite["FullName"].map(norm_name)
lite["phone_key"] = lite["Phone"].map(norm_phone)
lite["postcode_key"] = lite["Postcode"].map(norm_postcode)

attendance["ParticipantId_int"] = pd.to_numeric(
    attendance["ParticipantId"], errors="coerce"
).astype("Int64")
attendance["LiteMemberId_str"] = attendance["LiteMemberId"].astype(str).str.lower()
attendance["card_key"] = attendance["SaheliCardNumber"].map(norm_card)
attendance["name_key"] = attendance["MemberName"].map(norm_name)
attendance["phone_key"] = attendance["Phone"].map(norm_phone)

session_by_exact = defaultdict(set)
session_by_date_activity = defaultdict(set)
session_by_date_location = defaultdict(set)
session_by_date = defaultdict(set)

for _, r in sessions.iterrows():
    sid = int(r["SessionId"])
    d = r["date_key"]
    if pd.isna(d):
        continue
    session_by_exact[(d, r["location_key"], r["activity_key"], r["start_key"])].add(sid)
    session_by_date_activity[(d, r["activity_key"])].add(sid)
    session_by_date_location[(d, r["location_key"])].add(sid)
    session_by_date[d].add(sid)

full_by_card = defaultdict(set)
full_by_phone = defaultdict(set)
full_by_name_postcode = defaultdict(set)

for _, r in participants.iterrows():
    if pd.isna(r["ParticipantID"]):
        continue
    pid = int(r["ParticipantID"])
    if r["card_key"]:
        full_by_card[r["card_key"]].add(pid)
    if len(r["phone_key"]) >= 10:
        full_by_phone[r["phone_key"]].add(pid)
    if r["name_key"] and r["postcode_key"]:
        full_by_name_postcode[(r["name_key"], r["postcode_key"])].add(pid)

lite_by_phone = defaultdict(set)
lite_by_name_postcode = defaultdict(set)

for _, r in lite.iterrows():
    lid = r["Id_str"]
    if len(r["phone_key"]) >= 10:
        lite_by_phone[r["phone_key"]].add(lid)
    if r["name_key"] and r["postcode_key"]:
        lite_by_name_postcode[(r["name_key"], r["postcode_key"])].add(lid)

attendance_full = set()
attendance_lite = set()
attendance_card = defaultdict(set)
attendance_name_phone = defaultdict(set)
attendance_name = defaultdict(set)

for _, r in attendance.iterrows():
    sid = int(r["SessionId"])
    if pd.notna(r["ParticipantId_int"]):
        attendance_full.add((sid, int(r["ParticipantId_int"])))
    lid = r["LiteMemberId_str"]
    if lid and lid not in {"nan", "none", "<na>"}:
        attendance_lite.add((sid, lid))
    if r["card_key"]:
        attendance_card[r["card_key"]].add(sid)
    if r["name_key"] and r["phone_key"]:
        attendance_name_phone[(r["name_key"], r["phone_key"])].add(sid)
    if r["name_key"]:
        attendance_name[r["name_key"]].add(sid)

participant_id_set = set(participants["ParticipantID"].dropna().astype(int))
lite_id_set = set(lite["Id_str"])

results = []

for src_idx, row in source_only.iterrows():
    source_date = to_date(row[c_date])

    location = row[c_location] if c_location else None
    activity = row[c_activity] if c_activity else None
    start_time = row[c_start] if c_start else None

    location_key = norm_text(location)
    activity_key = norm_text(activity)
    start_key = to_time_key(start_time)

    name = row[c_name] if c_name else None
    card = row[c_card] if c_card else None
    phone = row[c_phone] if c_phone else None
    postcode = row[c_postcode] if c_postcode else None

    name_key = norm_name(name)
    card_key = norm_card(card)
    phone_key = norm_phone(phone)
    postcode_key = norm_postcode(postcode)

    full_candidates = set()
    lite_candidates = set()
    person_evidence = []

    if c_pid and pd.notna(row[c_pid]):
        try:
            pid = int(float(row[c_pid]))
            if pid in participant_id_set:
                full_candidates.add(pid)
                person_evidence.append("CRMParticipantId")
        except Exception:
            pass

    if c_lid and pd.notna(row[c_lid]):
        lid = str(row[c_lid]).strip().lower()
        if lid in lite_id_set:
            lite_candidates.add(lid)
            person_evidence.append("CRMLiteMemberId")

    if card_key and card_key in full_by_card:
        full_candidates |= full_by_card[card_key]
        person_evidence.append("SaheliCardNumber")

    if len(phone_key) >= 10:
        if phone_key in full_by_phone:
            full_candidates |= full_by_phone[phone_key]
            person_evidence.append("Exact phone -> FULL")
        if phone_key in lite_by_phone:
            lite_candidates |= lite_by_phone[phone_key]
            person_evidence.append("Exact phone -> LITE")

    if name_key and postcode_key:
        if (name_key, postcode_key) in full_by_name_postcode:
            full_candidates |= full_by_name_postcode[(name_key, postcode_key)]
            person_evidence.append("Exact name+postcode -> FULL")
        if (name_key, postcode_key) in lite_by_name_postcode:
            lite_candidates |= lite_by_name_postcode[(name_key, postcode_key)]
            person_evidence.append("Exact name+postcode -> LITE")

    resolved_full = unique_or_none(full_candidates)
    resolved_lite = unique_or_none(lite_candidates)

    if resolved_full is not None:
        resolved_person_type = "FULL"
        resolved_person_id = resolved_full
    elif resolved_lite is not None:
        resolved_person_type = "LITE"
        resolved_person_id = resolved_lite
    else:
        resolved_person_type = None
        resolved_person_id = None

    session_candidates = set()
    session_match_level = ""

    if pd.notna(source_date):
        exact_key = (source_date, location_key, activity_key, start_key)

        if start_key and session_by_exact.get(exact_key):
            session_candidates = set(session_by_exact[exact_key])
            session_match_level = "date+location+activity+start"

        if not session_candidates and activity_key:
            cands = session_by_date_activity.get((source_date, activity_key), set())
            if cands:
                session_candidates = set(cands)
                session_match_level = "date+activity"

        if not session_candidates and location_key:
            cands = session_by_date_location.get((source_date, location_key), set())
            if cands:
                session_candidates = set(cands)
                session_match_level = "date+location"

        if not session_candidates:
            cands = session_by_date.get(source_date, set())
            if cands:
                session_candidates = set(cands)
                session_match_level = "date only"

    attendance_match_session = None
    attendance_match_type = None

    for sid in sorted(session_candidates):
        if resolved_full is not None and (sid, resolved_full) in attendance_full:
            attendance_match_session = sid
            attendance_match_type = "FULL participant + session"
            break

        if resolved_lite is not None and (sid, resolved_lite) in attendance_lite:
            attendance_match_session = sid
            attendance_match_type = "LITE participant + session"
            break

    if attendance_match_session is None and card_key:
        common = attendance_card.get(card_key, set()) & session_candidates
        if len(common) == 1:
            attendance_match_session = next(iter(common))
            attendance_match_type = "SaheliCardNumber + session"

    if attendance_match_session is None and name_key and len(phone_key) >= 10:
        common = attendance_name_phone.get((name_key, phone_key), set()) & session_candidates
        if len(common) == 1:
            attendance_match_session = next(iter(common))
            attendance_match_type = "name+phone + session"

    if attendance_match_session is None and name_key:
        common = attendance_name.get(name_key, set()) & session_candidates
        if len(common) == 1:
            attendance_match_session = next(iter(common))
            attendance_match_type = "exact name + session"

    person_exists = resolved_person_id is not None
    session_exists = len(session_candidates) > 0
    exact_attendance = attendance_match_session is not None

    if exact_attendance:
        classification = "ALREADY_IN_CRM"
    elif person_exists and session_exists:
        if len(session_candidates) == 1:
            classification = "SESSION_EXISTS_ATTENDANCE_MISSING"
        else:
            classification = "PERSON_EXISTS_SESSION_AMBIGUOUS"
    elif person_exists and not session_exists:
        classification = "PARTICIPANT_EXISTS_SESSION_MISSING"
    elif not person_exists and session_exists:
        if len(session_candidates) == 1:
            classification = "SESSION_EXISTS_PERSON_UNRESOLVED"
        else:
            classification = "SESSION_AMBIGUOUS_PERSON_UNRESOLVED"
    else:
        classification = "NOT_FOUND_IN_CRM"

    out = {
        "SourceExcelRow": src_idx + 2,
        "CrossCheckStatus": classification,
        "Source": row[c_source] if c_source else None,
        "SourceFile": row[c_source_file] if c_source_file else None,
        "SourceSheet": row[c_source_sheet] if c_source_sheet else None,
        "SourceRow": row[c_source_row] if c_source_row else None,
        "SessionDate": row[c_date],
        "Location": location,
        "Activity": activity,
        "StartTime": start_time,
        "ParticipantName": name,
        "SaheliCardNumber": card,
        "Phone": phone,
        "Postcode": postcode,
        "ResolvedPersonType": resolved_person_type,
        "ResolvedPersonId": resolved_person_id,
        "PersonEvidence": " | ".join(sorted(set(person_evidence))),
        "FullCandidateCount": len(full_candidates),
        "LiteCandidateCount": len(lite_candidates),
        "SessionCandidateCount": len(session_candidates),
        "SessionMatchLevel": session_match_level,
        "CandidateSessionIds": ",".join(map(str, sorted(session_candidates))),
        "MatchedAttendanceSessionId": attendance_match_session,
        "AttendanceMatchEvidence": attendance_match_type,
    }

    if c_status:
        out["OriginalSourceStatus"] = row[c_status]

    results.append(out)

result_df = pd.DataFrame(results)

summary = (
    result_df["CrossCheckStatus"]
    .value_counts()
    .rename_axis("CrossCheckStatus")
    .reset_index(name="Rows")
)

summary["Percentage"] = (
    100 * summary["Rows"] / len(result_df)
).round(2)

group_cols = [c for c in ["SourceFile", "SourceSheet", "Activity"] if c in result_df.columns]

by_source = (
    result_df.groupby(group_cols + ["CrossCheckStatus"], dropna=False)
    .size()
    .reset_index(name="Rows")
    .sort_values("Rows", ascending=False)
)

result_df["Month"] = pd.to_datetime(
    result_df["SessionDate"], errors="coerce"
).dt.to_period("M").astype(str)

by_month = (
    result_df.groupby(["Month", "CrossCheckStatus"], dropna=False)
    .size()
    .reset_index(name="Rows")
    .sort_values(["Month", "Rows"], ascending=[True, False])
)

with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
    summary.to_excel(writer, sheet_name="SUMMARY", index=False)
    by_source.to_excel(writer, sheet_name="BY SOURCE", index=False)
    by_month.to_excel(writer, sheet_name="BY MONTH", index=False)
    result_df.to_excel(writer, sheet_name="ROW DETAIL", index=False)

    for status in [
        "ALREADY_IN_CRM",
        "SESSION_EXISTS_ATTENDANCE_MISSING",
        "PARTICIPANT_EXISTS_SESSION_MISSING",
        "SESSION_EXISTS_PERSON_UNRESOLVED",
        "PERSON_EXISTS_SESSION_AMBIGUOUS",
        "SESSION_AMBIGUOUS_PERSON_UNRESOLVED",
        "NOT_FOUND_IN_CRM",
    ]:
        subset = result_df[result_df["CrossCheckStatus"] == status]
        if not subset.empty:
            subset.to_excel(writer, sheet_name=status[:31], index=False)

    wb = writer.book
    for ws in wb.worksheets:
        ws.freeze_panes = "A2"
        ws.auto_filter.ref = ws.dimensions

        for cell in ws[1]:
            cell.font = cell.font.copy(bold=True)

        for col_cells in ws.columns:
            letter = col_cells[0].column_letter
            max_len = 0
            for cell in col_cells[:500]:
                val = "" if cell.value is None else str(cell.value)
                max_len = max(max_len, len(val))
            ws.column_dimensions[letter].width = min(max(max_len + 2, 12), 45)

print("\n" + "=" * 70)
print("LIVE CRM CROSS-CHECK COMPLETE")
print("=" * 70)
print(f"Source-only rows checked: {len(result_df):,}")

for _, r in summary.iterrows():
    print(f"{r['CrossCheckStatus']}: {int(r['Rows']):,} ({r['Percentage']:.2f}%)")

already = int(
    summary.loc[
        summary["CrossCheckStatus"] == "ALREADY_IN_CRM",
        "Rows"
    ].sum()
)

true_missing = len(result_df) - already

print("\n------------------------------------------")
print(f"Found already in live CRM: {already:,}")
print(f"Still not proven as CRM attendance: {true_missing:,}")
print("------------------------------------------")
print(f"\nExcel saved to:\n{OUTPUT_XLSX}")
print("\nREAD ONLY: no CRM data was changed.")
