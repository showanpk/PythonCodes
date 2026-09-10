#!/usr/bin/env python3
"""
Saheli CRM - Bike Giveaway Ambiguous Member Diagnostic V1
---------------------------------------------------------
READ-ONLY diagnostic for the 5 remaining Bike Giveaway identity reviews.

It:
- reads Participants and LiteMembers for the five exact names;
- reads CyclingRegistrations by exact normalized name (interest/identity evidence only);
- shows recent SessionAttendance history for each candidate;
- performs NO INSERT/UPDATE/DELETE and never commits anything.
"""

from __future__ import annotations

import os
import re
from collections import defaultdict
from datetime import datetime

CONNECTION_STRING = os.getenv("SAHELI_SQL_CONNECTION_STRING", "").strip()

TARGET_NAMES = [
    "Hayat Ali",
    "Yasmeen Akhtar",
    "Shamim Akhtar",
    "Noreen Akhtar",
    "Shahida Ali",
]

SOURCE_EVIDENCE = {
    "hayat ali": {
        "wellbeing": "THE1769785179930",
        "email": "Hayatali1963@icloud.com",
        "gender": "Male",
        "source_row": 12,
        "giveaway": 1,
    },
    "yasmeen akhtar": {
        "wellbeing": "MCR022558122",
        "email": "yasmeen-akhtar@hotmail.co.uk",
        "gender": "Female",
        "source_row": 19,
        "giveaway": 1,
    },
    "shamim akhtar": {
        "wellbeing": "MCR022496285",
        "email": "Shamim-akhtar@hotmail.co.uk",
        "gender": "Female",
        "source_row": 20,
        "giveaway": 1,
    },
    "noreen akhtar": {
        "wellbeing": "THE1774433555904",
        "email": None,
        "gender": "Female",
        "source_row": 138,
        "giveaway": 5,
    },
    "shahida ali": {
        "wellbeing": None,
        "email": None,
        "gender": "Female",
        "source_row": 140,
        "giveaway": 5,
    },
}


def clean(v):
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def norm_name(v):
    s = (clean(v) or "").lower()
    s = re.sub(r"^(mr|mrs|ms|miss|dr)\.?\s+", "", s)
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def get_connection():
    if not CONNECTION_STRING:
        raise RuntimeError(
            "Set SAHELI_SQL_CONNECTION_STRING before running this diagnostic."
        )
    try:
        import pyodbc
    except ImportError:
        raise RuntimeError("pyodbc is required.")
    return pyodbc.connect(CONNECTION_STRING, autocommit=False)


def table_exists(cur, table):
    return bool(cur.execute(
        "SELECT 1 FROM INFORMATION_SCHEMA.TABLES "
        "WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME=?",
        table,
    ).fetchone())


def columns(cur, table):
    return {
        r[0]
        for r in cur.execute(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME=?",
            table,
        ).fetchall()
    }


def val(row, mapping, name):
    idx = mapping.get(name)
    return clean(row[idx]) if idx is not None else None


def select_dynamic(cur, table, preferred_cols):
    actual = columns(cur, table)
    chosen = [c for c in preferred_cols if c in actual]
    if not chosen:
        return [], [], {}
    sql = "SELECT " + ", ".join(f"[{c}]" for c in chosen) + f" FROM dbo.[{table}]"
    rows = cur.execute(sql).fetchall()
    mapping = {c: i for i, c in enumerate(chosen)}
    return chosen, rows, mapping


def print_kv(label, value):
    print(f"    {label:22s}: {value if value not in (None, '') else '-'}")


def full_candidates(cur):
    cols, rows, m = select_dynamic(cur, "Participants", [
        "ParticipantID", "SaheliCardNumber", "FullName", "Gender", "Email",
        "DateOfBirth", "Postcode", "MobileNumber", "RegistrationDate", "CreatedAtUtc"
    ])
    out = defaultdict(list)
    for r in rows:
        name = val(r, m, "FullName")
        if norm_name(name) in SOURCE_EVIDENCE:
            out[norm_name(name)].append({
                c: clean(r[i]) for i, c in enumerate(cols)
            })
    return out


def lite_candidates(cur):
    cols, rows, m = select_dynamic(cur, "LiteMembers", [
        "Id", "MembershipId", "FirstName", "LastName", "Gender", "Email",
        "DateOfBirth", "Postcode", "Phone", "CreatedAtUtc"
    ])
    out = defaultdict(list)
    for r in rows:
        first = val(r, m, "FirstName") or ""
        last = val(r, m, "LastName") or ""
        name = f"{first} {last}".strip()
        if norm_name(name) in SOURCE_EVIDENCE:
            item = {c: clean(r[i]) for i, c in enumerate(cols)}
            item["FullName"] = name
            out[norm_name(name)].append(item)
    return out


def cycling_registrations(cur):
    if not table_exists(cur, "CyclingRegistrations"):
        return defaultdict(list)

    preferred = [
        "CyclingRegistrationId", "CyclingPublicLinkId", "SessionId", "Name",
        "SaheliCardNumber", "WellbeingCardNumber", "Email", "MobileNumber",
        "Postcode", "Gender", "RegistrationStatus", "CreatedAtUtc", "UpdatedAtUtc"
    ]
    cols, rows, m = select_dynamic(cur, "CyclingRegistrations", preferred)
    out = defaultdict(list)
    for r in rows:
        name = val(r, m, "Name")
        if norm_name(name) in SOURCE_EVIDENCE:
            out[norm_name(name)].append({
                c: clean(r[i]) for i, c in enumerate(cols)
            })
    return out


def attendance_for_full(cur, participant_id):
    if not table_exists(cur, "SessionAttendance"):
        return []
    rows = cur.execute(
        """
        SELECT TOP 12
            sa.SessionDate,
            COALESCE(s.ActivityName, sa.SessionName) AS ActivityName,
            s.VenueName,
            sa.Attended
        FROM dbo.SessionAttendance sa
        LEFT JOIN dbo.Sessions s ON s.SessionId = sa.SessionId
        WHERE sa.ParticipantId = ?
        ORDER BY sa.SessionDate DESC, sa.AttendanceId DESC
        """,
        participant_id,
    ).fetchall()
    return rows


def attendance_for_lite(cur, lite_id):
    if not table_exists(cur, "SessionAttendance"):
        return []
    rows = cur.execute(
        """
        SELECT TOP 12
            sa.SessionDate,
            COALESCE(s.ActivityName, sa.SessionName) AS ActivityName,
            s.VenueName,
            sa.Attended
        FROM dbo.SessionAttendance sa
        LEFT JOIN dbo.Sessions s ON s.SessionId = sa.SessionId
        WHERE sa.LiteMemberId = ?
        ORDER BY sa.SessionDate DESC, sa.AttendanceId DESC
        """,
        lite_id,
    ).fetchall()
    return rows


def attendance_count_full(cur, participant_id):
    return cur.execute(
        "SELECT COUNT(*) FROM dbo.SessionAttendance WHERE ParticipantId=?",
        participant_id,
    ).fetchone()[0]


def attendance_count_lite(cur, lite_id):
    return cur.execute(
        "SELECT COUNT(*) FROM dbo.SessionAttendance WHERE LiteMemberId=?",
        lite_id,
    ).fetchone()[0]


def cycling_history(rows):
    cycling_words = ("cycl", "ride", "bike")
    return [
        r for r in rows
        if any(w in (clean(r[1]) or "").lower() for w in cycling_words)
    ]


def print_attendance(rows):
    if not rows:
        print("      No attendance rows found.")
        return
    for r in rows:
        d = r[0].date().isoformat() if isinstance(r[0], datetime) else str(r[0])
        print(
            f"      {d} | {clean(r[1]) or '-'} | {clean(r[2]) or '-'} | attended={r[3]}"
        )


def main():
    conn = get_connection()
    try:
        cur = conn.cursor()

        full = full_candidates(cur)
        lite = lite_candidates(cur)
        regs = cycling_registrations(cur)

        print("Saheli CRM - Bike Giveaway Ambiguous Member Diagnostic V1")
        print("READ ONLY - no database changes are made.")

        for display in TARGET_NAMES:
            key = norm_name(display)
            src = SOURCE_EVIDENCE[key]

            print("\n" + "=" * 88)
            print(display)
            print("=" * 88)
            print("SOURCE BIKE GIVEAWAY EVIDENCE")
            print_kv("Giveaway", src["giveaway"])
            print_kv("Source row", src["source_row"])
            print_kv("Wellbeing ID", src["wellbeing"])
            print_kv("Email", src["email"])
            print_kv("Gender", src["gender"])

            print("\nFULL CRM CANDIDATES")
            if not full[key]:
                print("    None")
            for p in full[key]:
                print(f"  FULL card={p.get('SaheliCardNumber')} pid={p.get('ParticipantID')}")
                for fld in ["Gender", "Email", "DateOfBirth", "Postcode", "MobileNumber",
                            "RegistrationDate", "CreatedAtUtc"]:
                    if fld in p:
                        print_kv(fld, p.get(fld))

                hist = attendance_for_full(cur, p.get("ParticipantID"))
                count = attendance_count_full(cur, p.get("ParticipantID"))
                cyc = cycling_history(hist)
                print_kv("Attendance total", count)
                print_kv("Cycling rows in last 12", len(cyc))
                print("    Recent attendance:")
                print_attendance(hist)

            print("\nLITE CRM CANDIDATES")
            if not lite[key]:
                print("    None")
            for p in lite[key]:
                print(f"  LITE membership={p.get('MembershipId')} id={p.get('Id')}")
                for fld in ["Gender", "Email", "DateOfBirth", "Postcode", "Phone", "CreatedAtUtc"]:
                    if fld in p:
                        print_kv(fld, p.get(fld))

                hist = attendance_for_lite(cur, p.get("Id"))
                count = attendance_count_lite(cur, p.get("Id"))
                cyc = cycling_history(hist)
                print_kv("Attendance total", count)
                print_kv("Cycling rows in last 12", len(cyc))
                print("    Recent attendance:")
                print_attendance(hist)

            print("\nCYCLING REGISTRATIONS (identity/support evidence only; NOT attendance)")
            if not regs[key]:
                print("    None")
            for i, r in enumerate(regs[key], 1):
                print(f"  Registration {i}")
                for fld in [
                    "CyclingRegistrationId", "SessionId", "SaheliCardNumber",
                    "WellbeingCardNumber", "Email", "MobileNumber", "Postcode",
                    "Gender", "RegistrationStatus", "CreatedAtUtc", "UpdatedAtUtc"
                ]:
                    if fld in r:
                        print_kv(fld, r.get(fld))

        print("\n" + "=" * 88)
        print("DONE")
        print("This script is diagnostic only. No COMMIT is performed.")
        conn.rollback()

    finally:
        conn.close()


if __name__ == "__main__":
    main()
