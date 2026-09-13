#!/usr/bin/env python3
"""
Saheli CRM - Iffat Shaheen Bike Giveaway Identity Check V1
===========================================================

READ-ONLY diagnostic for the final Bike Giveaway blocker.

Checks:
- exact source Wellbeing ID across identity-like CRM text columns
- exact source email across identity-like CRM text columns
- exact-name FULL/LITE candidate profiles
- recent attendance history for each candidate

No INSERT / UPDATE / DELETE. Explicit rollback before exit.
"""

from __future__ import annotations
import os
import re
from datetime import datetime

CONNECTION_STRING = os.getenv("SAHELI_SQL_CONNECTION_STRING", "").strip()

SOURCE_NAME = "Iffat Shaheen"
SOURCE_WELLBEING = "THE1769791952260"
SOURCE_EMAIL = "asififfat42@gmail.com"

IDENTITY_COLUMN_RE = re.compile(
    r"(wellbeing|card|email|mail|name|phone|mobile|contact|postcode|identifier|member|participant|notes?)",
    re.I,
)
TEXT_TYPES = {"char", "nchar", "varchar", "nvarchar", "text", "ntext"}


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
        raise RuntimeError("Set SAHELI_SQL_CONNECTION_STRING before running.")
    import pyodbc
    return pyodbc.connect(CONNECTION_STRING, autocommit=False)


def quote_ident(name):
    return "[" + name.replace("]", "]]") + "]"


def all_identity_text_columns(cur):
    rows = cur.execute("""
        SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA='dbo'
        ORDER BY TABLE_NAME, ORDINAL_POSITION
    """).fetchall()
    return [
        (str(t), str(c))
        for t, c, dt in rows
        if str(dt).lower() in TEXT_TYPES and IDENTITY_COLUMN_RE.search(str(c))
    ]


def table_columns(cur, table):
    return [
        str(r[0]) for r in cur.execute("""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME=?
            ORDER BY ORDINAL_POSITION
        """, table).fetchall()
    ]


def context_columns(cur, table):
    actual = set(table_columns(cur, table))
    preferred = [
        "ParticipantID", "ParticipantId", "Id", "MembershipId",
        "SaheliCardNumber", "WellbeingCardNumber", "FullName", "Name",
        "FirstName", "LastName", "Email", "EmailAddress", "Gender",
        "DateOfBirth", "DOB", "Postcode", "MobileNumber", "Phone",
        "PhoneNumber", "SessionId", "ActivityName", "SessionDate",
        "RegistrationStatus", "CreatedAtUtc", "UpdatedAtUtc", "Notes", "Note"
    ]
    return [c for c in preferred if c in actual][:18]


def search_exact(cur, value):
    hits = []
    for table, column in all_identity_text_columns(cur):
        qtable = quote_ident(table)
        qcol = quote_ident(column)
        try:
            count = int(cur.execute(
                f"SELECT COUNT(*) FROM dbo.{qtable} "
                f"WHERE LTRIM(RTRIM(CONVERT(nvarchar(4000), {qcol}))) = ?",
                value,
            ).fetchone()[0])
        except Exception:
            continue
        if not count:
            continue

        ctx = context_columns(cur, table)
        rows = []
        if ctx:
            try:
                rows = cur.execute(
                    f"SELECT TOP 20 {', '.join(quote_ident(c) for c in ctx)} "
                    f"FROM dbo.{qtable} "
                    f"WHERE LTRIM(RTRIM(CONVERT(nvarchar(4000), {qcol}))) = ?",
                    value,
                ).fetchall()
            except Exception:
                pass
        hits.append((table, column, count, ctx, rows))
    return hits


def print_exact(label, value, hits):
    print(f"\n{label}: {value}")
    if not hits:
        print("  NO EXACT CRM MATCH FOUND")
        return
    for table, column, count, ctx, rows in hits:
        print(f"  HIT dbo.{table}.{column} | rows={count}")
        for i, row in enumerate(rows, 1):
            bits = []
            for j, c in enumerate(ctx):
                v = clean(row[j])
                if v:
                    bits.append(f"{c}={v}")
            print(f"    [{i}] " + " | ".join(bits))


def print_recent_full(cur, pid):
    rows = cur.execute("""
        SELECT TOP 12 sa.SessionDate,
               COALESCE(s.ActivityName, sa.SessionName),
               s.VenueName,
               sa.Attended
        FROM dbo.SessionAttendance sa
        LEFT JOIN dbo.Sessions s ON s.SessionId=sa.SessionId
        WHERE sa.ParticipantId=?
        ORDER BY sa.SessionDate DESC, sa.AttendanceId DESC
    """, pid).fetchall()
    total = cur.execute(
        "SELECT COUNT(*) FROM dbo.SessionAttendance WHERE ParticipantId=?",
        pid
    ).fetchone()[0]
    print(f"    Attendance total: {total}")
    for r in rows:
        print(f"      {r[0]} | {clean(r[1]) or '-'} | {clean(r[2]) or '-'} | attended={r[3]}")


def print_recent_lite(cur, lid):
    rows = cur.execute("""
        SELECT TOP 12 sa.SessionDate,
               COALESCE(s.ActivityName, sa.SessionName),
               s.VenueName,
               sa.Attended
        FROM dbo.SessionAttendance sa
        LEFT JOIN dbo.Sessions s ON s.SessionId=sa.SessionId
        WHERE sa.LiteMemberId=?
        ORDER BY sa.SessionDate DESC, sa.AttendanceId DESC
    """, lid).fetchall()
    total = cur.execute(
        "SELECT COUNT(*) FROM dbo.SessionAttendance WHERE LiteMemberId=?",
        lid
    ).fetchone()[0]
    print(f"    Attendance total: {total}")
    for r in rows:
        print(f"      {r[0]} | {clean(r[1]) or '-'} | {clean(r[2]) or '-'} | attended={r[3]}")


def main():
    conn = get_connection()
    try:
        cur = conn.cursor()

        print("Saheli CRM - Iffat Shaheen Bike Giveaway Identity Check V1")
        print("READ ONLY - no database changes are made.")

        print_exact("SOURCE WELLBEING ID", SOURCE_WELLBEING, search_exact(cur, SOURCE_WELLBEING))
        print_exact("SOURCE EMAIL", SOURCE_EMAIL, search_exact(cur, SOURCE_EMAIL))

        print("\nEXACT-NAME FULL CANDIDATES")
        full_rows = cur.execute("""
            SELECT ParticipantID, SaheliCardNumber, FullName, Gender, Email,
                   DateOfBirth, Postcode, MobileNumber, RegistrationDate
            FROM dbo.Participants
        """).fetchall()

        found_full = False
        for r in full_rows:
            if norm_name(r[2]) != norm_name(SOURCE_NAME):
                continue
            found_full = True
            print(
                f"  FULL card={clean(r[1])} pid={r[0]} | gender={clean(r[3]) or '-'} | "
                f"email={clean(r[4]) or '-'} | dob={clean(r[5]) or '-'} | "
                f"postcode={clean(r[6]) or '-'} | mobile={clean(r[7]) or '-'} | "
                f"registration={clean(r[8]) or '-'}"
            )
            print_recent_full(cur, r[0])
        if not found_full:
            print("  None")

        print("\nEXACT-NAME LITE CANDIDATES")
        lite_cols = set(table_columns(cur, "LiteMembers"))
        select_cols = ["Id", "MembershipId", "FirstName", "LastName"]
        for c in ["Gender", "Email", "DateOfBirth", "Postcode", "Phone", "CreatedAtUtc"]:
            if c in lite_cols:
                select_cols.append(c)
        rows = cur.execute(
            "SELECT " + ", ".join(quote_ident(c) for c in select_cols) + " FROM dbo.LiteMembers"
        ).fetchall()

        found_lite = False
        for r in rows:
            data = {select_cols[i]: r[i] for i in range(len(select_cols))}
            name = f"{clean(data.get('FirstName')) or ''} {clean(data.get('LastName')) or ''}".strip()
            if norm_name(name) != norm_name(SOURCE_NAME):
                continue
            found_lite = True
            print(
                f"  LITE membership={clean(data.get('MembershipId'))} id={clean(data.get('Id'))} | "
                f"gender={clean(data.get('Gender')) or '-'} | email={clean(data.get('Email')) or '-'} | "
                f"dob={clean(data.get('DateOfBirth')) or '-'} | postcode={clean(data.get('Postcode')) or '-'} | "
                f"phone={clean(data.get('Phone')) or '-'} | created={clean(data.get('CreatedAtUtc')) or '-'}"
            )
            print_recent_lite(cur, data["Id"])
        if not found_lite:
            print("  None")

        print("\nDONE")
        print("No data was changed. Transaction rolled back.")
        conn.rollback()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
