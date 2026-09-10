#!/usr/bin/env python3
"""
Saheli CRM - Bike Giveaway Identity Evidence Scanner V2
=======================================================

READ-ONLY diagnostic for the remaining ambiguous Bike Giveaway recipients.

Purpose:
- Search the LIVE CRM for the exact source Wellbeing IDs and source email
  addresses across all dbo text columns that look identity-related.
- This can find evidence stored outside Participants/LiteMembers/
  CyclingRegistrations (for example legacy registration/import/support tables).

Safety:
- SELECT only.
- No INSERT / UPDATE / DELETE.
- Explicit rollback before exit.
"""

from __future__ import annotations

import os
import re
from collections import defaultdict

CONNECTION_STRING = os.getenv("SAHELI_SQL_CONNECTION_STRING", "").strip()

CASES = {
    "Hayat Ali": {
        "wellbeing": "THE1769785179930",
        "email": "Hayatali1963@icloud.com",
    },
    "Yasmeen Akhtar": {
        "wellbeing": "MCR022558122",
        "email": "yasmeen-akhtar@hotmail.co.uk",
    },
    "Shamim Akhtar": {
        "wellbeing": "MCR022496285",
        "email": "Shamim-akhtar@hotmail.co.uk",
    },
    "Noreen Akhtar": {
        "wellbeing": "THE1774433555904",
        "email": None,
    },
    "Shahida Ali": {
        "wellbeing": None,
        "email": None,
    },
}

# Restrict dynamic search to identity-ish columns, not every nvarchar column.
IDENTITY_COLUMN_RE = re.compile(
    r"(wellbeing|card|email|mail|name|phone|mobile|contact|postcode|identifier|member|participant|notes?)",
    re.I,
)

TEXT_TYPES = {"char", "nchar", "varchar", "nvarchar", "text", "ntext"}


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


def quote_ident(name: str) -> str:
    return "[" + name.replace("]", "]]") + "]"


def all_identity_text_columns(cur):
    rows = cur.execute(
        """
        SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA='dbo'
        ORDER BY TABLE_NAME, ORDINAL_POSITION
        """
    ).fetchall()

    cols = []
    for table, column, dtype in rows:
        if str(dtype).lower() not in TEXT_TYPES:
            continue
        if not IDENTITY_COLUMN_RE.search(str(column)):
            continue
        cols.append((str(table), str(column)))
    return cols


def table_columns(cur, table):
    rows = cur.execute(
        """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME=?
        ORDER BY ORDINAL_POSITION
        """,
        table,
    ).fetchall()
    return [str(r[0]) for r in rows]


def primary_key_columns(cur, table):
    rows = cur.execute(
        """
        SELECT c.name
        FROM sys.indexes i
        JOIN sys.index_columns ic
          ON i.object_id=ic.object_id AND i.index_id=ic.index_id
        JOIN sys.columns c
          ON ic.object_id=c.object_id AND ic.column_id=c.column_id
        WHERE i.object_id=OBJECT_ID(?)
          AND i.is_primary_key=1
        ORDER BY ic.key_ordinal
        """,
        f"dbo.{table}",
    ).fetchall()
    return [str(r[0]) for r in rows]


def context_columns(cur, table):
    actual = set(table_columns(cur, table))
    preferred = [
        "ParticipantID", "ParticipantId", "Id", "MembershipId",
        "SaheliCardNumber", "WellbeingCardNumber",
        "FullName", "Name", "FirstName", "LastName",
        "Email", "EmailAddress", "Gender", "DateOfBirth", "DOB",
        "Postcode", "MobileNumber", "Phone", "PhoneNumber",
        "SessionId", "ActivityName", "SessionDate",
        "RegistrationStatus", "CreatedAtUtc", "UpdatedAtUtc",
        "Notes", "Note"
    ]
    out = []
    for c in preferred:
        if c in actual and c not in out:
            out.append(c)

    for pk in primary_key_columns(cur, table):
        if pk in actual and pk not in out:
            out.insert(0, pk)

    return out[:18]


def search_exact(cur, value):
    hits = []
    if not value:
        return hits

    for table, column in all_identity_text_columns(cur):
        qtable = quote_ident(table)
        qcol = quote_ident(column)

        # Trim/case-insensitive exact comparison under the DB collation.
        sql = (
            f"SELECT COUNT(*) FROM dbo.{qtable} "
            f"WHERE LTRIM(RTRIM(CONVERT(nvarchar(4000), {qcol}))) = ?"
        )
        try:
            count = int(cur.execute(sql, value).fetchone()[0])
        except Exception:
            continue

        if count <= 0:
            continue

        ctx = context_columns(cur, table)
        if not ctx:
            hits.append((table, column, count, []))
            continue

        select_list = ", ".join(quote_ident(c) for c in ctx)
        sql2 = (
            f"SELECT TOP 20 {select_list} FROM dbo.{qtable} "
            f"WHERE LTRIM(RTRIM(CONVERT(nvarchar(4000), {qcol}))) = ?"
        )
        try:
            rows = cur.execute(sql2, value).fetchall()
        except Exception:
            rows = []

        rendered = []
        for row in rows:
            rendered.append(
                {ctx[i]: row[i] for i in range(len(ctx))}
            )
        hits.append((table, column, count, rendered))

    return hits


def print_hits(label, value, hits):
    print(f"\n{label}: {value or '-'}")
    if not value:
        print("  No source value available.")
        return
    if not hits:
        print("  NO EXACT CRM MATCH FOUND")
        return

    for table, column, count, rows in hits:
        print(f"  HIT dbo.{table}.{column} | rows={count}")
        for idx, row in enumerate(rows, 1):
            compact = []
            for k, v in row.items():
                if v is None:
                    continue
                text = str(v).strip()
                if not text:
                    continue
                if len(text) > 180:
                    text = text[:177] + "..."
                compact.append(f"{k}={text}")
            print(f"    [{idx}] " + " | ".join(compact))


def main():
    conn = get_connection()
    try:
        cur = conn.cursor()
        identity_cols = all_identity_text_columns(cur)

        print("Saheli CRM - Bike Giveaway Identity Evidence Scanner V2")
        print("READ ONLY - no database changes are made.")
        print(f"Identity-like text columns scanned: {len(identity_cols)}")

        for name, source in CASES.items():
            print("\n" + "=" * 100)
            print(name)
            print("=" * 100)

            wellbeing_hits = search_exact(cur, source.get("wellbeing"))
            print_hits("SOURCE WELLBEING ID", source.get("wellbeing"), wellbeing_hits)

            email_hits = search_exact(cur, source.get("email"))
            print_hits("SOURCE EMAIL", source.get("email"), email_hits)

        print("\n" + "=" * 100)
        print("DONE")
        print("No data was changed. Transaction rolled back.")
        conn.rollback()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
