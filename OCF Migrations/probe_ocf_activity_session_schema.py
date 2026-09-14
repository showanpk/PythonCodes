#!/usr/bin/env python3
"""
OCF CRM - Activity/Session DB Preflight Probe (READ ONLY)

Purpose:
- Resolve what sessions.activity_id actually references.
- Show Activity/Activity Type related tables and views.
- Show relevant enum values.
- Show current activity types.
- Show existing session time patterns without exposing participant data.

No INSERT/UPDATE/DELETE statements are used.
"""

from __future__ import annotations
import os
import sys
from urllib.parse import urlparse, parse_qs, unquote
import pymysql

CONNECTION_STRING = os.getenv(
    "OCF_MYSQL_CONNECTION_STRING",
    "mysql://YOUR_DB_USER:YOUR_DB_PASSWORD@YOUR_DB_HOST:3306/YOUR_DB_NAME?charset=utf8mb4",
)

def connect(cs: str):
    p = urlparse(cs)
    if p.scheme not in {"mysql", "mysql+pymysql"}:
        raise ValueError("Connection string must start mysql://")
    if not p.hostname or not p.username or not p.path.strip("/"):
        raise ValueError("Connection string is incomplete.")
    q = parse_qs(p.query)
    kwargs = dict(
        host=p.hostname,
        port=p.port or 3306,
        user=unquote(p.username),
        password=unquote(p.password or ""),
        database=p.path.lstrip("/"),
        charset=q.get("charset", ["utf8mb4"])[0],
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=15,
    )
    if q.get("ssl", ["false"])[0].lower() in {"1","true","yes"}:
        kwargs["ssl"] = {}
    return pymysql.connect(**kwargs)

def print_rows(title, rows):
    print()
    print(title)
    print("-" * len(title))
    if not rows:
        print("(none)")
        return
    cols = list(rows[0].keys())
    widths = {c:max(len(c), *(len(str(r.get(c,""))) for r in rows)) for c in cols}
    print(" | ".join(c.ljust(widths[c]) for c in cols))
    print("-+-".join("-"*widths[c] for c in cols))
    for r in rows:
        print(" | ".join(str(r.get(c,"")).ljust(widths[c]) for c in cols))

def main():
    if "YOUR_DB_" in CONNECTION_STRING:
        print("ERROR: set OCF_MYSQL_CONNECTION_STRING first.", file=sys.stderr)
        return 2

    conn = connect(CONNECTION_STRING)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT TABLE_NAME, TABLE_TYPE
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = DATABASE()
                  AND (
                    LOWER(TABLE_NAME) LIKE '%activ%'
                    OR LOWER(TABLE_NAME) LIKE '%session%'
                    OR LOWER(TABLE_NAME) LIKE '%attend%'
                  )
                ORDER BY TABLE_NAME
            """)
            print_rows("Activity / Session / Attendance tables and views", cur.fetchall())

            cur.execute("""
                SELECT
                    CONSTRAINT_NAME,
                    TABLE_NAME,
                    COLUMN_NAME,
                    REFERENCED_TABLE_NAME,
                    REFERENCED_COLUMN_NAME
                FROM information_schema.KEY_COLUMN_USAGE
                WHERE TABLE_SCHEMA = DATABASE()
                  AND REFERENCED_TABLE_NAME IS NOT NULL
                  AND TABLE_NAME IN (
                    'sessions',
                    'attendance',
                    'session_registrations',
                    'participant_activity_types'
                  )
                ORDER BY TABLE_NAME, COLUMN_NAME
            """)
            fks = cur.fetchall()
            print_rows("Relevant foreign keys", fks)

            cur.execute("""
                SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND DATA_TYPE = 'enum'
                  AND TABLE_NAME IN (
                    'activity_types',
                    'sessions',
                    'attendance',
                    'session_registrations'
                  )
                ORDER BY TABLE_NAME, ORDINAL_POSITION
            """)
            print_rows("Relevant ENUM definitions", cur.fetchall())

            cur.execute("""
                SELECT id, name, status
                FROM activity_types
                ORDER BY name
            """)
            print_rows("Current activity_types", cur.fetchall())

            # Resolve sessions.activity_id target.
            target = None
            for fk in fks:
                if fk["TABLE_NAME"] == "sessions" and fk["COLUMN_NAME"] == "activity_id":
                    target = fk["REFERENCED_TABLE_NAME"]
                    break

            if target:
                cur.execute("""
                    SELECT COLUMN_NAME
                    FROM information_schema.COLUMNS
                    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME=%s
                    ORDER BY ORDINAL_POSITION
                """, (target,))
                cols = [r["COLUMN_NAME"] for r in cur.fetchall()]
                print()
                print(f"sessions.activity_id references: {target}")
                print(f"Referenced table columns: {', '.join(cols)}")

                safe_cols = [c for c in ("id","name","title","status","activity_type_id") if c in cols]
                if safe_cols:
                    cur.execute(f"SELECT {', '.join(safe_cols)} FROM `{target}` ORDER BY 1 LIMIT 200")
                    print_rows(f"Rows from referenced table {target}", cur.fetchall())
            else:
                print()
                print("WARNING: No FK metadata found for sessions.activity_id.")

            # Existing session time patterns by activity_id.
            cur.execute("""
                SELECT
                    activity_id,
                    COUNT(*) AS session_count,
                    MIN(start_at) AS first_session,
                    MAX(start_at) AS last_session,
                    COUNT(DISTINCT TIME(start_at)) AS distinct_start_times,
                    COUNT(DISTINCT TIME(end_at)) AS distinct_end_times
                FROM sessions
                GROUP BY activity_id
                ORDER BY session_count DESC, activity_id
            """)
            print_rows("Existing session patterns by activity_id", cur.fetchall())

            cur.execute("""
                SELECT
                    activity_id,
                    TIME(start_at) AS start_time,
                    TIME(end_at) AS end_time,
                    COUNT(*) AS sessions
                FROM sessions
                GROUP BY activity_id, TIME(start_at), TIME(end_at)
                ORDER BY activity_id, sessions DESC, start_time
            """)
            print_rows("Existing session time pairs", cur.fetchall())

        print()
        print("READ-ONLY PREFLIGHT COMPLETE. Database was not changed.")
        return 0
    finally:
        conn.close()

if __name__ == "__main__":
    raise SystemExit(main())
