#!/usr/bin/env python3
"""
Saheli CRM - Bike Giveaway Historical Migration V1.1
==================================================

Scope:
- Reads ONLY the "Bike Giveaway" sheet from "Cycling Register 2026.xlsx".
- Creates/reuses 5 "Bike Giveaway" Sessions.
- Reuses existing FULL/LITE CRM members when safely matched.
- Creates a new LITE member when the person is not already in CRM.
- Adds the resolved member to SessionAttendance with Attended=1.
- Does NOT migrate normal cycling sessions.
- Does NOT alter CyclingRegistrations.
- Does NOT delete or overwrite existing CRM records.

Important identity rule:
The source "Wellbeing Card Number" (THE..., MCR..., long numeric values) is NOT
treated as a Saheli Card Number. It may be used as supporting identity evidence
via dbo.CyclingRegistrations, but a new Saheli Card Number is never invented.

PREVIEW is the default. Use --commit only after REVIEW_* = 0 and recipient reconciliation is complete.
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import sys
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Optional

from openpyxl import load_workbook

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

CONNECTION_STRING = os.getenv("SAHELI_SQL_CONNECTION_STRING", "").strip()
BASE_DIR = Path(__file__).resolve().parent
SOURCE_FILENAME = "Cycling Register 2026.xlsx"
SOURCE_SHEET = "Bike Giveaway"

ACTIVITY_NAME = "Bike Giveaway"

# The source workbook lists the five giveaway dates in E3:E7 but does not
# repeat the date on every recipient row. The recipient list is stored in five
# consecutive batches. These row ranges preserve the source ordering:
#   34 + 36 + 22 + 22 + 25 = 139 source recipient rows.
EVENTS = [
    {"number": 1, "date_cell": "E3", "start_row": 2,   "end_row": 35},
    {"number": 2, "date_cell": "E4", "start_row": 36,  "end_row": 71},
    {"number": 3, "date_cell": "E5", "start_row": 72,  "end_row": 93},
    {"number": 4, "date_cell": "E6", "start_row": 94,  "end_row": 115},
    {"number": 5, "date_cell": "E7", "start_row": 116, "end_row": 140},
]

# Source gives no venue or time for the giveaway events.
# "Various Locations" is used so we do not falsely assign a specific Saheli site.
DEFAULT_VENUE = "Various Locations"
PLACEHOLDER_START = time(12, 0)
PLACEHOLDER_END = time(13, 0)

DEFAULT_FREQUENCY = "One-off"
DEFAULT_CATEGORY = "Cycling"
DEFAULT_ACTIVITY_CATEGORY = "Cycling"
MIGRATION_NOTE_PREFIX = "Historical Bike Giveaway migration"
NO_SURNAME_LABEL = "Unknown"

PLACEHOLDER_NAME_MARKERS = {
    "partipant a club",
    "participant a club",
}

# ---------------------------------------------------------------------------
# DATA MODELS
# ---------------------------------------------------------------------------

@dataclass
class Recipient:
    source_row: int
    event_number: int
    event_date: date
    name: str
    wellbeing_id: Optional[str]
    email: Optional[str]
    gender: Optional[str]

@dataclass
class DbFull:
    participant_id: int
    card: Optional[str]
    name: str
    email: Optional[str]
    gender: Optional[str]
    dob: Optional[date] = None
    postcode: Optional[str] = None
    mobile: Optional[str] = None

@dataclass
class DbLite:
    lite_id: str
    membership_id: str
    first_name: str
    last_name: str
    email: Optional[str]
    gender: Optional[str]
    dob: Optional[date] = None
    postcode: Optional[str] = None
    phone: Optional[str] = None

    @property
    def full_name(self) -> str:
        return f"{self.first_name} {self.last_name}".strip()

@dataclass
class ResolvedMember:
    kind: str  # FULL or LITE
    member_id: str
    display_id: str
    name: str
    action: str

@dataclass
class DbSession:
    session_id: int
    session_date: date
    venue_name: str
    activity_name: str
    start_time: time
    end_time: time

# ---------------------------------------------------------------------------
# NORMALISATION
# ---------------------------------------------------------------------------

def clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).replace("\xa0", " ").strip()
    if not text:
        return None
    if text.lower() in {"none", "null", "nan", "#n/a", "#ref!"}:
        return None
    return re.sub(r"\s+", " ", text).strip() or None


def normalize_name(value: Any) -> str:
    text = clean_text(value) or ""
    text = re.sub(r"^(mr|mrs|ms|miss|dr)\.?\s+", "", text, flags=re.I)
    text = text.strip(" -")
    text = text.lower().replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def normalize_email(value: Any) -> str:
    return (clean_text(value) or "").lower().replace(" ", "")


def normalize_gender(value: Any) -> str:
    text = (clean_text(value) or "").strip().lower()
    if text in {"f", "female", "woman", "women"}:
        return "female"
    if text in {"m", "male", "man", "men"}:
        return "male"
    return text


def normalize_postcode(value: Any) -> str:
    return re.sub(r"\s+", "", (clean_text(value) or "").upper())


def normalize_card(value: Any) -> str:
    text = (clean_text(value) or "").upper()
    text = re.sub(r"^SAH(?:ELI)?[-\s]*", "", text)
    text = text.strip(" .")
    if re.fullmatch(r"\d+(?:\.0+)?", text):
        return str(int(float(text)))
    return text


def normalize_wellbeing_id(value: Any) -> Optional[str]:
    text = clean_text(value)
    if not text:
        return None
    text = text.upper().replace(" ", "")
    if re.fullmatch(r"\d+(?:\.0+)?", text):
        text = str(int(float(text)))
    return text or None


def split_name(value: str) -> tuple[str, str]:
    text = clean_text(value) or ""
    text = re.sub(r"^(mr|mrs|ms|miss|dr)\.?\s+", "", text, flags=re.I)
    text = text.strip(" -")
    bits = text.split()
    if not bits:
        raise ValueError("No usable participant name")
    if len(bits) == 1:
        return bits[0], NO_SURNAME_LABEL
    return bits[0], " ".join(bits[1:])


def names_compatible(a: str, b: str) -> bool:
    na, nb = normalize_name(a), normalize_name(b)
    if not na or not nb:
        return False
    if na == nb:
        return True
    return SequenceMatcher(None, na, nb).ratio() >= 0.78


def parse_event_date(value: Any, year: int = 2026) -> date:
    text = (clean_text(value) or "").lower()
    text = re.sub(r"(\d+)(st|nd|rd|th)", r"\1", text)
    for fmt in ("%d %B %Y", "%d %b %Y", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            parsed = datetime.strptime(f"{text} {year}", fmt)
            return parsed.date()
        except ValueError:
            pass
    raise ValueError(f"Cannot parse giveaway date: {value!r}")

# ---------------------------------------------------------------------------
# SOURCE PARSING / AUDIT
# ---------------------------------------------------------------------------

def source_path() -> Path:
    path = BASE_DIR / SOURCE_FILENAME
    if not path.exists():
        raise FileNotFoundError(
            f"{SOURCE_FILENAME} not found beside this script.\n"
            f"Expected: {path}"
        )
    return path


def parse_source() -> tuple[list[Recipient], dict[str, Any]]:
    path = source_path()
    wb = load_workbook(path, data_only=True, read_only=True)
    try:
        if SOURCE_SHEET not in wb.sheetnames:
            raise RuntimeError(f"Sheet '{SOURCE_SHEET}' not found in {SOURCE_FILENAME}")
        ws = wb[SOURCE_SHEET]

        recipients: list[Recipient] = []
        event_dates: dict[int, date] = {}
        skipped_placeholders = 0

        for event in EVENTS:
            event_date = parse_event_date(ws[event["date_cell"]].value)
            event_dates[event["number"]] = event_date

            for row in range(event["start_row"], event["end_row"] + 1):
                name = clean_text(ws.cell(row, 1).value)
                if not name:
                    continue

                name_key = normalize_name(name)
                if name_key in PLACEHOLDER_NAME_MARKERS:
                    skipped_placeholders += 1
                    continue

                recipients.append(
                    Recipient(
                        source_row=row,
                        event_number=event["number"],
                        event_date=event_date,
                        name=name.strip(" -"),
                        wellbeing_id=normalize_wellbeing_id(ws.cell(row, 2).value),
                        email=clean_text(ws.cell(row, 3).value),
                        gender=clean_text(ws.cell(row, 4).value),
                    )
                )

        # Rows after the fifth source batch are template placeholders, not recipients.
        for row in range(max(e["end_row"] for e in EVENTS) + 1, ws.max_row + 1):
            name = clean_text(ws.cell(row, 1).value)
            if normalize_name(name) in PLACEHOLDER_NAME_MARKERS:
                skipped_placeholders += 1
    finally:
        wb.close()

    # Source-level conflicts.
    wellbeing_names: dict[str, set[str]] = defaultdict(set)
    same_event_names: dict[tuple[int, str], list[Recipient]] = defaultdict(list)
    repeat_identity: dict[tuple[str, str], list[Recipient]] = defaultdict(list)

    for p in recipients:
        if p.wellbeing_id:
            wellbeing_names[p.wellbeing_id].add(normalize_name(p.name))
        same_event_names[(p.event_number, normalize_name(p.name))].append(p)
        repeat_identity[(normalize_name(p.name), p.wellbeing_id or "")].append(p)

    wellbeing_conflicts = {
        wid: names for wid, names in wellbeing_names.items() if len(names) > 1
    }
    duplicate_name_same_event = {
        key: vals for key, vals in same_event_names.items() if len(vals) > 1
    }
    repeat_across_events = {
        key: vals
        for key, vals in repeat_identity.items()
        if len({x.event_number for x in vals}) > 1
    }

    stats = {
        "source_file": path.name,
        "event_dates": event_dates,
        "recipient_rows": len(recipients),
        "unique_normalized_names": len({normalize_name(p.name) for p in recipients}),
        "with_wellbeing_id": sum(1 for p in recipients if p.wellbeing_id),
        "with_email": sum(1 for p in recipients if p.email),
        "skipped_placeholders": skipped_placeholders,
        "wellbeing_conflicts": wellbeing_conflicts,
        "duplicate_name_same_event": duplicate_name_same_event,
        "repeat_across_events": repeat_across_events,
    }
    return recipients, stats


def print_source_audit(recipients: list[Recipient], stats: dict[str, Any]) -> None:
    print("\n=== BIKE GIVEAWAY SOURCE AUDIT ===")
    print(f"Source file                      : {stats['source_file']}")
    print(f"Giveaway sessions                : {len(EVENTS)}")
    print(f"Recipient rows                   : {stats['recipient_rows']}")
    print(f"Unique normalized names          : {stats['unique_normalized_names']}")
    print(f"Rows with Wellbeing Card Number  : {stats['with_wellbeing_id']}")
    print(f"Rows with email                  : {stats['with_email']}")
    print(f"Placeholder rows skipped         : {stats['skipped_placeholders']}")
    print(f"Conflicting Wellbeing IDs        : {len(stats['wellbeing_conflicts'])}")
    print(f"Duplicate names within one event : {len(stats['duplicate_name_same_event'])}")
    print(f"Repeat identities across events  : {len(stats['repeat_across_events'])}")

    print("\nGiveaway batches:")
    for event in EVENTS:
        n = sum(1 for p in recipients if p.event_number == event["number"])
        d = stats["event_dates"][event["number"]]
        print(
            f"  Giveaway {event['number']}: {d.isoformat()} | "
            f"source rows {event['start_row']}-{event['end_row']} | {n} recipient rows"
        )

    if stats["wellbeing_conflicts"]:
        print("\nSource Wellbeing ID conflicts:")
        for wid, names in stats["wellbeing_conflicts"].items():
            print(f"  {wid}: {', '.join(sorted(names))}")

    if stats["duplicate_name_same_event"]:
        print("\nDuplicate names inside the same giveaway:")
        for (event_no, name_key), vals in stats["duplicate_name_same_event"].items():
            rows = ", ".join(str(v.source_row) for v in vals)
            ids = ", ".join(v.wellbeing_id or "-" for v in vals)
            print(f"  Giveaway {event_no}: {name_key} | rows {rows} | ids {ids}")

    if stats["repeat_across_events"]:
        print("\nRepeat exact identities across different giveaways:")
        for (name_key, wid), vals in stats["repeat_across_events"].items():
            evs = ", ".join(f"G{v.event_number}/row{v.source_row}" for v in vals)
            print(f"  {name_key} | {wid or '-'} | {evs}")


# ---------------------------------------------------------------------------
# DB HELPERS
# ---------------------------------------------------------------------------

REQUIRED_COLUMNS = {
    "Participants": {"ParticipantID", "SaheliCardNumber", "FullName", "Email", "Gender"},
    "LiteMembers": {"Id", "MembershipId", "FirstName", "LastName", "Email", "Gender"},
    "Sessions": {
        "SessionId", "Frequency", "Category", "ActivityCategory", "VenueName",
        "ActivityName", "Notes", "IsRecurringWeekly", "SessionDate", "StartTime",
        "EndTime", "IsBookingRequired", "IsCancelled"
    },
    "SessionAttendance": {
        "AttendanceId", "SessionId", "AttendanceMemberKind", "ParticipantId",
        "LiteMemberId", "MemberDisplayId", "SaheliCardNumber", "MemberName",
        "SessionName", "SessionDay", "SessionDate", "SessionMonth",
        "SessionStartTime", "SessionEndTime", "Attended", "Notes"
    },
}


def get_connection():
    if not CONNECTION_STRING:
        raise RuntimeError(
            "Database connection not configured. Set SAHELI_SQL_CONNECTION_STRING "
            "before database preview or --commit."
        )
    try:
        import pyodbc
    except ImportError:
        raise RuntimeError("pyodbc is required. Run: py -m pip install pyodbc")
    return pyodbc.connect(CONNECTION_STRING, autocommit=False)


def table_exists(cur, table_name: str) -> bool:
    row = cur.execute(
        "SELECT 1 FROM INFORMATION_SCHEMA.TABLES "
        "WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME=?",
        table_name,
    ).fetchone()
    return bool(row)


def get_columns(cur, table_name: str) -> set[str]:
    return {
        r[0]
        for r in cur.execute(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME=?",
            table_name,
        ).fetchall()
    }


def preflight_schema(cur) -> None:
    for table, expected in REQUIRED_COLUMNS.items():
        actual = get_columns(cur, table)
        missing = sorted(expected - actual)
        if missing:
            raise RuntimeError(f"dbo.{table} missing required columns: {missing}")


def load_full_members(cur):
    by_name: dict[str, list[DbFull]] = defaultdict(list)
    by_card: dict[str, DbFull] = {}
    by_email: dict[str, list[DbFull]] = defaultdict(list)

    rows = cur.execute(
        "SELECT ParticipantID, SaheliCardNumber, FullName, Email, Gender, "
        "DateOfBirth, Postcode, MobileNumber "
        "FROM dbo.Participants"
    ).fetchall()

    for r in rows:
        p = DbFull(
            participant_id=int(r[0]),
            card=clean_text(r[1]),
            name=clean_text(r[2]) or "",
            email=clean_text(r[3]),
            gender=clean_text(r[4]),
            dob=r[5],
            postcode=clean_text(r[6]),
            mobile=clean_text(r[7]),
        )
        nk = normalize_name(p.name)
        if nk:
            by_name[nk].append(p)
        ck = normalize_card(p.card)
        if ck:
            by_card[ck] = p
        ek = normalize_email(p.email)
        if ek:
            by_email[ek].append(p)
    return by_name, by_card, by_email


def load_lite_members(cur):
    by_name: dict[str, list[DbLite]] = defaultdict(list)
    by_email: dict[str, list[DbLite]] = defaultdict(list)
    by_id: dict[str, DbLite] = {}

    rows = cur.execute(
        "SELECT Id, MembershipId, FirstName, LastName, Email, Gender, "
        "DateOfBirth, Postcode, Phone "
        "FROM dbo.LiteMembers"
    ).fetchall()

    for r in rows:
        p = DbLite(
            lite_id=str(r[0]),
            membership_id=clean_text(r[1]) or "",
            first_name=clean_text(r[2]) or "",
            last_name=clean_text(r[3]) or "",
            email=clean_text(r[4]),
            gender=clean_text(r[5]),
            dob=r[6],
            postcode=clean_text(r[7]),
            phone=clean_text(r[8]),
        )
        nk = normalize_name(p.full_name)
        if nk:
            by_name[nk].append(p)
        ek = normalize_email(p.email)
        if ek:
            by_email[ek].append(p)
        by_id[p.lite_id.lower()] = p

    return by_name, by_email, by_id


def load_cycling_registration_identity_map(cur):
    """
    Supporting identity evidence only.
    A CyclingRegistration is NEVER treated as proof of attendance.
    """
    by_wellbeing: dict[str, list[dict[str, Any]]] = defaultdict(list)

    if not table_exists(cur, "CyclingRegistrations"):
        return by_wellbeing

    cols = get_columns(cur, "CyclingRegistrations")
    needed = {"WellbeingCardNumber", "SaheliCardNumber", "Name"}
    if not needed.issubset(cols):
        return by_wellbeing

    rows = cur.execute(
        "SELECT WellbeingCardNumber, SaheliCardNumber, Name "
        "FROM dbo.CyclingRegistrations "
        "WHERE WellbeingCardNumber IS NOT NULL"
    ).fetchall()

    for r in rows:
        wid = normalize_wellbeing_id(r[0])
        if not wid:
            continue
        by_wellbeing[wid].append(
            {
                "saheli_card": clean_text(r[1]),
                "name": clean_text(r[2]),
            }
        )
    return by_wellbeing


def next_lite_number(cur) -> int:
    mx = 0
    rows = cur.execute(
        "SELECT MembershipId FROM dbo.LiteMembers WITH (UPDLOCK,HOLDLOCK)"
    ).fetchall()
    for (mid,) in rows:
        m = re.fullmatch(r"LITE-(\d+)", clean_text(mid) or "", re.I)
        if m:
            mx = max(mx, int(m.group(1)))
    return mx + 1


def insert_lite(cur, membership_id: str, p: Recipient) -> DbLite:
    first, last = split_name(p.name)
    lid = str(uuid.uuid4())
    cur.execute(
        """
        INSERT INTO dbo.LiteMembers
            (Id, MembershipId, FirstName, LastName, Email, Gender, CreatedAtUtc, CreatedByUserId)
        VALUES
            (?, ?, ?, ?, ?, ?, SYSUTCDATETIME(), NULL)
        """,
        lid,
        membership_id[:100],
        first[:200],
        last[:200],
        (clean_text(p.email) or "")[:400] or None,
        (clean_text(p.gender) or "")[:200] or None,
    )
    return DbLite(
        lite_id=lid,
        membership_id=membership_id,
        first_name=first,
        last_name=last,
        email=p.email,
        gender=p.gender,
        dob=None,
        postcode=None,
        phone=None,
    )


def activity_key(value: Any) -> str:
    return normalize_name(value).replace(" ", "")


def find_existing_session(cur, event_date: date) -> tuple[Optional[DbSession], Optional[str]]:
    rows = cur.execute(
        """
        SELECT SessionId, SessionDate, VenueName, ActivityName, StartTime, EndTime
        FROM dbo.Sessions
        WHERE SessionDate=?
          AND LOWER(REPLACE(REPLACE(ActivityName, ' ', ''), '-', ''))
              IN ('bikegiveaway','bigbikegiveaway')
        ORDER BY SessionId
        """,
        event_date,
    ).fetchall()

    sessions = []
    for r in rows:
        sd = r[1].date() if isinstance(r[1], datetime) else r[1]
        st = r[4].time() if isinstance(r[4], datetime) else r[4]
        et = r[5].time() if isinstance(r[5], datetime) else r[5]
        sessions.append(
            DbSession(
                session_id=int(r[0]),
                session_date=sd,
                venue_name=clean_text(r[2]) or "",
                activity_name=clean_text(r[3]) or ACTIVITY_NAME,
                start_time=st or PLACEHOLDER_START,
                end_time=et or PLACEHOLDER_END,
            )
        )

    if not sessions:
        return None, None
    if len(sessions) == 1:
        return sessions[0], None

    return None, (
        f"{len(sessions)} existing Bike Giveaway sessions found on {event_date}; "
        "refusing to choose one automatically."
    )


def create_session(cur, event_number: int, event_date: date) -> DbSession:
    notes = (
        f"{MIGRATION_NOTE_PREFIX}; source={SOURCE_FILENAME}/{SOURCE_SHEET}; "
        f"giveaway={event_number}; source_venue_not_recorded=1; "
        f"venue_placeholder={DEFAULT_VENUE}; source_time_not_recorded=1; "
        f"time_quality=PLACEHOLDER_12_00_13_00"
    )[:1000]

    cur.execute(
        """
        INSERT INTO dbo.Sessions
            (Frequency, Category, SubCategory, ActivityCategory, VenueName,
             ActivityName, Notes, IsRecurringWeekly, DayOfWeek, SessionDate,
             ArrivalTime, StartTime, EndTime, Capacity, IsBookingRequired,
             IsCancelled, CreatedAtUtc)
        OUTPUT INSERTED.SessionId
        VALUES
            (?, ?, NULL, ?, ?, ?, ?, 0, NULL, ?, NULL, ?, ?, NULL, 0, 0, SYSUTCDATETIME())
        """,
        DEFAULT_FREQUENCY,
        DEFAULT_CATEGORY,
        DEFAULT_ACTIVITY_CATEGORY,
        DEFAULT_VENUE,
        ACTIVITY_NAME,
        notes,
        event_date,
        PLACEHOLDER_START,
        PLACEHOLDER_END,
    )

    sid = int(cur.fetchone()[0])
    return DbSession(
        session_id=sid,
        session_date=event_date,
        venue_name=DEFAULT_VENUE,
        activity_name=ACTIVITY_NAME,
        start_time=PLACEHOLDER_START,
        end_time=PLACEHOLDER_END,
    )


def attendance_exists(cur, session_id: int, member: ResolvedMember) -> bool:
    if member.kind == "FULL":
        row = cur.execute(
            "SELECT TOP 1 AttendanceId FROM dbo.SessionAttendance "
            "WHERE SessionId=? AND ParticipantId=?",
            session_id,
            int(member.member_id),
        ).fetchone()
    else:
        row = cur.execute(
            "SELECT TOP 1 AttendanceId FROM dbo.SessionAttendance "
            "WHERE SessionId=? AND LiteMemberId=?",
            session_id,
            member.member_id,
        ).fetchone()
    return bool(row)


def insert_attendance(cur, session: DbSession, p: Recipient, member: ResolvedMember) -> None:
    full = member.kind == "FULL"
    participant_id = int(member.member_id) if full else None
    lite_id = None if full else member.member_id
    card = member.display_id if full else None

    notes = (
        f"{MIGRATION_NOTE_PREFIX}; source={SOURCE_FILENAME}/{SOURCE_SHEET}/row{p.source_row}; "
        f"giveaway={p.event_number}; wellbeing_card={p.wellbeing_id or ''}; "
        f"event_assignment=SOURCE_ROW_BATCH"
    )[:2000]

    cur.execute(
        """
        INSERT INTO dbo.SessionAttendance
            (SessionId, AttendanceMemberKind, ParticipantId, LiteMemberId,
             MemberDisplayId, SaheliCardNumber, MemberName,
             SessionName, SessionDay, SessionDate, SessionMonth,
             SessionStartTime, SessionEndTime, Attended, Notes, CreatedAtUtc)
        VALUES
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, SYSUTCDATETIME())
        """,
        session.session_id,
        member.kind,
        participant_id,
        lite_id,
        member.display_id[:100],
        card[:100] if card else None,
        member.name[:400],
        session.activity_name[:400],
        p.event_date.strftime("%A"),
        p.event_date,
        p.event_date.strftime("%B"),
        session.start_time,
        session.end_time,
        notes,
    )

# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------

class MigrationLog:
    def __init__(self):
        self.rows: list[dict[str, Any]] = []
        self.counts = Counter()

    def add(
        self,
        action: str,
        p: Optional[Recipient] = None,
        detail: str = "",
        session_id: Optional[int] = None,
        member_ref: str = "",
    ) -> None:
        self.counts[action] += 1
        self.rows.append(
            {
                "Action": action,
                "Giveaway": p.event_number if p else "",
                "Date": p.event_date.isoformat() if p else "",
                "SourceRow": p.source_row if p else "",
                "SourceName": p.name if p else "",
                "WellbeingCardNumber": p.wellbeing_id if p else "",
                "Email": p.email if p else "",
                "Gender": p.gender if p else "",
                "SessionId": session_id or "",
                "MemberRef": member_ref,
                "Detail": detail,
            }
        )

    def write(self, path: Path) -> None:
        fields = [
            "Action", "Giveaway", "Date", "SourceRow", "SourceName",
            "WellbeingCardNumber", "Email", "Gender", "SessionId",
            "MemberRef", "Detail"
        ]
        with path.open("w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            w.writerows(self.rows)

# ---------------------------------------------------------------------------
# MEMBER RESOLUTION
# ---------------------------------------------------------------------------

def describe_full(x: DbFull) -> str:
    return (
        f"FULL:{x.card or x.participant_id}"
        f"[pid={x.participant_id}; gender={x.gender or '-'}; email={x.email or '-'}; "
        f"dob={x.dob or '-'}; postcode={x.postcode or '-'}; mobile={x.mobile or '-'}]"
    )


def describe_lite(x: DbLite) -> str:
    return (
        f"LITE:{x.membership_id}"
        f"[id={x.lite_id}; gender={x.gender or '-'}; email={x.email or '-'}; "
        f"dob={x.dob or '-'}; postcode={x.postcode or '-'}; phone={x.phone or '-'}]"
    )


def resolve_by_known_gender(
    p: Recipient,
    full_matches: list[DbFull],
    lite_matches: list[DbLite],
) -> Optional[ResolvedMember]:
    """
    Gender is only used when it is decisive and every candidate has a known
    gender. A blank CRM gender never gets eliminated by assumption.
    """
    source_gender = normalize_gender(p.gender)
    if not source_gender:
        return None

    candidates = (
        [("FULL", x, normalize_gender(x.gender)) for x in full_matches]
        + [("LITE", x, normalize_gender(x.gender)) for x in lite_matches]
    )
    if not candidates or any(not g for _, _, g in candidates):
        return None

    same = [(kind, x) for kind, x, g in candidates if g == source_gender]
    if len(same) != 1:
        return None

    kind, x = same[0]
    if kind == "FULL":
        return ResolvedMember(
            "FULL", str(x.participant_id),
            x.card or str(x.participant_id), x.name,
            "REUSE_FULL_BY_EXACT_NAME_AND_GENDER"
        )
    return ResolvedMember(
        "LITE", x.lite_id, x.membership_id, x.full_name,
        "REUSE_LITE_BY_EXACT_NAME_AND_GENDER"
    )


def resolve_member(
    cur,
    p: Recipient,
    source_stats: dict[str, Any],
    full_by_name,
    full_by_card,
    full_by_email,
    lite_by_name,
    lite_by_email,
    cycling_by_wellbeing,
    lite_num_state: list[int],
    log: MigrationLog,
) -> Optional[ResolvedMember]:

    name_key = normalize_name(p.name)
    email_key = normalize_email(p.email)

    # A conflicting source Wellbeing ID is treated as an unreliable source field,
    # not as a reason to discard the person's name. We NEVER turn it into a
    # Saheli Card Number. Continue matching using independent CRM evidence.
    wellbeing_conflicted = bool(
        p.wellbeing_id and p.wellbeing_id in source_stats["wellbeing_conflicts"]
    )
    if wellbeing_conflicted:
        log.add(
            "WARN_SOURCE_WELLBEING_ID_CONFLICT_IGNORED",
            p,
            detail=(
                f"Wellbeing ID {p.wellbeing_id} is used for multiple source names: "
                + ", ".join(sorted(source_stats["wellbeing_conflicts"][p.wellbeing_id]))
                + ". ID ignored for member resolution; name/email/gender rules continue."
            ),
        )

    # The same person may legitimately appear in more than one giveaway session.
    # Therefore repeat identities across different giveaway dates are not blocked.
    #
    # Within ONE giveaway, however, the same normalized name with different
    # Wellbeing IDs may represent two different people. Strong evidence (email or
    # Wellbeing->SaheliCard link) may resolve them. If not, we only auto-create
    # separate Lite members when there are no existing same-name CRM candidates.
    same_event = source_stats["duplicate_name_same_event"].get((p.event_number, name_key), [])
    same_event_distinct_ids = {
        x.wellbeing_id for x in same_event if x.wellbeing_id
    }

    # 1) Strongest evidence: source Wellbeing ID -> CyclingRegistration -> Saheli Card -> FULL member.
    if p.wellbeing_id and not wellbeing_conflicted:
        regs = cycling_by_wellbeing.get(p.wellbeing_id, [])
        cards = {
            normalize_card(x.get("saheli_card"))
            for x in regs
            if normalize_card(x.get("saheli_card"))
        }
        full_matches = {
            full_by_card[c].participant_id: full_by_card[c]
            for c in cards
            if c in full_by_card
        }
        if len(full_matches) == 1:
            dbp = next(iter(full_matches.values()))
            if not names_compatible(p.name, dbp.name):
                log.add(
                    "REVIEW_WELLBEING_TO_FULL_NAME_MISMATCH",
                    p,
                    detail=(
                        f"CyclingRegistration links Wellbeing ID to Saheli Card {dbp.card}, "
                        f"but CRM name is '{dbp.name}'."
                    ),
                )
                return None
            return ResolvedMember(
                kind="FULL",
                member_id=str(dbp.participant_id),
                display_id=dbp.card or str(dbp.participant_id),
                name=dbp.name or p.name,
                action="REUSE_FULL_BY_WELLBEING_LINK",
            )
        if len(full_matches) > 1:
            log.add(
                "REVIEW_WELLBEING_MULTIPLE_FULL_MATCHES",
                p,
                detail="Wellbeing ID maps through CyclingRegistrations to multiple FULL CRM members.",
            )
            return None

    # 2) Unique exact email match, but only when name is compatible.
    if email_key:
        email_full = full_by_email.get(email_key, [])
        email_lite = lite_by_email.get(email_key, [])
        compatible_full = [x for x in email_full if names_compatible(p.name, x.name)]
        compatible_lite = [x for x in email_lite if names_compatible(p.name, x.full_name)]
        total = len(compatible_full) + len(compatible_lite)
        if total == 1:
            if compatible_full:
                x = compatible_full[0]
                return ResolvedMember(
                    "FULL", str(x.participant_id),
                    x.card or str(x.participant_id), x.name, "REUSE_FULL_BY_EMAIL"
                )
            x = compatible_lite[0]
            return ResolvedMember(
                "LITE", x.lite_id, x.membership_id,
                x.full_name, "REUSE_LITE_BY_EMAIL"
            )
        if total > 1:
            log.add(
                "REVIEW_AMBIGUOUS_EMAIL_MATCH",
                p,
                detail=f"Email matches {total} compatible CRM members.",
            )
            return None

    # 3) Exact normalized name across FULL + LITE.
    full_name_matches = full_by_name.get(name_key, [])
    lite_name_matches = lite_by_name.get(name_key, [])
    total_name_matches = len(full_name_matches) + len(lite_name_matches)

    # Same name appears more than once in the SAME source giveaway with distinct
    # source IDs. Do not collapse two source identities onto one CRM person.
    if (
        len(same_event) > 1
        and len(same_event_distinct_ids) > 1
        and total_name_matches > 0
    ):
        candidates = (
            [describe_full(x) for x in full_name_matches]
            + [describe_lite(x) for x in lite_name_matches]
        )
        log.add(
            "REVIEW_SAME_NAME_MULTIPLE_SOURCE_IDENTITIES",
            p,
            detail=(
                f"{len(same_event)} rows share this name in Giveaway {p.event_number} "
                f"with distinct Wellbeing IDs={sorted(same_event_distinct_ids)}. "
                "At least one same-name CRM member already exists, so the script cannot "
                "safely decide which source identity belongs to which CRM record. "
                "Candidates: " + " | ".join(candidates)
            ),
        )
        return None

    if total_name_matches == 1:
        if full_name_matches:
            x = full_name_matches[0]
            return ResolvedMember(
                "FULL", str(x.participant_id),
                x.card or str(x.participant_id), x.name, "REUSE_FULL_BY_EXACT_NAME"
            )
        x = lite_name_matches[0]
        return ResolvedMember(
            "LITE", x.lite_id, x.membership_id,
            x.full_name, "REUSE_LITE_BY_EXACT_NAME"
        )

    if total_name_matches > 1:
        by_gender = resolve_by_known_gender(p, full_name_matches, lite_name_matches)
        if by_gender:
            return by_gender

        candidates = (
            [describe_full(x) for x in full_name_matches]
            + [describe_lite(x) for x in lite_name_matches]
        )
        log.add(
            "REVIEW_AMBIGUOUS_EXISTING_MEMBER",
            p,
            detail=(
                f"Exact source name matches {total_name_matches} CRM members. "
                f"Source gender={p.gender or '-'}; source email={p.email or '-'}. "
                "Candidates: " + " | ".join(candidates)
            ),
        )
        return None

    # 4) No safe existing match -> create a new Lite member.
    # If the same name occurs more than once in this giveaway with different
    # Wellbeing IDs, distinct cache keys will create distinct Lite identities.
    mid = f"LITE-{lite_num_state[0]}"
    lite_num_state[0] += 1
    try:
        new_lite = insert_lite(cur, mid, p)
    except Exception as exc:
        log.add("REVIEW_CREATE_LITE_FAILED", p, detail=str(exc))
        return None

    lite_by_name[name_key].append(new_lite)
    if email_key:
        lite_by_email[email_key].append(new_lite)

    log.add(
        "CREATED_LITE",
        p,
        detail=(
            f"MembershipId={mid}; source Wellbeing ID retained only in migration/attendance notes, "
            "not used as Saheli Card Number."
        ),
        member_ref=f"LITE:{new_lite.lite_id}",
    )
    return ResolvedMember(
        "LITE",
        new_lite.lite_id,
        new_lite.membership_id,
        new_lite.full_name,
        "CREATED_LITE",
    )

# ---------------------------------------------------------------------------
# MIGRATION
# ---------------------------------------------------------------------------

def run_migration(commit: bool, audit_only: bool) -> int:
    recipients, stats = parse_source()
    print("Saheli CRM - Bike Giveaway Historical Migration V1.1")
    print(f"Source directory: {BASE_DIR}")
    print(f"  OK source: {SOURCE_FILENAME}")
    print_source_audit(recipients, stats)

    if audit_only:
        print("\nMode: SOURCE AUDIT ONLY - no database connection made.")
        return 0

    conn = get_connection()
    log = MigrationLog()

    try:
        cur = conn.cursor()
        preflight_schema(cur)

        full_by_name, full_by_card, full_by_email = load_full_members(cur)
        lite_by_name, lite_by_email, _ = load_lite_members(cur)
        cycling_by_wellbeing = load_cycling_registration_identity_map(cur)
        lite_num_state = [next_lite_number(cur)]

        print("\n=== DATABASE PREFLIGHT ===")
        print(f"Existing FULL participants loaded    : {sum(len(v) for v in full_by_name.values()):,}")
        print(f"Existing LITE members loaded         : {sum(len(v) for v in lite_by_name.values()):,}")
        print(f"Cycling Wellbeing identity keys      : {len(cycling_by_wellbeing):,}")
        print(f"Next reserved Lite membership number : {lite_num_state[0]:,}")
        print("Cycling registrations used as attendance: 0")

        sessions_by_event: dict[int, Optional[DbSession]] = {}

        # Sessions first.
        for event in EVENTS:
            event_no = event["number"]
            event_date = stats["event_dates"][event_no]

            existing, error = find_existing_session(cur, event_date)
            if error:
                # Attach one representative source row so the CSV is useful.
                representative = next(
                    (p for p in recipients if p.event_number == event_no), None
                )
                log.add("REVIEW_SESSION_AMBIGUOUS", representative, detail=error)
                sessions_by_event[event_no] = None
                continue

            if existing:
                sessions_by_event[event_no] = existing
                representative = next(
                    (p for p in recipients if p.event_number == event_no), None
                )
                log.add(
                    "EXISTING_SESSION",
                    representative,
                    detail=f"Reused SessionId={existing.session_id}",
                    session_id=existing.session_id,
                )
            else:
                created = create_session(cur, event_no, event_date)
                sessions_by_event[event_no] = created
                representative = next(
                    (p for p in recipients if p.event_number == event_no), None
                )
                log.add(
                    "NEW_SESSION",
                    representative,
                    detail=(
                        f"Created Bike Giveaway session; venue={DEFAULT_VENUE}; "
                        "source did not record venue/time."
                    ),
                    session_id=created.session_id,
                )

        # Participants + attendance.
        member_cache: dict[tuple[str, str, str], Optional[ResolvedMember]] = {}

        for p in recipients:
            session = sessions_by_event.get(p.event_number)
            if not session:
                log.add(
                    "REVIEW_ATTENDANCE_BLOCKED_BY_SESSION",
                    p,
                    detail="Giveaway session is unresolved.",
                )
                continue

            cache_key = (
                normalize_name(p.name),
                p.wellbeing_id or "",
                normalize_email(p.email),
            )

            if cache_key in member_cache:
                member = member_cache[cache_key]
            else:
                member = resolve_member(
                    cur,
                    p,
                    stats,
                    full_by_name,
                    full_by_card,
                    full_by_email,
                    lite_by_name,
                    lite_by_email,
                    cycling_by_wellbeing,
                    lite_num_state,
                    log,
                )
                if member is not None:
                    member_cache[cache_key] = member

            if member is None:
                # resolve_member already emitted a REVIEW_* reason for THIS row.
                continue

            if attendance_exists(cur, session.session_id, member):
                log.add(
                    "ALREADY_IN_CRM",
                    p,
                    detail=f"{member.action}; attendance already exists",
                    session_id=session.session_id,
                    member_ref=f"{member.kind}:{member.display_id}",
                )
                continue

            insert_attendance(cur, session, p, member)
            log.add(
                "NEW_ATTENDANCE",
                p,
                detail=member.action,
                session_id=session.session_id,
                member_ref=f"{member.kind}:{member.display_id}",
            )

        # Every source recipient row must finish with one terminal row-level
        # outcome: NEW_ATTENDANCE, ALREADY_IN_CRM, or REVIEW_*.
        source_rows = {p.source_row for p in recipients}
        terminal_rows = {
            int(r["SourceRow"])
            for r in log.rows
            if r.get("SourceRow") not in ("", None)
            and (
                r["Action"] in {"NEW_ATTENDANCE", "ALREADY_IN_CRM"}
                or r["Action"].startswith("REVIEW_")
            )
        }
        missing_rows = sorted(source_rows - terminal_rows)
        if missing_rows:
            by_row = {p.source_row: p for p in recipients}
            for row_no in missing_rows:
                log.add(
                    "REVIEW_INTERNAL_UNACCOUNTED_SOURCE_ROW",
                    by_row[row_no],
                    detail="Internal safety check: source row has no terminal migration outcome.",
                )
            terminal_rows.update(missing_rows)

        review_count = sum(
            count for action, count in log.counts.items()
            if action.startswith("REVIEW_")
        )

        print("\n=== RECIPIENT RECONCILIATION ===")
        print(f"Source recipient rows            : {len(source_rows):,}")
        print(f"Rows with terminal outcome       : {len(terminal_rows):,}")
        print(f"Unaccounted source rows          : {len(source_rows - terminal_rows):,}")

        print("\n=== ACTION SUMMARY ===")
        for action in sorted(log.counts):
            print(f"{action:42s} {log.counts[action]:,}")
        print(f"\nReview-required rows/actions: {review_count:,}")

        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        mode = "commit" if commit else "preview"
        report = BASE_DIR / f"bike_giveaway_migration_{mode}_{stamp}.csv"
        log.write(report)
        print(f"Detailed migration report     : {report}")

        if review_count:
            conn.rollback()
            print(
                "\nMode: PREVIEW ONLY - transaction rolled back because REVIEW_* items remain."
            )
            if commit:
                print("COMMIT REFUSED: resolve all REVIEW_* items first.")
                return 2
            return 0

        if commit:
            conn.commit()
            print("\nMode: COMMITTED")
        else:
            conn.rollback()
            print("\nMode: PREVIEW ONLY - transaction rolled back; database unchanged.")
            print("Run again with --commit only after the preview summary/report is correct.")

        return 0

    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Migrate Bike Giveaway sessions and recipients into Saheli CRM."
    )
    parser.add_argument(
        "--audit-only",
        action="store_true",
        help="Parse and audit the Excel source only. No DB connection.",
    )
    parser.add_argument(
        "--commit",
        action="store_true",
        help="Commit only when REVIEW_* = 0. Default is rollback preview.",
    )
    args = parser.parse_args()

    if args.audit_only and args.commit:
        parser.error("--audit-only and --commit cannot be used together.")

    return run_migration(commit=args.commit, audit_only=args.audit_only)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nCancelled.", file=sys.stderr)
        raise SystemExit(130)
