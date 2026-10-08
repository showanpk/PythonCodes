#!/usr/bin/env python3
"""Saheli CRM / Innerva: manual, incremental Excel -> Azure SQL sync.

Read-only preview by default. Only Innerva sessions and their booking/attendance
records are candidates for update. Never changes other CRM activity sessions.
Requires: pip install openpyxl pyodbc
Connection: SAHELI_SQL_CONNECTION_STRING (no passwords embedded in source).

The parser supports the original block-of-nine Innerva sheet layout. It must be
validated against the staff's *actual* current .xlsx before production import.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import os
import re
import sys
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, time
from pathlib import Path
from typing import Any, Optional

from openpyxl import load_workbook

VENUE = "Alum Rock Community Centre"
REPORT_DIR = Path(__file__).resolve().parent / "reports"
INNERVA_NAMES = {"innerva", "mens innerva", "mens innerva session", "innerva mix"}
BAD = {"", "0", "none", "null", "nan", "n a", "ref", "value", "name", "error"}
YES = {"yes", "y", "true", "1", "attended", "x"}
NO = {"no", "n", "false", "0", "not attended"}
CANCEL = {"cancelled", "canceled", "do not book", "bank holiday", "no session",
          "no class", "staff training", "closed", "closure", "holiday", "eid", "unavailable"}
MONTHS = {n.lower(): i for i, n in enumerate(
    ("", "January", "February", "March", "April", "May", "June", "July",
     "August", "September", "October", "November", "December")) if n}
MONTH_SHORT = {n[:3]: v for n, v in MONTHS.items()}


def clean(value: Any) -> str:
    if value is None:
        return ""
    v = str(value).replace("\xa0", " ").strip()
    if v.startswith("#") or norm(v) in BAD:
        return ""
    return re.sub(r"\s+", " ", v)


def norm(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str("" if value is None else value).lower().replace("’", "").replace("'", "")).strip()


def hd(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())


def norm_name(value: Any) -> str:
    return re.sub(r"^(mr|mrs|ms|miss|dr)\s+", "", norm(value))


def card_key(value: Any) -> str:
    s = clean(value).upper()
    s = re.sub(r"^SAH(?:ELI)?[-\s]*", "", s)
    if re.fullmatch(r"\d+\.0+", s):
        s = s.partition(".")[0]
    if s in {"MEMBER", "EXISTING MEMBER", "ALREADY MEMBER"}:
        return ""
    return re.sub(r"\s+", "", s)


def date_value(v: Any) -> Optional[date]:
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    t = clean(v)
    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%d/%m/%y"):
        try:
            return datetime.strptime(t, fmt).date()
        except ValueError:
            pass
    return None


def month_num(value: Any) -> Optional[int]:
    v = norm(value)
    for k, n in MONTHS.items():
        if v == k or v.startswith(k + " "):
            return n
    return MONTH_SHORT.get(v[:3])


def sheet_year_month(name: str) -> tuple[Optional[int], Optional[int]]:
    low = norm(name)
    found = re.search(r"\b(20\d{2})\b", low)
    if found:
        return int(found.group(1)), month_num(low)
    found = re.search(r"\b([a-z]+)\s+(\d{2})\b", low)
    if found and month_num(found.group(1)):
        return 2000 + int(found.group(2)), month_num(found.group(1))
    return None, None


def booking_sheet_kind(title: str, current_year: Optional[int]) -> str:
    """Read only verified source tabs, not report/pivot/helper copies."""
    simple = norm(title)
    if re.fullmatch(r"20\d{2}", simple):
        return "booking"
    if re.fullmatch(r"(?:january|february|march|april|may|june|july|august|september|october|november|december) (?:20\d{2}|\d{2})", simple):
        return "booking"
    if simple == "current":
        return "booking" if current_year else "ambiguous_current"
    return "helper"


def slot_date(raw: Any, month: Any, sheet: str, current_year: Optional[int]) -> date:
    yr, mo = sheet_year_month(sheet)
    full = date_value(raw)
    if full:
        if yr and full.year != yr:
            raise ValueError(f"date year {full.year} conflicts with sheet year {yr}")
        return full
    year = yr or current_year
    mon = month_num(month) or mo
    if not year or not mon:
        raise ValueError("missing year/month; use --current-year for a 'Current' sheet")
    m = re.search(r"(\d{1,2})", clean(raw))
    if not m:
        raise ValueError(f"invalid date/day: {str(raw)[:30]}")
    return date(year, mon, int(m.group(1)))


def clock(token: str) -> time:
    raw = token.lower().replace(".", ":").replace(" ", "")
    m = re.fullmatch(r"(\d{1,2})(?::(\d{1,2}))?(am|pm)?", raw)
    if not m:
        raise ValueError(f"invalid time: {token!r}")
    h, minute = int(m.group(1)), int(m.group(2) or 0)
    if m.group(3):
        if not 1 <= h <= 12:
            raise ValueError(f"invalid AM/PM time: {token}")
        h = h % 12 + (12 if m.group(3) == "pm" else 0)
    return time(h, minute)


def time_range(value: Any) -> tuple[time, time]:
    v = clean(value).replace("–", "-").replace("—", "-")
    m = re.search(r"(\d{1,2}(?:[:.]\d{1,2})?\s*(?:am|pm)?)\s*-\s*"
                  r"(\d{1,2}(?:[:.]\d{1,2})?\s*(?:am|pm)?)", v, re.I)
    if not m:
        raise ValueError(f"unrecognised time range {v!r}")
    left, right = m.group(1), m.group(2)
    suffix = re.search(r"(am|pm)\s*$", right, re.I)
    if suffix and not re.search(r"(am|pm)\s*$", left, re.I):
        left += suffix.group(1)
    st, en = clock(left), clock(right)
    if en <= st and st.hour < 12 and en.hour < 12:
        en = time(en.hour + 12, en.minute)
    if en <= st:
        raise ValueError(f"end time must be later than start time: {v}")
    duration = (en.hour * 60 + en.minute) - (st.hour * 60 + st.minute)
    if st.hour < 6 or duration > 180:
        raise ValueError(f"suspicious Innerva time range {v!r}; needs manual correction")
    return st, en


def clock_maybe(value: Any) -> Optional[time]:
    if isinstance(value, time):
        return value
    if isinstance(value, datetime):
        return value.time()
    try:
        return clock(clean(value)) if clean(value) else None
    except ValueError:
        return None


def cancelled(value: Any) -> bool:
    t = norm(value)
    if not t or t == "closed session high risk only":
        return False
    # A restriction like "NO INDUCTION" is NOT a cancellation.
    return any(t == x or t.startswith(x + " ") for x in CANCEL)


def attendance(value: Any) -> Optional[bool]:
    v = norm(value)
    if v in YES or str(value).strip() in {"✓", "✔"}:
        return True
    if v in NO:
        return False
    if not clean(value):
        return None
    if v in YES:
        return True
    if v in NO:
        return False
    raise ValueError(f"unknown attendance status {str(value)[:40]!r}")


def bool_maybe(value: Any) -> Optional[bool]:
    v = norm(value)
    if v in YES:
        return True
    if v in NO:
        return False
    return None


def sql_time(value: Any) -> time:
    if isinstance(value, datetime):
        return value.time().replace(microsecond=0)
    if isinstance(value, time):
        return value.replace(microsecond=0)
    return time.fromisoformat(str(value))


@dataclass
class Member:
    source_row: int
    card: str
    name: str
    dob: Optional[date]
    attended: Optional[bool]
    risk: str
    induction_signed: Optional[bool]
    health: str
    issues: str = ""

    @property
    def key(self) -> str:
        return "C:" + self.card if self.card else "N:" + norm_name(self.name)


@dataclass
class Slot:
    day: date
    start: time
    end: time
    kind: str
    lead: str
    induction_time: Optional[time]
    is_cancelled: bool
    sheet: str
    first_row: int
    people: list[Member] = field(default_factory=list)

    @property
    def key(self) -> tuple:
        return self.day, self.start, self.end


class Report:
    def __init__(self):
        self.rows: list[dict] = []
        self.count: Counter = Counter()

    def add(self, status: str, slot: Optional[Slot] = None, person: Optional[Member] = None,
            detail: str = "", session_id: Any = "", source_sheet: str = "",
            source_row: Any = "") -> None:
        self.count[status] += 1
        self.rows.append({
            "Action": status, "Date": slot.day.isoformat() if slot else "",
            "Time": f"{slot.start}-{slot.end}" if slot else "",
            "Sheet": slot.sheet if slot else source_sheet,
            "ExcelRow": person.source_row if person else (slot.first_row if slot else source_row),
            "SessionId": session_id, "SaheliCard": person.card if person else "",
            "Name": person.name if person else "", "Detail": detail,
        })

    def save(self, filename: Path) -> None:
        filename.parent.mkdir(parents=True, exist_ok=True)
        with filename.open("w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=list(self.rows[0]) if self.rows else
                                    ["Action", "Date", "Time", "Sheet", "ExcelRow",
                                     "SessionId", "SaheliCard", "Name", "Detail"])
            writer.writeheader()
            writer.writerows(self.rows)


def parse_workbook(path: Path, args, report: Report) -> list[Slot]:
    workbook = load_workbook(path, read_only=True, data_only=True)
    raw_slots: list[Slot] = []
    parsed_sheets = 0
    for ws in workbook.worksheets:
        sheet_kind = booking_sheet_kind(ws.title, args.current_year)
        if sheet_kind != "booking":
            status = "SKIP_AMBIGUOUS_CURRENT_SHEET" if sheet_kind == "ambiguous_current" else "SKIP_HELPER_SHEET"
            report.add(status, detail=f"{ws.title}: excluded from booking import" +
                       ("; use --current-year if this really is the active register" if sheet_kind == "ambiguous_current" else ""))
            continue
        # Find header within top rows instead of assuming row 1.
        header = None
        for rownum, values in enumerate(ws.iter_rows(min_row=1, max_row=min(ws.max_row, 15),
                                                     max_col=min(ws.max_column, 35),
                                                     values_only=True), 1):
            labels = [hd(v) for v in values]
            if "spaces" in labels and "attended" in labels and "sahelicardnumber" in labels:
                header = (rownum, labels)
                break
        if not header:
            continue
        parsed_sheets += 1
        start_row, labels = header
        idx = {v: k for k, v in enumerate(labels) if v}
        # Legacy 2024 worksheets use 'Session' twice: first for date,
        # second for the clock range. The last 'Session' is the time.
        session_cols = [i for i, v in enumerate(labels) if v == "session"]
        if "date" not in idx and "sessiondate" not in idx and len(session_cols) >= 2:
            idx["date"] = session_cols[0]
            idx["session"] = session_cols[-1]
        elif "date" not in idx and "sessiondate" in idx:
            idx["date"] = idx["sessiondate"]

        def field(row: tuple, name: str, default: Any = None) -> Any:
            i = idx.get(hd(name))
            return row[i] if i is not None and i < len(row) else default

        # Require the date, session slot and name fields; don't guess shifted columns.
        missing = [x for x in ("date", "Session", "Name") if hd(x) not in idx]
        if missing:
            report.add("REVIEW_BAD_HEADER", detail=f"{ws.title}: missing {missing}",
                       source_sheet=ws.title, source_row=start_row)
            continue
        current: Optional[Slot] = None
        for rownum, values in enumerate(ws.iter_rows(min_row=start_row + 1,
                                                     max_col=min(ws.max_column, 35),
                                                     values_only=True), start_row + 1):
            if not any(v is not None for v in values):
                continue
            raw_pos = field(values, "Spaces")
            try:
                position = int(raw_pos)
            except (ValueError, TypeError):
                continue
            if position == 1:
                current = None
                try:
                    d = slot_date(field(values, "date"), field(values, "Month"),
                                  ws.title, args.current_year)
                    start, end = time_range(field(values, "Session"))
                    if args.start and d < args.start:
                        continue
                    if args.end and d > args.end:
                        continue
                    status = (cancelled(field(values, "Induction Time")) or
                              cancelled(field(values, "Session")) or
                              cancelled(field(values, "Name")))
                    current = Slot(d, start, end, clean(field(values, "Session Type")),
                                   clean(field(values, "Lead")),
                                   clock_maybe(field(values, "Induction Time")),
                                   status, ws.title, rownum)
                    raw_slots.append(current)
                except (ValueError, TypeError) as e:
                    report.add("REVIEW_BAD_SLOT", detail=f"{ws.title} row {rownum}: {e}",
                               source_sheet=ws.title, source_row=rownum)
                    continue
            if current is None or current.is_cancelled or not 1 <= position <= 9:
                continue
            card = card_key(field(values, "Saheli Card Number"))
            name = clean(field(values, "Name"))
            if not card and not name:
                continue
            try:
                att = attendance(field(values, "Attended"))
            except ValueError as exc:
                report.add("REVIEW_ATTENDANCE_VALUE", current,
                           detail=f"row {rownum}: {exc}")
                continue
            if re.search(r"[/,;&]", card):
                report.add("REVIEW_COMPOSITE_CARD", current,
                           detail=f"row {rownum}: {card}")
                continue
            current.people.append(Member(rownum, card, name,
                                         date_value(field(values, "Date of birth")), att,
                                         clean(field(values, "Risk Stratification")),
                                         bool_maybe(field(values, "Signed Induction Paper")),
                                         clean(field(values, "Medical condition affecting use of machine")),
                                         clean(field(values, "Any Issues during session"))))
    workbook.close()
    if not parsed_sheets:
        raise RuntimeError("No Innerva 'Spaces / Attended / Saheli Card Number' worksheets found")

    # Merge identical *source* slots, but never merge two different Session Types.
    groups: dict[tuple, Slot] = {}
    incompatible: set[tuple] = set()
    for slot in raw_slots:
        key = slot.key
        prior = groups.get(key)
        if not prior:
            groups[key] = slot
        elif (norm(prior.kind) != norm(slot.kind) or prior.is_cancelled != slot.is_cancelled):
            incompatible.add(key)
        else:
            prior.people.extend(slot.people)
    for key in incompatible:
        slot = groups.pop(key)
        report.add("REVIEW_CONFLICTING_EXCEL_SLOTS", slot,
                   detail="Same date/time, incompatible type or cancellation")
    for slot in groups.values():
        if slot.is_cancelled:
            continue
        dedup: dict[str, Member] = {}
        bad: set[str] = set()
        for member in slot.people:
            k = member.key
            if k not in dedup:
                dedup[k] = member
            else:
                prev = dedup[k]
                if prev.attended != member.attended or (prev.name and member.name and
                        norm_name(prev.name) != norm_name(member.name)):
                    bad.add(k)
                else:
                    report.add("SKIP_DUPLICATE_EXCEL_BOOKING", slot, member)
        for k in bad:
            report.add("REVIEW_EXCEL_MEMBER_CONFLICT", slot, dedup[k],
                       detail="Duplicate member in same slot with differing values")
            dedup.pop(k, None)
        slot.people = list(dedup.values())
        if len(slot.people) > 9:
            report.add("REVIEW_EXCEL_CAPACITY", slot,
                       detail=f"{len(slot.people)} people in a nine-space booking slot")
    return sorted(groups.values(), key=lambda s: (s.day, s.start, s.end))


def check_schema(cur) -> None:
    expected = {
        "Sessions": {"SessionId", "SessionDate", "VenueName", "ActivityName", "StartTime", "EndTime",
                     "IsBookingRequired", "IsCancelled", "Capacity", "Category", "SubCategory", "ActivityCategory",
                     "Frequency", "Notes", "IsRecurringWeekly", "DayOfWeek", "CreatedAtUtc"},
        "SessionAttendance": {"SessionId", "AttendanceMemberKind", "ParticipantId", "LiteMemberId",
                              "MemberDisplayId", "SaheliCardNumber", "MemberName", "SessionName",
                              "SessionDate", "SessionDay", "SessionMonth", "SessionStartTime",
                              "SessionEndTime", "Attended", "SignedInductionPaper", "MedicalCondition",
                              "RiskStratification", "CreatedAtUtc", "Notes"},
        "Participants": {"ParticipantID", "SaheliCardNumber", "FullName", "DateOfBirth", "Site", "Notes", "CreatedAt"},
        "LiteMembers": {"Id", "MembershipId", "FirstName", "LastName", "DateOfBirth", "CreatedAtUtc"},
    }
    for table, columns in expected.items():
        actual = {r[0] for r in cur.execute(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME=?", table)}
        absent = columns - actual
        if absent:
            raise RuntimeError(f"dbo.{table}: missing columns {sorted(absent)}")


def load_database(cur, slots: list[Slot]) -> dict:
    start, end = slots[0].day, slots[-1].day
    sessions = defaultdict(list)
    same_start = defaultdict(list)
    by_id = {}
    for r in cur.execute("""SELECT SessionId,SessionDate,VenueName,ActivityName,StartTime,EndTime,
                           IsBookingRequired,IsCancelled,Capacity FROM dbo.Sessions
                           WHERE SessionDate BETWEEN ? AND ? AND ActivityName LIKE '%Innerva%'""", start, end):
        if "innerva" not in norm(r[3]) or norm(r[2]) != norm(VENUE):
            continue
        d = r[1].date() if isinstance(r[1], datetime) else r[1]
        item = {"id": int(r[0]), "date": d, "start": sql_time(r[4]), "end": sql_time(r[5]),
                "name": str(r[3]), "booking": bool(r[6]), "cancel": bool(r[7]),
                "capacity": r[8]}
        sessions[(d, item["start"], item["end"])].append(item)
        same_start[(d, item["start"])].append(item)
        by_id[item["id"]] = item
    attended = {}
    session_names = defaultdict(list)
    count_by_session: Counter = Counter()
    for r in cur.execute("""SELECT a.SessionId,a.AttendanceMemberKind,a.ParticipantId,
                                  a.LiteMemberId,a.Attended,a.MemberName,a.SaheliCardNumber
                           FROM dbo.SessionAttendance a
                           INNER JOIN dbo.Sessions s ON s.SessionId=a.SessionId
                           WHERE s.SessionDate BETWEEN ? AND ? AND s.ActivityName LIKE '%Innerva%'""", start, end):
        sid = int(r[0])
        if sid not in by_id:
            continue
        kind = str(r[1] or "").upper()
        member = str(r[2]) if kind == "FULL" else str(r[3]).lower()
        attended[(sid, kind, member)] = bool(r[4])
        if len(r) > 5 and clean(r[5]):
            session_names[(sid, norm_name(r[5]))].append({
                "kind": kind, "member": member, "card": card_key(r[6]) if len(r) > 6 else ""
            })
        count_by_session[sid] += 1

    full_cards = defaultdict(list)
    full_names = defaultdict(list)
    for r in cur.execute("SELECT ParticipantID,SaheliCardNumber,FullName,DateOfBirth FROM dbo.Participants"):
        person = {"id": int(r[0]), "card": str(r[1] or ""), "name": clean(r[2]),
                  "dob": date_value(r[3])}
        full_cards[card_key(r[1])].append(person)
        if person["name"]:
            full_names[norm_name(person["name"])].append(person)
    lite_names = defaultdict(list)
    for r in cur.execute("SELECT Id,MembershipId,FirstName,LastName,DateOfBirth FROM dbo.LiteMembers"):
        person = {"id": str(r[0]).lower(), "mid": str(r[1]),
                  "name": (str(r[2]) + " " + str(r[3])).strip(), "dob": date_value(r[4])}
        lite_names[norm_name(person["name"])].append(person)
    return {"sessions": sessions, "same_start": same_start, "attended": attended,
            "session_names": session_names, "counts": count_by_session,
            "full": full_cards, "full_names": full_names, "lite": lite_names}


def new_lite_id(cur, simulate: bool, counters: list[int]) -> str:
    if counters[0] == 0:
        mids = [str(row[0] or "") for row in cur.execute(
            "SELECT MembershipId FROM dbo.LiteMembers" +
            (" WITH (UPDLOCK,HOLDLOCK)" if not simulate else ""))]
        ids = [int(m.group(1)) for value in mids if (m := re.fullmatch(r"LITE-(\d+)", value, re.I))]
        counters[0] = (max(ids) if ids else 0) + 1
    result = f"LITE-{counters[0]}"
    counters[0] += 1
    return result


def resolve_member(cur, p: Member, db, slot: Slot, report: Report, write: bool,
                   create_missing: bool, lite_counter: list[int],
                   allow_new_lite: bool = True):
    if p.card:
        cands = db["full"].get(p.card, [])
        if len(cands) > 1:
            report.add("REVIEW_DUPLICATE_CARD_IN_CRM", slot, p)
            return None
        if cands:
            member = cands[0]
            if (p.name and member["name"] and norm_name(p.name) != norm_name(member["name"])) or (
                    p.dob and member["dob"] and p.dob != member["dob"]):
                report.add("REVIEW_CARD_IDENTITY_CONFLICT", slot, p,
                           detail=f"CRM ParticipantId={member['id']}; verify name/DOB")
                return None
            return "FULL", str(member["id"]), member["card"], member["name"] or p.name
        if not p.name or not create_missing or (p.card.isdigit() and len(p.card) >= 7):
            report.add("REVIEW_MISSING_FULL_MEMBER", slot, p,
                       detail="Card missing from CRM; requires review or --create-missing-members")
            return None
        if db["full_names"].get(norm_name(p.name)):
            report.add("REVIEW_SAME_NAME_DIFFERENT_CARD", slot, p)
            return None
        new_id = f"PREVIEW-FULL-{p.card}"
        if write:
            cur.execute("""INSERT dbo.Participants
                           (SaheliCardNumber,FullName,DateOfBirth,Site,Notes,CreatedAt)
                           OUTPUT INSERTED.ParticipantID
                           VALUES(?,?,?,?,?,SYSDATETIME())""",
                        p.card[:50], p.name[:500], p.dob, VENUE,
                        "Manual Innerva Excel sync; verify profile details")
            new_id = str(int(cur.fetchone()[0]))
        person = {"id": new_id, "card": p.card, "name": p.name, "dob": p.dob}
        db["full"][p.card].append(person)
        db["full_names"][norm_name(p.name)].append(person)
        report.add("CREATE_FULL_MEMBER", slot, p, detail=f"ParticipantId={new_id}")
        return "FULL", new_id, p.card, p.name

    if not p.name:
        report.add("REVIEW_UNIDENTIFIED_BOOKING", slot, p)
        return None
    name = norm_name(p.name)
    # Excel row has no Saheli card: resolve its existing LITE identity FIRST.
    # An existing FULL member with the same name does not mean the existing LITE
    # booking can be skipped. A booking belongs to a SessionId + LiteMemberId.
    cands = db["lite"].get(name, [])
    if len(cands) > 1:
        if p.dob:
            cands = [c for c in cands if c["dob"] == p.dob]
        if len(cands) != 1:
            report.add("REVIEW_AMBIGUOUS_LITE", slot, p)
            return None
    if cands:
        existing = cands[0]
        if p.dob and existing["dob"] and existing["dob"] != p.dob:
            report.add("REVIEW_LITE_DOB_CONFLICT", slot, p)
            return None
        return "LITE", existing["id"], existing["mid"], existing["name"]
    # Confirmed Innerva rule: a missing card in Excel identifies the LITE path.
    # If no LITE name match exists, create a new LITE member, even if a FULL
    # participant happens to share the same name. Never modify a FULL profile.
    # Do not create an orphan LITE identity if the booking has no free capacity.
    if not allow_new_lite:
        report.add("REVIEW_SESSION_FULL", slot, p,
                   detail="New Lite identity not created because the session is full")
        return None
    pieces = clean(p.name).split(" ", 1)
    first, last = pieces[0], pieces[1] if len(pieces) > 1 else "Unknown"
    mid = new_lite_id(cur, not write, lite_counter)
    new_id = str(uuid.uuid4()).lower()
    if write:
        cur.execute("""INSERT dbo.LiteMembers(Id,MembershipId,FirstName,LastName,
                       DateOfBirth,HealthConditions,CreatedAtUtc,CreatedByUserId)
                       VALUES(?,?,?,?,?,?,SYSUTCDATETIME(),NULL)""",
                    new_id, mid, first[:100], last[:100], p.dob, p.health or None)
    db["lite"][name].append({"id": new_id, "mid": mid, "name": p.name, "dob": p.dob})
    report.add("CREATE_LITE_MEMBER", slot, p, detail=f"MembershipId={mid}")
    return "LITE", new_id, mid, p.name


def insert_booking(cur, sid: int, slot: Slot, p: Member, resolution, write: bool):
    if not write:
        return
    kind, member_id, display, member_name = resolution
    cur.execute("""INSERT dbo.SessionAttendance
        (SessionId,AttendanceMemberKind,ParticipantId,LiteMemberId,MemberDisplayId,
         SaheliCardNumber,MemberName,SessionName,SessionDay,SessionDate,SessionMonth,
         SessionStartTime,SessionEndTime,RiskStratification,Attended,Notes,
         SignedInductionPaper,MedicalCondition,CreatedAtUtc,UpdatedAtUtc)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,SYSUTCDATETIME(),NULL)""",
        sid, kind, int(member_id) if kind == "FULL" else None,
        member_id if kind == "LITE" else None, display[:50],
        display[:50] if kind == "FULL" else None, member_name[:200] if member_name else None,
        "Innerva", slot.day.strftime("%A"), slot.day, slot.day.strftime("%B"),
        slot.start, slot.end, p.risk[:100] if p.risk else None,
        bool(p.attended),
        (f"Manual Innerva sync; sheet={slot.sheet[:65]}; row={p.source_row}; "
         f"ExcelAttended={'Yes' if p.attended else 'No' if p.attended is False else 'Blank'}; "
         f"Issues={p.issues[:220]}")[:500],
        p.induction_signed, p.health[:1000] if p.health else None)


def sync(cur, slots: list[Slot], report: Report, args, write: bool,
         excluded_slots: Optional[set[tuple]] = None) -> None:
    check_schema(cur)
    db = load_database(cur, slots)
    lite_counter = [0]
    preview_sid = -1
    for slot in slots:
        if excluded_slots and (slot.day.isoformat(), f"{slot.start}-{slot.end}") in excluded_slots:
            report.add("SKIP_REVIEW_BLOCKED_SLOT", slot,
                       detail="Excluded from partial import because the preview contains REVIEW actions")
            continue
        # Never create/modify a session from a malformed over-capacity source block.
        if not slot.is_cancelled and len(slot.people) > 9:
            report.add("REVIEW_EXCEL_CAPACITY_BLOCK", slot)
            continue
        matches = db["sessions"].get(slot.key, [])
        nearby = db["same_start"].get((slot.day, slot.start), [])
        if len(matches) > 1:
            report.add("REVIEW_DUPLICATE_CRM_SESSIONS", slot,
                       detail="Same Innerva date/time appears multiple times in CRM: " +
                              ",".join(str(s["id"]) for s in matches))
            continue
        if not matches and nearby:
            report.add("REVIEW_CRM_END_TIME_CONFLICT", slot,
                       detail="Innerva session at same start time but different end time")
            continue
        if matches:
            target = matches[0]
            sid = target["id"]
            excel_type = norm(slot.kind)
            crm_type = norm(target["name"])
            if crm_type not in INNERVA_NAMES:
                report.add("REVIEW_UNKNOWN_INNERVA_VARIANT", slot,
                           detail=f"CRM activity={target['name']}", session_id=sid)
                continue
            if db["counts"].get(sid, 0) > 9:
                report.add("REVIEW_EXISTING_CAPACITY", slot,
                           detail=f"CRM already has {db['counts'][sid]} bookings", session_id=sid)
                continue
            if (crm_type.startswith("mens innerva") and excel_type == "female") or (
                    crm_type == "innerva mix" and excel_type in {"male", "female"}):
                report.add("REVIEW_SESSION_TYPE_CONFLICT", slot,
                           detail=f"CRM={target['name']} Excel={slot.kind}", session_id=sid)
                continue
            if target["cancel"] != slot.is_cancelled:
                # Cancelled source with existing delivered bookings must never be erased.
                if db["counts"].get(sid, 0) or target["cancel"]:
                    report.add("REVIEW_CANCELLATION_CONFLICT", slot,
                               detail=f"SessionId={sid}; CRM cancelled={target['cancel']}",
                               session_id=sid)
                    continue
                report.add("MARK_CANCELLED", slot, session_id=sid)
                if write:
                    cur.execute("UPDATE dbo.Sessions SET IsCancelled=1 WHERE SessionId=?", sid)
                target["cancel"] = True
            if slot.is_cancelled and db["counts"].get(sid, 0):
                report.add("REVIEW_CANCELLED_WITH_BOOKINGS", slot,
                           detail="Cancelled session already has CRM bookings", session_id=sid)
                continue
            if not target["booking"]:
                report.add("ENABLE_BOOKING", slot, session_id=sid)
                if write:
                    cur.execute("UPDATE dbo.Sessions SET IsBookingRequired=1 WHERE SessionId=?", sid)
                target["booking"] = True
        else:
            sid = preview_sid
            preview_sid -= 1
            if write:
                note = (f"Manual Innerva Excel sync; sheet={slot.sheet}; row={slot.first_row}; "
                        f"type={slot.kind}; lead={slot.lead}")[:500]
                cur.execute("""INSERT dbo.Sessions
                    (Frequency,Category,SubCategory,ActivityCategory,VenueName,ActivityName,Notes,
                     IsRecurringWeekly,DayOfWeek,SessionDate,ArrivalTime,StartTime,EndTime,
                     Capacity,IsBookingRequired,IsCancelled,CreatedAtUtc)
                     OUTPUT INSERTED.SessionId
                     VALUES('Historical','Innerva',?,'Innerva',?,'Innerva',?,0,NULL,?,?,?, ?,9,1,?,SYSUTCDATETIME())""",
                            slot.kind[:30] or None, VENUE, note, slot.day,
                            slot.induction_time, slot.start, slot.end, int(slot.is_cancelled))
                sid = int(cur.fetchone()[0])
            report.add("CREATE_SESSION", slot, detail=f"type={slot.kind}; lead={slot.lead}",
                       session_id=sid if sid > 0 else "NEW")
            target = {"id": sid, "date": slot.day, "start": slot.start, "end": slot.end,
                      "name": "Innerva", "booking": True, "cancel": slot.is_cancelled,
                      "capacity": 9}
            db["sessions"][slot.key].append(target)
            db["same_start"][(slot.day, slot.start)].append(target)
        if slot.is_cancelled:
            report.add("SKIP_CANCELLED_BOOKINGS", slot, session_id=sid)
            continue
        existing_count = db["counts"].get(sid, 0)
        if existing_count > 9 or (target["capacity"] is not None and
                                   existing_count > target["capacity"]):
            report.add("REVIEW_EXISTING_CAPACITY", slot,
                       detail=f"CRM already has {existing_count} bookings", session_id=sid)
            continue
        for person in slot.people:
            # A blank Excel card does NOT prove a person is missing from this
            # session. Historic CRM may already contain them as FULL even though
            # the newer Excel entry lacks their card. Review, never duplicate.
            booked_same_name = db["session_names"].get((sid, norm_name(person.name)), []) if person.name else []
            if not person.card and any(row["kind"] == "FULL" for row in booked_same_name):
                report.add("REVIEW_POSSIBLE_EXISTING_FULL_BOOKING", slot, person,
                           detail="This name already booked as FULL in the same session; avoid duplicate Lite booking",
                           session_id=sid)
                continue
            if person.card and any(row["kind"] == "FULL" and row["card"] and row["card"] != person.card
                                   for row in booked_same_name):
                report.add("REVIEW_SAME_NAME_DIFFERENT_CARD_IN_SESSION", slot, person,
                           detail="Name is already booked with another card in this session", session_id=sid)
                continue
            cap = target["capacity"]
            at_capacity = existing_count >= 9 or (cap is not None and existing_count >= cap)
            # Never create a new member when the session has no room for them.
            resolution = resolve_member(cur, person, db, slot, report, write,
                                        args.create_missing_members and not at_capacity,
                                        lite_counter, allow_new_lite=not at_capacity)
            if resolution is None:
                continue
            kind, member_id, _, _ = resolution
            key = (sid, kind, member_id.lower() if kind == "LITE" else member_id)
            if key in db["attended"]:
                crm_att = db["attended"][key]
                if person.attended is not None and crm_att != person.attended:
                    if person.attended and args.promote_attendance:
                        report.add("PROMOTE_ATTENDANCE", slot, person, session_id=sid)
                        if write:
                            cur.execute("""UPDATE dbo.SessionAttendance SET Attended=1,
                                UpdatedAtUtc=SYSUTCDATETIME()
                                WHERE SessionId=? AND AttendanceMemberKind=? AND """ +
                                ("ParticipantId=?" if kind == "FULL" else "LiteMemberId=?"),
                                sid, kind, int(member_id) if kind == "FULL" else member_id)
                        db["attended"][key] = True
                    else:
                        report.add("REVIEW_ATTENDANCE_CONFLICT", slot, person,
                                   detail=f"CRM={int(crm_att)} Excel={int(person.attended)}; no change",
                                   session_id=sid)
                else:
                    report.add("SKIP_EXISTING_BOOKING", slot, person, session_id=sid)
                continue
            if at_capacity:
                report.add("REVIEW_SESSION_FULL", slot, person, session_id=sid)
                continue
            report.add("INSERT_BOOKING", slot, person,
                       detail=f"Attended={'Yes' if person.attended else 'No' if person.attended is False else 'Blank'}",
                       session_id=sid if sid > 0 else "NEW")
            insert_booking(cur, sid, slot, person, resolution, write)
            db["attended"][key] = bool(person.attended)
            if person.name:
                db["session_names"][(sid, norm_name(person.name))].append({
                    "kind": kind, "member": member_id,
                    "card": person.card if kind == "FULL" else ""})
            existing_count += 1
            db["counts"][sid] = existing_count


SAFE_ACTIONS = {"CREATE_SESSION", "ENABLE_BOOKING", "INSERT_BOOKING",
                "CREATE_LITE_MEMBER", "CREATE_FULL_MEMBER", "MARK_CANCELLED", "PROMOTE_ATTENDANCE"}


def report_slot_key(row: dict) -> Optional[tuple[str, str]]:
    if row.get("Date") and row.get("Time"):
        return row["Date"], row["Time"]
    return None


def blocked_slot_keys(report: Report) -> set[tuple[str, str]]:
    return {k for row in report.rows if row.get("Action", "").startswith("REVIEW_")
            if (k := report_slot_key(row)) is not None}


def export_triage(report: Report, timestamp: str) -> tuple[int, int]:
    blocked = blocked_slot_keys(report)
    ready, review = Report(), Report()
    for row in report.rows:
        if row["Action"].startswith("REVIEW_"):
            review.rows.append(row)
        elif row["Action"] in SAFE_ACTIONS:
            if report_slot_key(row) not in blocked:
                ready.rows.append(row)
    ready.save(REPORT_DIR / f"innerva_ready_to_import_{timestamp}.csv")
    review.save(REPORT_DIR / f"innerva_needs_review_{timestamp}.csv")
    return len(ready.rows), len(blocked)


def print_summary(report: Report, label: str) -> None:
    print(f"\n===== {label} =====")
    for k, v in sorted(report.count.items()):
        print(f"  {k:38} {v:,}")
    print(f"  {'REVIEW records':38} {sum(v for k, v in report.count.items() if k.startswith('REVIEW_')):,}")


def parse_args():
    ap = argparse.ArgumentParser(description="Manual Innerva-only booking synchronisation")
    ap.add_argument("--excel", help="Path to Innerva Booking Sheet (.xlsx); required if multiple matching files")
    ap.add_argument("--test-connection", action="store_true", help="Check Azure SQL login only; do not read Excel or write data")
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--preview", action="store_true", help="Read-only preview (default)")
    mode.add_argument("--commit", action="store_true", help="Confirm, then insert/upsert Innerva only")
    ap.add_argument("--allow-safe-partial", action="store_true",
                    help="Explicit opt-in: import only review-free slots, leaving ALL flagged slots unchanged")
    ap.add_argument("--start", type=date.fromisoformat, help="Inclusive start date YYYY-MM-DD")
    ap.add_argument("--end", type=date.fromisoformat, help="Inclusive end date YYYY-MM-DD; default today")
    ap.add_argument("--all-dates", action="store_true", help="Include future booking slots")
    ap.add_argument("--current-year", type=int, help="Year for ambiguous 'Current' sheet, e.g. 2026")
    ap.add_argument("--create-missing-members", action="store_true",
                    help="Opt-in: create FULL participants for previously unknown Saheli cards; new cardless LITE members are created automatically")
    ap.add_argument("--promote-attendance", action="store_true",
                    help="Opt-in: change existing Attended=0 to 1 for explicit Excel Yes")
    a = ap.parse_args()
    if not a.all_dates and not a.end:
        a.end = date.today()
    if a.start and a.end and a.start > a.end:
        ap.error("--start must be <= --end")
    if a.test_connection:
        return a
    if a.excel:
        a.excel = Path(a.excel).expanduser().resolve()
    else:
        files = list(Path(__file__).resolve().parent.glob("Innerva Booking Sheet*.xlsx"))
        if len(files) != 1:
            ap.error("Put ONE 'Innerva Booking Sheet*.xlsx' next to script, or use --excel PATH")
        a.excel = files[0]
    if not a.excel.is_file():
        ap.error(f"Workbook not found: {a.excel}")
    return a


def main() -> int:
    args = parse_args()
    conn_string = os.environ.get("SAHELI_SQL_CONNECTION_STRING", "").strip()
    if not conn_string:
        print("ERROR: SAHELI_SQL_CONNECTION_STRING environment variable is not set.", file=sys.stderr)
        return 2
    try:
        import pyodbc
    except ImportError:
        print("Run: py -m pip install openpyxl pyodbc", file=sys.stderr)
        return 2
    if args.test_connection:
        print("Checking Azure SQL login only. No Excel scanning or CRM changes.")
        try:
            with pyodbc.connect(conn_string, autocommit=False, timeout=15) as cn:
                row = cn.cursor().execute("SELECT DB_NAME(), SUSER_SNAME()").fetchone()
                db_name = str(row[0]) if row and row[0] is not None else "(unknown)"
                sql_login = str(row[1]) if row and row[1] is not None else "(unknown)"
                print(f"CONNECTED: database={db_name}, SQL login={sql_login}")
                cn.rollback()
                if db_name.lower() != "sahelihubcrm":
                    print("ERROR: Connected to an unexpected database. No migration allowed.", file=sys.stderr)
                    return 2
                print("SQL LOGIN TEST PASSED. You can now run Preview.")
                return 0
        except pyodbc.Error as exc:
            msg = str(exc)
            if "18456" in msg:
                print("LOGIN FAILED (SQL Server 18456): server responded, but authentication was rejected.", file=sys.stderr)
                print("Check the SQL-auth username and password against the credentials that work in SSMS/Azure SQL.", file=sys.stderr)
                print("Do not reset a shared production SQL password before checking CRM App Service dependencies.", file=sys.stderr)
            else:
                print("SQL CONNECTION FAILED. Check ODBC driver, server, database, network/firewall and credentials.", file=sys.stderr)
            print(f"ODBC diagnostic: {msg}", file=sys.stderr)
            return 1
    digest = hashlib.sha256(args.excel.read_bytes()).hexdigest()[:16]
    print(f"Excel file: {args.excel}\nSHA256 prefix: {digest}")
    print(f"Date range: {args.start or 'earliest'} to {args.end or 'latest'}")
    parse_report = Report()
    slots = parse_workbook(args.excel, args, parse_report)
    if not slots:
        print("No supported Innerva slots found. No changes made.")
        print_summary(parse_report, "SOURCE REVIEW")
        return 2
    print(f"Parsed {len(slots)} unique slots, {sum(len(s.people) for s in slots)} booking rows")
    if any(k in parse_report.count for k in ("REVIEW_CONFLICTING_EXCEL_SLOTS", "REVIEW_BAD_HEADER")):
        print("Warning: some source slots need manual review; they will be skipped.")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    # Phase 1 is genuinely READ ONLY: no INSERT, UPDATE or DELETE, even inside a transaction.
    with pyodbc.connect(conn_string, autocommit=False, timeout=20) as cn:
        cn.timeout = 30  # pyodbc Connection query timeout; Cursor has no timeout attribute
        cur = cn.cursor()
        preview = Report()
        preview.rows.extend(parse_report.rows)
        preview.count.update(parse_report.count)
        sync(cur, slots, preview, args, write=False)
        cn.rollback()
    preview_path = REPORT_DIR / f"innerva_preview_{timestamp}.csv"
    preview.save(preview_path)
    print_summary(preview, "READ-ONLY PREVIEW")
    ready_rows, blocked_count = export_triage(preview, timestamp)
    review_count = sum(v for k, v in preview.count.items() if k.startswith("REVIEW_"))
    print(f"Preview CSV: {preview_path}")
    print(f"READY action rows from review-free slots: {ready_rows:,}; blocked slots: {blocked_count:,}")
    print(f"Ready CSV: {REPORT_DIR / f'innerva_ready_to_import_{timestamp}.csv'}")
    print(f"Review CSV: {REPORT_DIR / f'innerva_needs_review_{timestamp}.csv'}")
    if not args.commit:
        print("Nothing written to CRM. To apply, run with --commit.")
        return 0
    blocked = blocked_slot_keys(preview)
    if review_count and not args.allow_safe_partial:
        print(f"COMMIT BLOCKED: {review_count:,} review items found.")
        print("Review the CSVs. To import only review-free slots, use --commit --allow-safe-partial.")
        return 3
    if args.allow_safe_partial and not ready_rows:
        print("COMMIT BLOCKED: No verified ready actions.")
        return 3
    print("\nThe next step will write ONLY Innerva sessions and their booking records.")
    if args.allow_safe_partial:
        print(f"PARTIAL MODE: all {blocked_count:,} flagged slots will be skipped entirely.")
        confirm = "IMPORT SAFE"
    else:
        confirm = "IMPORT"
    print("Review the ready and review CSV files before proceeding.")
    if input(f"Type {confirm} to commit (anything else cancels): ").strip() != confirm:
        print("Cancelled; database unchanged.")
        return 0

    with pyodbc.connect(conn_string, autocommit=False, timeout=20) as cn:
        cn.timeout = 30  # pyodbc Connection query timeout; Cursor has no timeout attribute
        cur = cn.cursor()
        cur.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        cur.execute("SET LOCK_TIMEOUT 12000")
        actual = Report()
        actual.rows.extend(parse_report.rows)
        actual.count.update(parse_report.count)
        try:
            sync(cur, slots, actual, args, write=True,
                 excluded_slots=blocked if args.allow_safe_partial else None)
            unexpected = [row for row in actual.rows if row["Action"].startswith("REVIEW_")]
            if args.allow_safe_partial:
                # Source parsing reviews may describe rows that were excluded before
                # database matching. All database-side review actions must be zero.
                unexpected = [row for row in actual.rows[len(parse_report.rows):]
                              if row["Action"].startswith("REVIEW_")]
            if unexpected:
                raise RuntimeError(f"COMMIT ABORTED: {len(unexpected)} new review items; transaction rolled back")
            cn.commit()
        except Exception:
            cn.rollback()
            raise
    result_path = REPORT_DIR / f"innerva_commit_{timestamp}.csv"
    actual.save(result_path)
    print_summary(actual, "COMMITTED")
    print(f"Result CSV: {result_path}")
    print("Re-run --preview to verify new bookings are reported as existing.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nCancelled. Any uncommitted transaction should be rolled back.", file=sys.stderr)
        raise SystemExit(130)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
