#!/usr/bin/env python3
"""
Saheli CRM - Tennis Full Historical Migration V1

Sources (same folder as this script):
  - Tennis Register 2024.xlsx
  - Tennis Register 2025 (2).xlsx

Safety model
------------
* --audit-only parses Excel only and never connects to SQL.
* Default mode runs all proposed DB work inside one transaction and ROLLS BACK.
* --commit is refused while any REVIEW_* blockers remain.
* Existing CRM participants, Lite members, Tennis sessions and attendance are reused.
* Existing CRM profile data is never overwritten.
* 2024 has no source session times: V1 only reuses an unambiguous existing CRM
  Tennis session on the same venue/date. It NEVER invents a 2024 time.
* 2025/26 source times are preserved/reconstructed; different time slots remain
  different sessions.
"""
from __future__ import annotations

import argparse
import csv
import os
import re
import sys
import uuid
import warnings
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Optional

try:
    from openpyxl import load_workbook
    from openpyxl.utils.datetime import from_excel
except ImportError:
    print("ERROR: openpyxl is required. Run: py -m pip install openpyxl pyodbc", file=sys.stderr)
    raise

warnings.filterwarnings("ignore", message=r"Data Validation extension is not supported.*")
warnings.filterwarnings("ignore", message=r"Cell .* is marked as a date but the serial value.*")

BASE_DIR = Path(__file__).resolve().parent
SOURCE_2024 = "Tennis Register 2024.xlsx"
SOURCE_2025 = "Tennis Register 2025 (2).xlsx"
CONNECTION_STRING = os.getenv("SAHELI_SQL_CONNECTION_STRING", "").strip()

ACTIVITY = "Tennis"
DEFAULT_FREQUENCY = "Historical"
MIGRATION_NOTE_PREFIX = "Historical Tennis register import"
NO_SURNAME_LABEL = "Unknown"
CREATE_MISSING_FULL_PARTICIPANTS = True
CREATE_MISSING_LITE_MEMBERS = True

VENUE_MAP = {
    "calthorpe park": "Calthorpe Wellbeing Hub",
    "calthorpe": "Calthorpe Wellbeing Hub",
    "calthorpe wellbeing hub": "Calthorpe Wellbeing Hub",
    "handsworth park": "Handsworth",
    "handsworth": "Handsworth",
    "parkfield": "Parkfield Community School",
    "parkfield community": "Parkfield Community School",
    "parkfield community school": "Parkfield Community School",
    "wardend": "Ward End Park",
    "ward end": "Ward End Park",
    "ward end park": "Ward End Park",
    "heathmount": "Heathmount",
    "st albans": "St Albans",
    "st. albans": "St Albans",
    "cannon hill": "Cannon Hill Park",
    "cannon hill park": "Cannon Hill Park",
}
SHEET_VENUES = {
    "Calthorpe": "Calthorpe Wellbeing Hub",
    "Handsworth": "Handsworth",
    "Parkfield": "Parkfield Community School",
    "Wardend": "Ward End Park",
    "Heathmount": "Heathmount",
    "St Albans": "St Albans",
    "Cannon Hill": "Cannon Hill Park",
}
VALID_VENUES = set(SHEET_VENUES.values())

INVALID_TEXT = {"", "0", "#n/a", "#ref!", "#value!", "#name?", "none", "null", "nan"}
ZERO_ATTENDANCE_MARKERS = {
    "no one", "no one attended", "no attended", "no attendance", "none attended",
}
CANCEL_MARKERS = {
    "cancelled", "canceled", "bad weather", "eid", "easter", "summer break",
    "heatwave", "heatwve", "severe heat", "holiday", "closed", "closure",
}

# -----------------------------------------------------------------------------
# Models
# -----------------------------------------------------------------------------

@dataclass
class PersonSource:
    source_file: str
    source_sheet: str
    source_row: int
    raw_card: Any = None
    card: Optional[str] = None
    full_name: Optional[str] = None
    dob: Optional[date] = None
    postcode: Optional[str] = None
    phone: Optional[str] = None
    emergency_name: Optional[str] = None
    emergency_phone: Optional[str] = None
    risk: Optional[str] = None
    gender: Optional[str] = None
    ethnicity: Optional[str] = None
    health_conditions: Optional[str] = None

    @property
    def name_key(self) -> str:
        return normalize_name(self.full_name)


@dataclass
class SourceSession:
    source_file: str
    source_sheet: str
    source_ref: str
    session_date: date
    venue_name: str
    start_time: Optional[time]
    end_time: Optional[time]
    time_quality: str
    people: list[PersonSource] = field(default_factory=list)
    is_cancelled: bool = False
    cancel_reason: Optional[str] = None
    zero_attendance_marker: bool = False
    source_notes: list[str] = field(default_factory=list)

    @property
    def key(self):
        return (self.session_date, norm_venue(self.venue_name), self.start_time, self.end_time)


@dataclass
class DbParticipant:
    participant_id: int
    card: str
    full_name: str
    dob: Optional[date]
    postcode: Optional[str]
    mobile: Optional[str]


@dataclass
class DbLite:
    lite_id: str
    membership_id: str
    first_name: str
    last_name: str
    dob: Optional[date]
    postcode: Optional[str]
    phone: Optional[str]

    @property
    def full_name(self):
        return f"{self.first_name} {self.last_name}".strip()


@dataclass
class DbSession:
    session_id: int
    session_date: date
    venue_name: str
    start_time: Optional[time]
    end_time: Optional[time]
    is_cancelled: bool
    attendance_count: int
    created_in_run: bool = False


# -----------------------------------------------------------------------------
# General cleaning
# -----------------------------------------------------------------------------

def clean_text(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, float) and v != v:
        return ""
    s = str(v).replace("\xa0", " ").strip()
    if s.lower() in INVALID_TEXT:
        return ""
    return re.sub(r"\s+", " ", s).strip()


def normalize_name(v: Any) -> str:
    s = clean_text(v).lower()
    s = s.replace("’", "'")
    s = re.sub(r"\b(new to tennis|new|visiting|visting)\b", " ", s)
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def normalize_simple(v: Any) -> str:
    s = clean_text(v).lower()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def normalize_postcode(v: Any) -> Optional[str]:
    s = clean_text(v).upper().replace(" ", "")
    if not s:
        return None
    # Keep short 2024 outward-code values (e.g. B12) but reject obvious non-postcodes.
    if re.match(r"^[A-Z]{1,2}\d[A-Z0-9]?(?:\d[A-Z]{2})?$", s):
        return s
    return None


def normalize_phone(v: Any) -> Optional[str]:
    s = re.sub(r"\D", "", clean_text(v))
    return s or None


def norm_venue(v: Any) -> str:
    n = normalize_simple(v)
    return normalize_simple(VENUE_MAP.get(n, clean_text(v)))


def canonical_venue(v: Any) -> Optional[str]:
    n = normalize_simple(v)
    return VENUE_MAP.get(n)


def split_name(full_name: Any) -> tuple[Optional[str], Optional[str]]:
    s = clean_text(full_name)
    if not s:
        return None, None
    parts = s.split()
    if len(parts) == 1:
        return parts[0], None
    return parts[0], " ".join(parts[1:])


def display_lite_key(full_name: str) -> str:
    first, last = split_name(full_name)
    if not first:
        return ""
    return normalize_name(f"{first} {last or NO_SURNAME_LABEL}")


def parse_dob(v: Any) -> Optional[date]:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    if isinstance(v, (int, float)):
        try:
            d = from_excel(v)
            return d.date() if isinstance(d, datetime) else d
        except Exception:
            return None
    s = clean_text(v)
    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%d/%m/%y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    return None


def parse_excel_date(v: Any) -> Optional[date]:
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    if isinstance(v, (int, float)):
        # Ordinary Excel serials only. Large corrupt values are repaired from context.
        if 30000 <= float(v) <= 70000:
            try:
                d = from_excel(v)
                return d.date() if isinstance(d, datetime) else d
            except Exception:
                return None
        return None
    s = clean_text(v)
    if not s:
        return None
    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%d/%m/%y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    return None


def parse_card_and_name(raw_identity: Any, raw_name: Any = None) -> tuple[Optional[str], Optional[str], str]:
    ident = clean_text(raw_identity)
    name_col = clean_text(raw_name)
    if normalize_simple(name_col) in ZERO_ATTENDANCE_MARKERS or is_cancel_marker(name_col):
        name_col = ""

    # Numeric cells are Saheli cards unless implausibly long.
    if isinstance(raw_identity, (int, float)) and not isinstance(raw_identity, bool):
        if float(raw_identity).is_integer():
            card = str(int(raw_identity))
            return card, (name_col or None), "NUMERIC_CARD"

    # Text such as 'Tayyaba Nazir (526)' or 'Aasia (842)'.
    m = re.match(r"^\s*(.*?)\s*\((\d{1,6})\)\s*$", ident)
    if m:
        nm = clean_text(m.group(1)) or name_col
        return m.group(2), (nm or None), "EMBEDDED_CARD"

    if re.fullmatch(r"\d{1,6}", ident):
        return str(int(ident)), (name_col or None), "TEXT_CARD"

    if re.fullmatch(r"\d{7,}", ident):
        # Preserve it as a source card candidate; DB resolution will block/reclassify safely.
        return ident, (name_col or None), "SUSPICIOUS_LONG_NUMBER"

    # Card column is frequently used as a participant-name column in the Tennis workbooks.
    nm = name_col or ident
    if not nm or normalize_simple(nm) in ZERO_ATTENDANCE_MARKERS or is_cancel_marker(nm):
        nm = None
    return None, nm, "NAME_IN_CARD_COLUMN" if ident else "NAME_COLUMN"


def is_cancel_marker(v: Any) -> bool:
    n = normalize_simple(v)
    return bool(n) and any(marker in n for marker in CANCEL_MARKERS)


def is_zero_marker(v: Any) -> bool:
    n = normalize_simple(v)
    return n in ZERO_ATTENDANCE_MARKERS


def name_similarity(a: Any, b: Any) -> float:
    aa, bb = normalize_name(a), normalize_name(b)
    if not aa or not bb:
        return 0.0
    if aa == bb:
        return 1.0
    ta, tb = set(aa.split()), set(bb.split())
    if ta and tb and (ta <= tb or tb <= ta):
        return 0.92
    return SequenceMatcher(None, aa, bb).ratio()


# -----------------------------------------------------------------------------
# Time parsing
# -----------------------------------------------------------------------------

def _clock(h: int, m: int, mer: Optional[str]) -> time:
    if mer == "pm" and h < 12:
        h += 12
    elif mer == "am" and h == 12:
        h = 0
    if not (0 <= h <= 23 and 0 <= m <= 59):
        raise ValueError
    return time(h, m)


def _infer_pair(h1: int, m1: int, mer1: Optional[str], h2: int, m2: int, mer2: Optional[str]) -> tuple[time, time]:
    # Propagate explicit meridiem sensibly.
    if mer1 is None and mer2 == "am":
        mer1 = "am"
    if mer1 is None and mer2 == "pm":
        # 10-12pm means 10am-12pm; 4-5pm means 4pm-5pm.
        if h2 == 12 and 7 <= h1 <= 11:
            mer1 = "am"
        elif 1 <= h1 <= 6:
            mer1 = "pm"
        elif h1 == 12:
            mer1 = "pm"
        else:
            mer1 = "am"
    if mer2 is None and mer1 is not None:
        mer2 = mer1

    st = _clock(h1, m1, mer1)
    et = _clock(h2, m2, mer2)

    # No meridiem: historic classes are daytime. Handle noon crossover.
    if mer1 is None and mer2 is None:
        if h1 == 12 and 1 <= h2 <= 6:
            st = time(12, m1); et = time(h2 + 12, m2)
        elif 7 <= h1 <= 11 and 1 <= h2 <= 6:
            st = time(h1, m1); et = time(h2 + 12, m2)
        elif 1 <= h1 <= 6 and 1 <= h2 <= 6:
            st = time(h1 + 12, m1); et = time(h2 + 12, m2)

    if et <= st:
        if et.hour + 12 <= 23:
            et = time(et.hour + 12, et.minute)
    if et <= st:
        raise ValueError
    return st, et


def parse_time_range(v: Any) -> Optional[tuple[time, time]]:
    s = clean_text(v).lower()
    if not s or is_zero_marker(s) or is_cancel_marker(s):
        return None
    s = s.replace("–", "-").replace("—", "-").replace("`", "").replace("#", "-")
    s = s.replace("to", "-")
    s = re.sub(r"\s+", "", s)

    # 24-hour / dotted normal ranges: 16:30-17:30, 11.30-12.30
    m = re.fullmatch(r"(\d{1,2})[:.](\d{1,2})(am|pm)?-(\d{1,2})[:.](\d{1,2})(am|pm)?", s)
    if m:
        return _infer_pair(int(m[1]), int(m[2]), m[3], int(m[4]), int(m[5]), m[6])

    # Four-number forms: 10-00-11-30, 12-15-1-15, 9-15-10-15.
    m = re.fullmatch(r"(\d{1,2})-(\d{2})-(\d{1,2})-(\d{2})(am|pm)?", s)
    if m:
        return _infer_pair(int(m[1]), int(m[2]), None, int(m[3]), int(m[4]), m[5])

    # Mixed punctuation forms: 9.15-10-15 / 12-15-1.15.
    m = re.fullmatch(r"(\d{1,2})[.:](\d{2})-(\d{1,2})-(\d{2})(am|pm)?", s)
    if m:
        return _infer_pair(int(m[1]), int(m[2]), None, int(m[3]), int(m[4]), m[5])
    m = re.fullmatch(r"(\d{1,2})-(\d{2})-(\d{1,2})[.:](\d{2})(am|pm)?", s)
    if m:
        return _infer_pair(int(m[1]), int(m[2]), None, int(m[3]), int(m[4]), m[5])

    # Missing separator inside the range: 10-0011-30.
    m = re.fullmatch(r"(\d{1,2})-(\d{2})(\d{1,2})-(\d{2})(am|pm)?", s)
    if m:
        return _infer_pair(int(m[1]), int(m[2]), None, int(m[3]), int(m[4]), m[5])

    # End clock collapsed to HHMM: 11.30-1230 or 10-00-1130.
    m = re.fullmatch(r"(\d{1,2})[.:](\d{2})-(\d{1,2})(\d{2})(am|pm)?", s)
    if m:
        return _infer_pair(int(m[1]), int(m[2]), None, int(m[3]), int(m[4]), m[5])
    m = re.fullmatch(r"(\d{1,2})-(\d{2})-(\d{1,2})(\d{2})(am|pm)?", s)
    if m:
        return _infer_pair(int(m[1]), int(m[2]), None, int(m[3]), int(m[4]), m[5])

    # Corrupt compact endings: 12-15-115 / 12-15-116 etc.
    m = re.fullmatch(r"(\d{1,2})-(\d{2})-(\d)(\d{2})(am|pm)?", s)
    if m:
        return _infer_pair(int(m[1]), int(m[2]), None, int(m[3]), int(m[4]), m[5])

    # 11-12.30, 10am-12pm, 4pm-5pm, 11-11:45.
    m = re.fullmatch(r"(\d{1,2})(?::(\d{1,2}))?(am|pm)?-(\d{1,2})(?:[:.](\d{1,2}))?(am|pm)?", s)
    if m:
        return _infer_pair(int(m[1]), int(m[2] or 0), m[3], int(m[4]), int(m[5] or 0), m[6])

    return None


# -----------------------------------------------------------------------------
# Source parsing
# -----------------------------------------------------------------------------

def person_from_2024(path: Path, row_num: int, row: tuple[Any, ...]) -> Optional[PersonSource]:
    name = clean_text(row[3] if len(row) > 3 else None)
    if not name:
        return None
    raw_card = row[4] if len(row) > 4 else None
    card, parsed_name, _ = parse_card_and_name(raw_card, name)
    return PersonSource(
        path.name, "Tennis", row_num,
        raw_card=raw_card,
        card=card,
        full_name=parsed_name or name,
        postcode=normalize_postcode(row[5] if len(row) > 5 else None),
        gender=clean_text(row[10] if len(row) > 10 else None) or None,
        emergency_phone=normalize_phone(row[11] if len(row) > 11 else None),
        ethnicity=clean_text(row[8] if len(row) > 8 else None) or None,
        health_conditions=clean_text(row[9] if len(row) > 9 else None) or None,
    )


def parse_2024(path: Path) -> tuple[list[SourceSession], list[dict[str, Any]]]:
    wb = load_workbook(path, data_only=True, read_only=True)
    if "Tennis" not in wb.sheetnames:
        raise RuntimeError(f"{path.name}: sheet 'Tennis' not found")
    ws = wb["Tennis"]
    groups: dict[tuple[date, str], SourceSession] = {}
    audits: list[dict[str, Any]] = []
    undated = 0
    for row_num, row in enumerate(ws.iter_rows(min_row=2, values_only=True), start=2):
        loc = canonical_venue(row[2] if len(row) > 2 else None)
        p = person_from_2024(path, row_num, row)
        if not loc or not p:
            continue
        d = parse_excel_date(row[1] if len(row) > 1 else None)
        if not d:
            undated += 1
            audits.append({"Action":"SOURCE_UNDATED_ATTENDANCE_SKIPPED", "File":path.name, "Sheet":"Tennis", "Ref":f"R{row_num}", "Date":"", "Venue":loc, "Detail":f"name={p.full_name}; source has no date; not attached to an invented session"})
            continue
        key = (d, loc)
        if key not in groups:
            groups[key] = SourceSession(path.name, "Tennis", f"{d.isoformat()}|{loc}", d, loc, None, None, "SOURCE_NO_TIME")
        groups[key].people.append(p)
    wb.close()
    audits.append({"Action":"SOURCE_2024_UNDATED_COUNT", "File":path.name, "Sheet":"Tennis", "Ref":"", "Date":"", "Venue":"", "Detail":f"{undated} undated participant rows skipped"})
    return sorted(groups.values(), key=lambda s:(s.session_date,s.venue_name)), audits


def _standard_person_fields(sheet: str, row: tuple[Any, ...]) -> tuple[Any, Any, Any, Any, Any, Any, Any]:
    # identity, name, dob, postcode, emergency_name, emergency_phone, risk
    identity = row[5] if len(row) > 5 else None
    if sheet == "Wardend":
        name = None
        dob = row[7] if len(row) > 7 else None
        postcode = row[6] if len(row) > 6 else None
        emergency_name = row[8] if len(row) > 8 else None
        emergency_phone = row[9] if len(row) > 9 else None
        risk = row[10] if len(row) > 10 else None
    elif sheet == "St Albans":
        # St Albans uses column F for pupil initials/names and column G for postcode.
        # Unlike the other sheets, column G is NOT a participant-name column.
        name = None
        dob = row[11] if len(row) > 11 else None
        postcode = row[6] if len(row) > 6 else None
        emergency_name = row[7] if len(row) > 7 else None
        emergency_phone = row[8] if len(row) > 8 else None
        risk = row[9] if len(row) > 9 else None
    else:
        name = row[6] if len(row) > 6 else None
        dob = row[11] if len(row) > 11 else None
        postcode = row[12] if len(row) > 12 else None
        emergency_name = row[7] if len(row) > 7 else None
        emergency_phone = row[8] if len(row) > 8 else None
        risk = row[9] if len(row) > 9 else None
    return identity, name, dob, postcode, emergency_name, emergency_phone, risk


def _repair_date(sheet: str, row_num: int, raw_date: Any, raw_month: Any, raw_day: Any,
                 raw_time: Any, previous: Optional[tuple[date,str,str,int]], audits: list[dict[str,Any]], path: Path) -> Optional[date]:
    # Explicit Parkfield spreadsheet corruption: rows contain 18/06/20236 ... 18/06/20240.
    s = clean_text(raw_date)
    if sheet == "Parkfield" and re.fullmatch(r"18/06/202(?:3[6-9]|40)", s):
        d = date(2026,6,18)
        audits.append({"Action":"SOURCE_DATE_CORRECTED", "File":path.name, "Sheet":sheet, "Ref":f"R{row_num}", "Date":d.isoformat(), "Venue":SHEET_VENUES[sheet], "Detail":f"{s} -> 2026-06-18 from weekly sequence"})
        return d

    d = parse_excel_date(raw_date)
    month_n = normalize_simple(raw_month)
    day_n = normalize_simple(raw_day)
    time_n = normalize_simple(raw_time)

    if d and not (date(2025, 1, 1) <= d <= date(2026, 12, 31)):
        # The 2025/26 workbook contains a few corrupt Excel dates rendered as year 2926.
        # Treat them as invalid here so the contiguous-row repair below can recover them.
        d = None

    if d:
        # Catch isolated serial typo where source Month/Day and adjacent rows prove the real session date.
        if previous and month_n:
            prev_d, prev_day, prev_time, prev_row = previous
            expected_month = normalize_simple(d.strftime("%B"))
            if expected_month != month_n and day_n == prev_day and time_n == prev_time and row_num == prev_row + 1:
                audits.append({"Action":"SOURCE_DATE_CORRECTED", "File":path.name, "Sheet":sheet, "Ref":f"R{row_num}", "Date":prev_d.isoformat(), "Venue":SHEET_VENUES[sheet], "Detail":f"serial {raw_date} conflicts with Month={raw_month}; carried same contiguous session date {prev_d}"})
                return prev_d
        return d

    # Large corrupt serials / blank date on continuation rows: only carry contiguous same day/time session.
    if previous:
        prev_d, prev_day, prev_time, prev_row = previous
        if row_num == prev_row + 1 and day_n == prev_day and time_n == prev_time:
            audits.append({"Action":"SOURCE_DATE_CORRECTED_FROM_CONTEXT", "File":path.name, "Sheet":sheet, "Ref":f"R{row_num}", "Date":prev_d.isoformat(), "Venue":SHEET_VENUES[sheet], "Detail":f"raw_date={clean_text(raw_date) or 'blank'}; contiguous same day/time as prior row"})
            return prev_d
    return None


def parse_vertical_2025(path: Path) -> tuple[list[SourceSession], list[dict[str, Any]]]:
    wb = load_workbook(path, data_only=True, read_only=True)
    audits: list[dict[str,Any]] = []
    observations: list[tuple[str,int,date,time,time,str,Optional[PersonSource],bool,bool,str]] = []
    # sheet,row,date,start,end,time_quality,person,cancel,zero,reason

    actual_sheet_by_norm = {normalize_simple(name): name for name in wb.sheetnames}
    for sheet, venue in SHEET_VENUES.items():
        actual_sheet = actual_sheet_by_norm.get(normalize_simple(sheet))
        if not actual_sheet:
            audits.append({"Action":"REVIEW_SOURCE_SHEET_MISSING", "File":path.name, "Sheet":sheet, "Ref":"", "Date":"", "Venue":venue, "Detail":"expected Tennis sheet missing"})
            continue
        ws = wb[actual_sheet]
        if actual_sheet != sheet:
            audits.append({"Action":"SOURCE_SHEET_NAME_NORMALISED", "File":path.name, "Sheet":sheet, "Ref":"", "Date":"", "Venue":venue, "Detail":f"workbook sheet {actual_sheet!r} treated as {sheet!r}"})
        previous: Optional[tuple[date,str,str,int]] = None
        for row_num, row in enumerate(ws.iter_rows(min_row=2, values_only=True), start=2):
            raw_activity = clean_text(row[0] if len(row)>0 else None)
            raw_day = row[1] if len(row)>1 else None
            raw_date = row[2] if len(row)>2 else None
            raw_month = row[3] if len(row)>3 else None
            raw_time = row[4] if len(row)>4 else None
            identity, name_col, dob_v, postcode_v, em_name, em_phone, risk_v = _standard_person_fields(sheet,row)
            id_text = clean_text(identity)
            name_text = clean_text(name_col)

            # Ignore decorative/unused rows unless there is a date or actual identity marker.
            has_identity = bool(id_text or name_text)
            has_session_signal = bool(clean_text(raw_date) or clean_text(raw_time) or has_identity)
            if not has_session_signal:
                continue

            cancel = is_cancel_marker(identity) or is_cancel_marker(name_col) or is_cancel_marker(raw_activity)
            zero = is_zero_marker(identity) or is_zero_marker(name_col) or is_zero_marker(raw_time)
            parsed_time = parse_time_range(raw_time)
            raw_time_norm = normalize_simple(raw_time)
            raw_day_norm = normalize_simple(raw_day)

            d = _repair_date(sheet,row_num,raw_date,raw_month,raw_day,raw_time,previous,audits,path)

            # Calthorpe 19-Jun-2026 is a continuation of the established Friday 10:00-11:30
            # Tennis slot between 12-Jun and 03-Jul. The source omitted the time on row 271
            # and omitted date/time on the immediately following participant row 272.
            if sheet == "Calthorpe" and d == date(2026, 6, 19) and parsed_time is None and row_num == 271:
                parsed_time = (time(10, 0), time(11, 30))
                audits.append({"Action":"SOURCE_TIME_INFERRED_FROM_WEEKLY_SEQUENCE", "File":path.name, "Sheet":sheet, "Ref":f"R{row_num}", "Date":d.isoformat(), "Venue":venue, "Detail":"missing source time -> 10:00-11:30 from adjacent Friday Tennis sessions"})
            if sheet == "Calthorpe" and row_num == 272 and id_text and not clean_text(raw_date) and not clean_text(raw_time):
                d = date(2026, 6, 19)
                parsed_time = (time(10, 0), time(11, 30))
                audits.append({"Action":"SOURCE_DATE_TIME_CARRIED_FROM_PRIOR_ROW", "File":path.name, "Sheet":sheet, "Ref":f"R{row_num}", "Date":d.isoformat(), "Venue":venue, "Detail":"participant continuation row -> 2026-06-19 10:00-11:30"})
            if d:
                previous = (d, raw_day_norm, raw_time_norm, row_num)
            elif has_identity:
                # Orphan identities such as Cannon Hill card 444 are retained in audit but not assigned a made-up date.
                if not cancel and not zero:
                    audits.append({"Action":"SOURCE_UNDATED_ATTENDANCE_SKIPPED", "File":path.name, "Sheet":sheet, "Ref":f"R{row_num}", "Date":"", "Venue":venue, "Detail":f"identity={id_text or name_text}; no defensible session date"})
                continue
            else:
                continue

            if parsed_time is None:
                if cancel and clean_text(raw_time):
                    # Cancellation row may still have a normal time; failure is review-worthy.
                    audits.append({"Action":"REVIEW_SOURCE_TIME", "File":path.name, "Sheet":sheet, "Ref":f"R{row_num}", "Date":d.isoformat(), "Venue":venue, "Detail":f"cannot parse cancellation time '{clean_text(raw_time)}'"})
                    continue
                if zero and not clean_text(raw_time):
                    audits.append({"Action":"REVIEW_SOURCE_TIME", "File":path.name, "Sheet":sheet, "Ref":f"R{row_num}", "Date":d.isoformat(), "Venue":venue, "Detail":"zero-attendance session has no parseable source time"})
                    continue
                if not cancel and not zero and has_identity:
                    audits.append({"Action":"REVIEW_SOURCE_TIME", "File":path.name, "Sheet":sheet, "Ref":f"R{row_num}", "Date":d.isoformat(), "Venue":venue, "Detail":f"attendance row has unparseable time '{clean_text(raw_time)}'"})
                continue

            st, et = parsed_time

            # Cannon Hill 09-Jun-2026 contains an obvious incremental end-time typo
            # (12:33, 12:34, 12:35, 12:36, 12:37) inside the normal 11:30-12:30 slot.
            # Normalize only this evidenced source anomaly; do not broadly round session times.
            if sheet == "Cannon Hill" and d == date(2026, 6, 9) and st == time(11, 30) and time(12, 31) <= et <= time(12, 39):
                audits.append({"Action":"SOURCE_TIME_CORRECTED", "File":path.name, "Sheet":sheet, "Ref":f"R{row_num}", "Date":d.isoformat(), "Venue":venue, "Detail":f"{st}-{et} -> 11:30-12:30; incremental source typo within recurring slot"})
                et = time(12, 30)

            person = None
            if not cancel and not zero:
                card, pname, id_quality = parse_card_and_name(identity, name_col)
                if card or pname:
                    person = PersonSource(
                        path.name, sheet, row_num, raw_card=identity, card=card, full_name=pname,
                        dob=parse_dob(dob_v), postcode=normalize_postcode(postcode_v),
                        emergency_name=clean_text(em_name) or None, emergency_phone=normalize_phone(em_phone),
                        risk=clean_text(risk_v) or None,
                        gender=clean_text(row[14] if len(row)>14 else None) or None,
                        ethnicity=clean_text(row[13] if len(row)>13 else None) or None,
                        health_conditions=clean_text(row[10] if len(row)>10 else None) or None,
                    )
                    if id_quality == "SUSPICIOUS_LONG_NUMBER":
                        audits.append({"Action":"SOURCE_SUSPICIOUS_LONG_NUMBER", "File":path.name, "Sheet":sheet, "Ref":f"R{row_num}", "Date":d.isoformat(), "Venue":venue, "Detail":f"source identity/card={clean_text(identity)}"})

            observations.append((sheet,row_num,d,st,et,"SOURCE",person,cancel,zero,clean_text(identity) or clean_text(name_col)))

    wb.close()

    # First normalize obvious per-session end-time typos (e.g. 12:31 ... 12:37)
    ends_by_start: dict[tuple[str,date,time], Counter] = defaultdict(Counter)
    for sheet,row_num,d,st,et,tq,p,cancel,zero,reason in observations:
        ends_by_start[(sheet,d,st)][et] += 1
    modal_end: dict[tuple[str,date,time], time] = {}
    for k,c in ends_by_start.items():
        modal_end[k] = c.most_common(1)[0][0]

    groups: dict[tuple[str,date,time,time], SourceSession] = {}
    for sheet,row_num,d,st,et,tq,p,cancel,zero,reason in observations:
        venue = SHEET_VENUES[sheet]
        preferred_end = modal_end[(sheet,d,st)]
        if et != preferred_end:
            delta = abs((datetime.combine(date.today(),et)-datetime.combine(date.today(),preferred_end)).total_seconds())
            if delta <= 15*60:
                audits.append({"Action":"SOURCE_TIME_CORRECTED_TO_SESSION_MODE", "File":path.name, "Sheet":sheet, "Ref":f"R{row_num}", "Date":d.isoformat(), "Venue":venue, "Detail":f"{st}-{et} -> {st}-{preferred_end}"})
                et = preferred_end
        key=(sheet,d,st,et)
        if key not in groups:
            groups[key]=SourceSession(path.name,sheet,f"{d.isoformat()}|{st}|{sheet}",d,venue,st,et,"SOURCE")
        s=groups[key]
        if cancel:
            s.is_cancelled=True
            s.cancel_reason = reason or "cancelled"
        elif zero:
            s.zero_attendance_marker=True
        elif p:
            s.people.append(p)

    return sorted(groups.values(),key=lambda s:(s.session_date,s.venue_name,s.start_time or time.min)),audits


def enrich_source_identity(sessions: list[SourceSession]) -> list[dict[str,Any]]:
    audits=[]
    people=[p for s in sessions for p in s.people]
    # Canonical name -> cards from rows where both are available.
    name_cards: dict[str,set[str]] = defaultdict(set)
    card_names: dict[str,list[str]] = defaultdict(list)
    for p in people:
        if p.card and p.full_name:
            name_cards[p.name_key].add(p.card)
            card_names[p.card].append(p.full_name)

    # Card -> canonical name only when all observed names are reasonably compatible.
    card_canonical: dict[str,str] = {}
    for card,names in card_names.items():
        unique=[]
        for n in names:
            if normalize_name(n) not in {normalize_name(x) for x in unique}:
                unique.append(n)
        if not unique:
            continue
        base=max(unique,key=lambda x:len(normalize_name(x)))
        if all(name_similarity(base,x)>=0.55 for x in unique):
            card_canonical[card]=base
        else:
            audits.append({"Action":"REVIEW_SOURCE_CARD_NAME_CONFLICT", "File":"multiple", "Sheet":"", "Ref":"", "Date":"", "Venue":"", "Detail":f"card {card} names={unique}"})

    for p in people:
        if not p.card and p.name_key and len(p.name_key.split())>=2:
            cards=name_cards.get(p.name_key,set())
            if len(cards)==1:
                p.card=next(iter(cards))
                audits.append({"Action":"SOURCE_CARD_RESOLVED_FROM_OTHER_ROW", "File":p.source_file, "Sheet":p.source_sheet, "Ref":f"R{p.source_row}", "Date":"", "Venue":"", "Detail":f"{p.full_name} -> card {p.card}"})
        if p.card and not p.full_name and p.card in card_canonical:
            p.full_name=card_canonical[p.card]
            audits.append({"Action":"SOURCE_NAME_INFERRED_FROM_CARD", "File":p.source_file, "Sheet":p.source_sheet, "Ref":f"R{p.source_row}", "Date":"", "Venue":"", "Detail":f"card {p.card} -> {p.full_name}"})
    return audits


def dedupe_source_attendance(sessions: list[SourceSession]) -> tuple[int,list[dict[str,Any]]]:
    removed=0; audits=[]
    for s in sessions:
        seen=set(); keep=[]
        for p in s.people:
            key=("CARD",p.card) if p.card else ("NAME",p.name_key,p.dob or "")
            if key in seen:
                removed+=1
                audits.append({"Action":"SKIP_SOURCE_DUPLICATE_ATTENDANCE", "File":p.source_file, "Sheet":p.source_sheet, "Ref":f"R{p.source_row}", "Date":s.session_date.isoformat(), "Venue":s.venue_name, "Detail":f"duplicate identity {key}"})
                continue
            seen.add(key); keep.append(p)
        s.people=keep
    return removed,audits


def parse_sources(base_dir: Path):
    p24=base_dir/SOURCE_2024; p25=base_dir/SOURCE_2025
    missing=[str(p) for p in (p24,p25) if not p.exists()]
    if missing:
        raise FileNotFoundError("Missing required Tennis workbook(s): " + ", ".join(missing))
    s24,a24=parse_2024(p24)
    s25,a25=parse_vertical_2025(p25)
    sessions=s24+s25
    identity_actions=enrich_source_identity(sessions)
    removed,dedupe_actions=dedupe_source_attendance(sessions)
    sessions.sort(key=lambda s:(s.session_date,s.venue_name,s.start_time or time.min))
    return sessions,a24+a25+identity_actions+dedupe_actions,removed


# -----------------------------------------------------------------------------
# DB helpers
# -----------------------------------------------------------------------------

REQUIRED_COLUMNS = {
    "Participants":{"ParticipantID","SaheliCardNumber","FullName","DateOfBirth","Postcode","MobileNumber","Site","Notes"},
    "LiteMembers":{"Id","MembershipId","FirstName","LastName","DateOfBirth","Phone","Postcode"},
    "Sessions":{"SessionId","Frequency","Category","SubCategory","ActivityCategory","VenueName","ActivityName","Notes","IsRecurringWeekly","DayOfWeek","SessionDate","StartTime","EndTime","IsBookingRequired","IsCancelled"},
    "SessionAttendance":{"AttendanceId","SessionId","AttendanceMemberKind","ParticipantId","LiteMemberId","MemberDisplayId","SaheliCardNumber","MemberName","Phone","EmergencyName","EmergencyPhone","SessionName","SessionDay","SessionDate","SessionMonth","SessionStartTime","SessionEndTime","RiskStratification","Attended","Notes"},
}


def fetch_dicts(cur,sql,params=()):
    cur.execute(sql,params); cols=[d[0] for d in cur.description]
    return [dict(zip(cols,r)) for r in cur.fetchall()]


def preflight_schema(cur):
    for table,expected in REQUIRED_COLUMNS.items():
        rows=cur.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME=?",table).fetchall()
        actual={r[0] for r in rows}; missing=sorted(expected-actual)
        if missing:
            raise RuntimeError(f"dbo.{table} missing required columns: {missing}")


def load_db_people(cur):
    by_card={}; full_by_name=defaultdict(list)
    for r in fetch_dicts(cur,"SELECT ParticipantID,SaheliCardNumber,FullName,DateOfBirth,Postcode,MobileNumber FROM dbo.Participants"):
        d=r["DateOfBirth"].date() if isinstance(r["DateOfBirth"],datetime) else r["DateOfBirth"]
        p=DbParticipant(int(r["ParticipantID"]),clean_text(r["SaheliCardNumber"]),clean_text(r["FullName"]),d,normalize_postcode(r["Postcode"]),normalize_phone(r["MobileNumber"]))
        if p.card: by_card[p.card]=p
        if normalize_name(p.full_name): full_by_name[normalize_name(p.full_name)].append(p)
    lites_by_key=defaultdict(list)
    for r in fetch_dicts(cur,"SELECT Id,MembershipId,FirstName,LastName,DateOfBirth,Postcode,Phone FROM dbo.LiteMembers"):
        d=r["DateOfBirth"].date() if isinstance(r["DateOfBirth"],datetime) else r["DateOfBirth"]
        l=DbLite(str(r["Id"]),clean_text(r["MembershipId"]),clean_text(r["FirstName"]),clean_text(r["LastName"]),d,normalize_postcode(r["Postcode"]),normalize_phone(r["Phone"]))
        lites_by_key[normalize_name(l.full_name)].append(l)
    return by_card,full_by_name,lites_by_key


def next_lite_number(cur):
    mx=0
    for (mid,) in cur.execute("SELECT MembershipId FROM dbo.LiteMembers WITH (UPDLOCK,HOLDLOCK)").fetchall():
        m=re.fullmatch(r"LITE-(\d+)",clean_text(mid),re.I)
        if m: mx=max(mx,int(m.group(1)))
    return mx+1


def load_db_sessions(cur,sessions):
    mn=min(s.session_date for s in sessions); mx=max(s.session_date for s in sessions)
    rows=fetch_dicts(cur,"""
      SELECT s.SessionId,s.SessionDate,s.VenueName,s.ActivityName,s.StartTime,s.EndTime,s.IsCancelled,COUNT(a.AttendanceId) AttendanceCount
      FROM dbo.Sessions s LEFT JOIN dbo.SessionAttendance a ON a.SessionId=s.SessionId AND a.Attended=1
      WHERE s.SessionDate>=? AND s.SessionDate<=? AND UPPER(LTRIM(RTRIM(s.ActivityName)))='TENNIS'
      GROUP BY s.SessionId,s.SessionDate,s.VenueName,s.ActivityName,s.StartTime,s.EndTime,s.IsCancelled
    """,(mn,mx))
    result=[]
    for r in rows:
        if norm_venue(r["VenueName"]) not in {norm_venue(v) for v in VALID_VENUES}: continue
        d=r["SessionDate"].date() if isinstance(r["SessionDate"],datetime) else r["SessionDate"]
        st=r["StartTime"].time() if isinstance(r["StartTime"],datetime) else r["StartTime"]
        et=r["EndTime"].time() if isinstance(r["EndTime"],datetime) else r["EndTime"]
        result.append(DbSession(int(r["SessionId"]),d,clean_text(r["VenueName"]),st,et,bool(r["IsCancelled"]),int(r["AttendanceCount"] or 0),False))
    return result


def load_existing_attendance_keys(cur,sessions):
    mn=min(s.session_date for s in sessions); mx=max(s.session_date for s in sessions); keys=set()
    rows=cur.execute("""SELECT a.SessionId,a.AttendanceMemberKind,a.ParticipantId,a.LiteMemberId
                        FROM dbo.SessionAttendance a JOIN dbo.Sessions s ON s.SessionId=a.SessionId
                        WHERE s.SessionDate>=? AND s.SessionDate<=? AND UPPER(LTRIM(RTRIM(s.ActivityName)))='TENNIS'""",mn,mx).fetchall()
    for sid,kind,pid,lid in rows:
        kind=(kind or "").upper()
        mid=str(pid) if kind=="FULL" else (str(lid).lower() if lid else "")
        if mid: keys.add((int(sid),kind,mid))
    return keys


def load_tennis_template(cur,venue):
    rows=fetch_dicts(cur,"""
      SELECT TOP 20 SessionId,Category,SubCategory,ActivityCategory,IsBookingRequired,VenueName,SessionDate
      FROM dbo.Sessions
      WHERE UPPER(LTRIM(RTRIM(ActivityName)))='TENNIS'
        AND Category IS NOT NULL
      ORDER BY CASE WHEN UPPER(LTRIM(RTRIM(VenueName)))=UPPER(LTRIM(RTRIM(?))) THEN 0 ELSE 1 END,
               SessionDate DESC, SessionId DESC
    """,(venue,))
    return rows[0] if rows else None


def session_candidates(source,db_sessions,preexisting_only=False):
    c=[x for x in db_sessions if x.session_date==source.session_date and norm_venue(x.venue_name)==norm_venue(source.venue_name)]
    if preexisting_only: c=[x for x in c if not x.created_in_run]
    return c


def exact_session_match(source,db_sessions):
    c=session_candidates(source,db_sessions)
    exact=[x for x in c if x.start_time==source.start_time and x.end_time==source.end_time]
    if exact: return sorted(exact,key=lambda x:(-x.attendance_count,x.session_id))[0]
    same_start=[x for x in c if x.start_time==source.start_time]
    if len(same_start)==1: return same_start[0]
    return None


def create_session(cur,source,template):
    if not source.start_time or not source.end_time:
        raise ValueError("Cannot create Tennis session without a defensible source time")
    category=clean_text(template.get("Category")); sub=clean_text(template.get("SubCategory")); ac=clean_text(template.get("ActivityCategory"))
    if not category: raise ValueError("Tennis template has NULL/blank Category")
    notes=(f"{MIGRATION_NOTE_PREFIX}; source={source.source_file}/{source.source_sheet}/{source.source_ref}; "
           f"time_quality={source.time_quality}; template_session={template.get('SessionId')}; cancelled={int(source.is_cancelled)}; "
           f"reason={source.cancel_reason or ''}")[:1000]
    cur.execute("""INSERT INTO dbo.Sessions
      (Frequency,Category,SubCategory,ActivityCategory,VenueName,AssignedStaffId,SessionProviderId,ActivityName,Notes,
       IsRecurringWeekly,DayOfWeek,SessionDate,ArrivalTime,StartTime,EndTime,Capacity,IsBookingRequired,IsCancelled,CreatedAtUtc)
      OUTPUT INSERTED.SessionId
      VALUES(?,?,?,?,?,NULL,NULL,'Tennis',?,0,NULL,?,NULL,?,?,NULL,?,?,SYSUTCDATETIME())""",
      DEFAULT_FREQUENCY[:60],category[:30],sub[:100] or None,ac[:100] or None,source.venue_name,notes,source.session_date,
      source.start_time,source.end_time,1 if template.get("IsBookingRequired") else 0,1 if source.is_cancelled else 0)
    sid=int(cur.fetchone()[0])
    return DbSession(sid,source.session_date,source.venue_name,source.start_time,source.end_time,source.is_cancelled,0,True)


def insert_full(cur,card,p,venue):
    name=clean_text(p.full_name)
    cur.execute("""INSERT INTO dbo.Participants
      (SaheliCardNumber,FullName,DateOfBirth,Postcode,MobileNumber,Gender,Ethnicity,Site,Notes,CreatedAt)
      OUTPUT INSERTED.ParticipantID
      VALUES(?,?,?,?,?,?,?,?,?,SYSDATETIME())""",
      card[:50],name[:510] if name else None,p.dob,(p.postcode or "")[:40] or None,(p.phone or "")[:100] or None,
      (clean_text(p.gender) or "")[:40] or None,clean_text(p.ethnicity) or None,venue,
      f"{MIGRATION_NOTE_PREFIX}; created from historical Tennis source"[:4000])
    return DbParticipant(int(cur.fetchone()[0]),card,name,p.dob,p.postcode,p.phone)


def insert_lite(cur,membership_id,p):
    first,last=split_name(p.full_name)
    if not first: raise ValueError("Cannot create Lite member without usable name")
    last=last or NO_SURNAME_LABEL; lid=str(uuid.uuid4())
    cur.execute("""INSERT INTO dbo.LiteMembers
      (Id,MembershipId,FirstName,LastName,DateOfBirth,Phone,Email,Address,Postcode,EmergencyName,EmergencyPhone,EmergencyRelation,
       HealthConditions,Gender,Ethnicity,CreatedAtUtc,CreatedByUserId)
      VALUES(?,?,?,?,?,?,NULL,NULL,?,?,?,NULL,?,?,?,SYSUTCDATETIME(),NULL)""",
      lid,membership_id[:50],first[:100],last[:100],p.dob,(p.phone or "")[:30] or None,(p.postcode or "")[:30] or None,
      (clean_text(p.emergency_name) or "")[:200] or None,(p.emergency_phone or "")[:30] or None,
      clean_text(p.health_conditions) or None,(clean_text(p.gender) or "")[:100] or None,(clean_text(p.ethnicity) or "")[:200] or None)
    return DbLite(lid,membership_id,first,last,p.dob,p.postcode,p.phone)


def choose_lite(cands,p):
    if not cands: return None,"NO_MATCH"
    # Exact DOB conflict means source person is distinct; do not merge solely on name.
    compatible=[]
    for c in cands:
        if p.dob and c.dob and p.dob != c.dob:
            continue
        compatible.append(c)
    if not compatible:
        return None,"NAME_EXISTS_BUT_PROFILE_CONFLICT"
    if len(compatible)==1:
        return compatible[0],"EXACT_NAME"
    scored=[]
    for c in compatible:
        score=0
        if p.dob and c.dob and p.dob==c.dob: score+=4
        if p.postcode and c.postcode and p.postcode==c.postcode: score+=3
        if p.phone and c.phone and p.phone==c.phone: score+=3
        scored.append((score,c))
    scored.sort(key=lambda x:(-x[0],x[1].membership_id))
    if scored and scored[0][0]>0 and (len(scored)==1 or scored[0][0]>scored[1][0]):
        return scored[0][1],"NAME_AND_PROFILE"
    return None,"AMBIGUOUS_NAME"


def resolve_member(cur,p,source,participants_by_card,full_by_name,lites_by_key,lite_state,cache,log):
    cache_key=("CARD",p.card) if p.card else ("NAME",display_lite_key(p.full_name or ""),p.dob,p.postcode,p.phone)
    if cache_key in cache: return cache[cache_key]

    if p.card:
        card=p.card
        existing=participants_by_card.get(card)
        if existing:
            if p.full_name:
                sim=name_similarity(p.full_name,existing.full_name)
                # Only block clear contradictions; card remains primary identity.
                if sim < 0.42:
                    log.add("REVIEW_CARD_NAME_MISMATCH",source,p,detail=f"card {card} CRM='{existing.full_name}' source='{p.full_name}' similarity={sim:.2f}")
                    return None
            result=("FULL",str(existing.participant_id),existing.card,existing.full_name or p.full_name,"REUSE_FULL_BY_CARD")
            cache[cache_key]=result; return result

        if len(re.sub(r"\D","",card))>=7:
            if p.full_name:
                # Long number is unlikely a Saheli card; preserve person as Lite and audit.
                log.add("SOURCE_LONG_NUMBER_RECLASSIFIED_TO_LITE",source,p,detail=f"source value {card}; no matching CRM FULL card")
                p.card=None
                return resolve_member(cur,p,source,participants_by_card,full_by_name,lites_by_key,lite_state,cache,log)
            log.add("REVIEW_SUSPICIOUS_CARD_WITHOUT_NAME",source,p,detail=f"source card/value={card}; not in CRM; no usable source name")
            return None

        if not p.full_name:
            log.add("REVIEW_MISSING_FULL_NAME",source,p,detail=f"card={card} not in CRM; cannot create nameless FULL participant")
            return None
        if not CREATE_MISSING_FULL_PARTICIPANTS:
            log.add("REVIEW_MISSING_FULL",source,p,detail=f"card={card}"); return None
        try:
            dbp=insert_full(cur,card,p,source.venue_name)
            participants_by_card[card]=dbp; full_by_name[normalize_name(dbp.full_name)].append(dbp)
            result=("FULL",str(dbp.participant_id),card,dbp.full_name,"CREATE_FULL_FROM_VALID_CARD")
            cache[cache_key]=result
            return result
        except Exception as exc:
            log.add("REVIEW_FULL_CREATE_FAILED",source,p,detail=str(exc)); return None

    if not p.full_name:
        log.add("REVIEW_CARDLESS_WITHOUT_NAME",source,p,detail="no card and no usable name"); return None

    # Reuse FULL by exact name only when source profile gives corroborating evidence.
    fn=normalize_name(p.full_name); fulls=full_by_name.get(fn,[])
    if len(fulls)==1:
        f=fulls[0]
        profile_match=(p.dob and f.dob and p.dob==f.dob) or (p.postcode and f.postcode and p.postcode==f.postcode) or (p.phone and f.mobile and p.phone==f.mobile)
        if profile_match:
            result=("FULL",str(f.participant_id),f.card,f.full_name,"REUSE_FULL_BY_NAME_AND_PROFILE")
            cache[cache_key]=result; return result

    lite_key=display_lite_key(p.full_name)
    cands=lites_by_key.get(lite_key,[])
    lite,why=choose_lite(cands,p)
    if lite:
        result=("LITE",lite.lite_id,lite.membership_id,lite.full_name,f"REUSE_LITE_BY_{why}")
        cache[cache_key]=result; return result
    if why=="AMBIGUOUS_NAME":
        bits=" | ".join(f"{x.membership_id}[dob={x.dob or '-'},postcode={x.postcode or '-'}]" for x in cands)
        log.add("REVIEW_AMBIGUOUS_LITE_NAME",source,p,detail=bits); return None

    if not CREATE_MISSING_LITE_MEMBERS:
        log.add("REVIEW_MISSING_LITE",source,p,detail="no safe existing Lite match"); return None
    mid=f"LITE-{lite_state[0]}"; lite_state[0]+=1
    try:
        lite=insert_lite(cur,mid,p); lites_by_key[lite_key].append(lite)
        result=("LITE",lite.lite_id,lite.membership_id,lite.full_name,"CREATE_LITE")
        cache[cache_key]=result; return result
    except Exception as exc:
        log.add("REVIEW_LITE_CREATE_FAILED",source,p,detail=str(exc)); return None


def insert_attendance(cur,dbs,source,p,resolved):
    kind,mid,display,name,resolution=resolved
    pid=int(mid) if kind=="FULL" else None; lid=None if kind=="FULL" else mid
    card=display if kind=="FULL" else None
    note=(f"{MIGRATION_NOTE_PREFIX}; source={source.source_file}/{source.source_sheet}/R{p.source_row}; "
          f"source_session={source.source_ref}; time_quality={source.time_quality}; source_identity={clean_text(p.raw_card)}")[:1000]
    cur.execute("""INSERT INTO dbo.SessionAttendance
      (SessionId,AttendanceMemberKind,ParticipantId,LiteMemberId,MemberDisplayId,SaheliCardNumber,MemberName,Phone,EmergencyName,EmergencyPhone,
       SessionName,SessionDay,SessionDate,SessionMonth,SessionStartTime,SessionEndTime,RiskStratification,Attended,Notes,MedicalCondition,CreatedAtUtc,UpdatedAtUtc)
      VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,?,?,SYSUTCDATETIME(),NULL)""",
      dbs.session_id,kind,pid,lid,str(display)[:50],str(card)[:50] if card else None,(clean_text(name) or clean_text(p.full_name))[:200] or None,
      (p.phone or "")[:30] or None,(clean_text(p.emergency_name) or "")[:200] or None,(p.emergency_phone or "")[:30] or None,
      ACTIVITY,source.session_date.strftime("%A")[:20],source.session_date,source.session_date.strftime("%B")[:20],dbs.start_time,dbs.end_time,
      (clean_text(p.risk) or "")[:100] or None,note,(clean_text(p.health_conditions) or "")[:1000] or None)


# -----------------------------------------------------------------------------
# Logging / reports
# -----------------------------------------------------------------------------

class MigrationLog:
    def __init__(self): self.rows=[]; self.counts=Counter()
    def add(self,action,source=None,person=None,detail="",session_id=None,member_ref=""):
        self.counts[action]+=1
        self.rows.append({
            "Action":action,
            "Date":source.session_date.isoformat() if source else "",
            "Venue":source.venue_name if source else "",
            "Activity":ACTIVITY,
            "SourceFile":source.source_file if source else "",
            "SourceSheet":source.source_sheet if source else "",
            "SourceSessionRef":source.source_ref if source else "",
            "SessionId":session_id or "",
            "SourceCard":person.card if person else "",
            "SourceRawIdentity":clean_text(person.raw_card) if person else "",
            "SourceName":clean_text(person.full_name) if person else "",
            "MemberRef":member_ref,
            "Detail":detail,
        })
    def write(self,path):
        fields=["Action","Date","Venue","Activity","SourceFile","SourceSheet","SourceSessionRef","SessionId","SourceCard","SourceRawIdentity","SourceName","MemberRef","Detail"]
        with path.open("w",newline="",encoding="utf-8-sig") as f:
            w=csv.DictWriter(f,fieldnames=fields); w.writeheader(); w.writerows(self.rows)


def write_source_audit(rows,out):
    fields=["Action","File","Sheet","Ref","Date","Venue","Detail"]
    with out.open("w",newline="",encoding="utf-8-sig") as f:
        w=csv.DictWriter(f,fieldnames=fields); w.writeheader();
        for r in rows: w.writerow({k:r.get(k,"") for k in fields})


def print_source_summary(sessions,source_actions,dupes):
    dated_2024=[s for s in sessions if s.source_file==SOURCE_2024]
    later=[s for s in sessions if s.source_file==SOURCE_2025]
    clean_att=sum(len(s.people) for s in sessions)
    cancelled=sum(1 for s in sessions if s.is_cancelled)
    zero=sum(1 for s in sessions if not s.is_cancelled and not s.people)
    print("\nTennis source audit")
    print("-----------------------")
    print(f"Parsed dated session groups: {len(sessions)}")
    print(f"  2024 no-time session groups: {len(dated_2024)}")
    print(f"  2025/26 timed session groups: {len(later)}")
    print(f"Clean attendance rows: {clean_att}")
    print(f"Source duplicate attendance removed: {dupes}")
    print(f"Cancelled/closure sessions: {cancelled}")
    print(f"Zero-attendance non-cancelled sessions: {zero}")
    print(f"Period: {min(s.session_date for s in sessions)} to {max(s.session_date for s in sessions)}")
    byvenue=Counter()
    for s in sessions: byvenue[s.venue_name]+=len(s.people)
    print("Attendance by venue:")
    for v,n in sorted(byvenue.items()): print(f"  {v}: {n}")
    reviews=Counter(r["Action"] for r in source_actions if r["Action"].startswith("REVIEW_"))
    if reviews:
        print("Source review items:")
        for k,n in sorted(reviews.items()): print(f"  {k}: {n}")


# -----------------------------------------------------------------------------
# Migration runner
# -----------------------------------------------------------------------------

def run_migration(commit=False,audit_only=False,start_filter=None,end_filter=None):
    print("Saheli CRM - Tennis Full Historical Migration V1")
    print(f"Source directory: {BASE_DIR}")
    for f in (SOURCE_2024,SOURCE_2025):
        print(f"  {'OK' if (BASE_DIR/f).exists() else 'MISSING'} {f}")

    sessions,source_actions,dupes=parse_sources(BASE_DIR)
    if start_filter: sessions=[s for s in sessions if s.session_date>=start_filter]
    if end_filter: sessions=[s for s in sessions if s.session_date<=end_filter]
    if not sessions: raise RuntimeError("No Tennis sessions remain after source/date filtering")
    print_source_summary(sessions,source_actions,dupes)
    audit_path=BASE_DIR/f"tennis_source_audit_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    write_source_audit(source_actions,audit_path)
    print(f"Source audit CSV: {audit_path}")
    if audit_only:
        print("AUDIT ONLY COMPLETE: database was not accessed.")
        return 0

    if not CONNECTION_STRING:
        raise RuntimeError("No SQL connection string. Set SAHELI_SQL_CONNECTION_STRING or pass --connection-string.")
    try: import pyodbc
    except ImportError as exc: raise RuntimeError("pyodbc is required. Run: py -m pip install pyodbc openpyxl") from exc

    cn=pyodbc.connect(CONNECTION_STRING,autocommit=False); cur=cn.cursor(); cur.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
    log=MigrationLog()
    try:
        preflight_schema(cur)
        participants_by_card,full_by_name,lites_by_key=load_db_people(cur)
        db_sessions=load_db_sessions(cur,sessions)
        existing_keys=load_existing_attendance_keys(cur,sessions)
        lite_state=[next_lite_number(cur)]
        initial_session_ids={x.session_id for x in db_sessions}
        template_cache={}
        member_cache={}

        # Source-level REVIEW items are real blockers too.
        for r in source_actions:
            if r["Action"].startswith("REVIEW_"):
                # Attach to matching source session where possible; generic source blocker otherwise.
                log.counts[r["Action"]]+=1
                log.rows.append({"Action":r["Action"],"Date":r.get("Date",""),"Venue":r.get("Venue",""),"Activity":ACTIVITY,"SourceFile":r.get("File",""),"SourceSheet":r.get("Sheet",""),"SourceSessionRef":r.get("Ref",""),"SessionId":"","SourceCard":"","SourceRawIdentity":"","SourceName":"","MemberRef":"","Detail":r.get("Detail","")})

        for source in sessions:
            preexisting=session_candidates(source,db_sessions,preexisting_only=True)
            if source.time_quality=="SOURCE_NO_TIME":
                if len(preexisting)==1:
                    dbs=preexisting[0]
                    log.add("REUSE_2024_SESSION_BY_UNAMBIGUOUS_DATE",source,detail=f"SessionId={dbs.session_id}; CRM time={dbs.start_time}-{dbs.end_time}",session_id=dbs.session_id)
                elif len(preexisting)==0:
                    log.add("REVIEW_2024_SESSION_TIME_MISSING",source,detail="2024 source has no time and CRM has no Tennis session for this venue/date; refusing to invent time")
                    continue
                else:
                    detail=", ".join(f"{x.session_id}:{x.start_time}-{x.end_time}" for x in sorted(preexisting,key=lambda z:z.session_id))
                    log.add("REVIEW_2024_SESSION_AMBIGUOUS",source,detail=f"multiple CRM Tennis sessions on venue/date: {detail}")
                    continue
            else:
                dbs=exact_session_match(source,db_sessions)
                if dbs:
                    log.add("REUSE_SESSION",source,detail=f"SessionId={dbs.session_id}; CRM time={dbs.start_time}-{dbs.end_time}",session_id=dbs.session_id)
                else:
                    # A pre-existing same-date Tennis session at a different time is review-worthy.
                    conflicting=[x for x in preexisting if x.session_id in initial_session_ids]
                    if conflicting:
                        detail=", ".join(f"{x.session_id}:{x.start_time}-{x.end_time}" for x in sorted(conflicting,key=lambda z:z.session_id))
                        log.add("REVIEW_SESSION_TIME_CONFLICT",source,detail=f"source={source.start_time}-{source.end_time}; CRM same-date candidates={detail}")
                        continue
                    template=template_cache.get(source.venue_name)
                    if template is None:
                        template=load_tennis_template(cur,source.venue_name); template_cache[source.venue_name]=template
                    if not template:
                        log.add("REVIEW_NO_TENNIS_TEMPLATE",source,detail="No existing Tennis session with Category metadata found to template new session")
                        continue
                    try:
                        dbs=create_session(cur,source,template); db_sessions.append(dbs)
                        log.add("CREATE_SESSION",source,detail=f"SessionId={dbs.session_id}; template={template.get('SessionId')}",session_id=dbs.session_id)
                    except Exception as exc:
                        log.add("REVIEW_SESSION_CREATE_FAILED",source,detail=str(exc)); continue

            if source.is_cancelled:
                if dbs.attendance_count>0 and not dbs.created_in_run:
                    log.add("REVIEW_CANCELLED_SESSION_HAS_ATTENDANCE",source,detail=f"SessionId={dbs.session_id} has {dbs.attendance_count} attended CRM rows",session_id=dbs.session_id)
                elif not dbs.is_cancelled:
                    cur.execute("UPDATE dbo.Sessions SET IsCancelled=1 WHERE SessionId=?",dbs.session_id); dbs.is_cancelled=True
                    log.add("MARK_SESSION_CANCELLED",source,detail=source.cancel_reason or "cancelled",session_id=dbs.session_id)
                log.add("SOURCE_CANCELLED_SESSION",source,detail=source.cancel_reason or "cancelled",session_id=dbs.session_id)
                continue

            if not source.people:
                log.add("SOURCE_ZERO_ATTENDANCE_SESSION",source,detail="session has no valid attendee rows",session_id=dbs.session_id)
                continue

            for p in source.people:
                resolved=resolve_member(cur,p,source,participants_by_card,full_by_name,lites_by_key,lite_state,member_cache,log)
                if not resolved: continue
                kind,mid,display,name,resolution=resolved
                key=(dbs.session_id,kind,str(mid) if kind=="FULL" else str(mid).lower())
                if key in existing_keys:
                    log.add("SKIP_EXISTING_ATTENDANCE",source,p,detail=resolution,session_id=dbs.session_id,member_ref=f"{kind}:{mid}")
                    continue
                try:
                    insert_attendance(cur,dbs,source,p,resolved); existing_keys.add(key); dbs.attendance_count+=1
                    log.add("CREATE_ATTENDANCE",source,p,detail=resolution,session_id=dbs.session_id,member_ref=f"{kind}:{mid}")
                except Exception as exc:
                    log.add("REVIEW_ATTENDANCE_INSERT_FAILED",source,p,detail=str(exc),session_id=dbs.session_id,member_ref=f"{kind}:{mid}")
                    raise

        stamp=datetime.now().strftime("%Y%m%d_%H%M%S")
        mode="commit" if commit else "preview"
        action_path=BASE_DIR/f"tennis_migration_{mode}_{stamp}.csv"; log.write(action_path)
        summary_path=BASE_DIR/f"tennis_migration_summary_{stamp}.txt"
        reviews=sum(n for a,n in log.counts.items() if a.startswith("REVIEW_"))
        with summary_path.open("w",encoding="utf-8") as f:
            f.write("Saheli CRM - Tennis historical migration\n")
            f.write(f"Mode: {mode}\nSessions in source: {len(sessions)}\nClean attendance: {sum(len(s.people) for s in sessions)}\nReview blockers: {reviews}\n\n")
            for a,n in sorted(log.counts.items()): f.write(f"{a}: {n}\n")

        print("\n"+("COMMIT CHECK" if commit else "PREVIEW COMPLETE"))
        print(f"Actions CSV: {action_path}")
        print(f"Summary:     {summary_path}")
        print(f"Review blockers: {reviews}")
        print("Action summary:")
        for a,n in sorted(log.counts.items()): print(f"  {a}: {n}")

        if commit:
            if reviews:
                cn.rollback(); print("\nCOMMIT BLOCKED: review-required items exist; transaction rolled back; CRM unchanged."); return 3
            cn.commit(); print("\nCOMMIT COMPLETE: Tennis migration committed successfully.")
        else:
            cn.rollback(); print("\nPREVIEW COMPLETE: transaction rolled back; CRM was not changed.")
            print("Do not use --commit until every REVIEW_* item has been reconciled.")
        return 0
    except Exception:
        cn.rollback(); raise
    finally:
        cur.close(); cn.close()


def parse_cli_date(v):
    return datetime.strptime(v,"%Y-%m-%d").date() if v else None


def main():
    ap=argparse.ArgumentParser(description="Saheli CRM Tennis full historical migration")
    ap.add_argument("--audit-only",action="store_true",help="Parse source only; do not connect to SQL")
    ap.add_argument("--commit",action="store_true",help="Commit only when preview has zero REVIEW_* blockers")
    ap.add_argument("--start",help="Optional inclusive source start date YYYY-MM-DD")
    ap.add_argument("--end",help="Optional inclusive source end date YYYY-MM-DD")
    ap.add_argument("--connection-string",help="Optional SQL connection string override")
    args=ap.parse_args()
    global CONNECTION_STRING
    if args.connection_string: CONNECTION_STRING=args.connection_string
    start=parse_cli_date(args.start); end=parse_cli_date(args.end)
    if start and end and start>end: raise SystemExit("--start cannot be after --end")
    return run_migration(args.commit,args.audit_only,start,end)


if __name__=="__main__":
    try: raise SystemExit(main())
    except Exception as exc:
        print(f"\nFATAL: {exc}",file=sys.stderr)
        import traceback; traceback.print_exc(); raise SystemExit(1)
