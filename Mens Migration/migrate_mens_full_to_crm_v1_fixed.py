#!/usr/bin/env python3
"""
Saheli CRM - Men's Programme Full Historical Migration V1
==========================================================

Reads the supplied Men's programme staff registers and migrates confidently
parsed sessions and attendance into the current Saheli CRM schema. PREVIEW is
the default; use --commit only after reviewing the generated CSV and confirming
that REVIEW_* = 0.

SOURCE RULES
------------
* The latest Zaheer "FFF Know Your Numbers" workbook is authoritative. The
  older non-Zaheer workbook is detected but excluded as an overlapping snapshot.
* Men's Sessions 2026 supplies Men's Multisports, Men's Exercise, Lunch Club,
  Digital Skills and Men's Walk & Talk. Blank/template sheets are ignored.
* A Saheli Card is treated as FULL identity. Cardless people are LITE and are
  matched by normalized first + last name before a new LiteMember is created.
* Non-Saheli source identifiers such as L-14 are not treated as Saheli Cards.
* Existing CRM sessions and existing member/session attendance are reused.
* Source duplicate attendance marks are removed before database comparison.
* No existing CRM attendance is deleted by this script.

The script does not overwrite existing participant/profile data.
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
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Iterable, Optional

from openpyxl import load_workbook
from openpyxl.utils import get_column_letter

# -----------------------------------------------------------------------------
# CONFIGURATION
# -----------------------------------------------------------------------------

# Keep database credentials outside source control.
# Set SAHELI_SQL_CONNECTION_STRING in the environment before database preview/commit.
CONNECTION_STRING = os.getenv("SAHELI_SQL_CONNECTION_STRING", "").strip()

BASE_DIR = Path(__file__).resolve().parent
ARCC_VENUE = "Alum Rock Community Centre"
OMNIA_VENUE = "Omnia Medical Practice"
ARCC_VENUE_ALIASES = {"arcc", "alum rock", "alum rock community centre"}
OMNIA_VENUE_ALIASES = {"omnia", "omnia medical", "omnia medical practice"}
ALL_VENUE_ALIASES = {
    "alum rock community centre", "calthorpe wellbeing hub", "ward end park",
    "bahu trust", "sparkbrook community centre", "strensham rd", "parkfield",
    "saltley centre"
}

# Default = migrate all confidently parsed dates in the supplied files.
# Override at runtime with --start YYYY-MM-DD / --end YYYY-MM-DD.
DEFAULT_START_DATE: Optional[date] = None
DEFAULT_END_DATE: Optional[date] = date.today()

CREATE_MISSING_FULL_PARTICIPANTS = True
CREATE_MISSING_LITE_MEMBERS = True
UPDATE_EXISTING_ATTENDANCE_TO_ATTENDED = True
ALLOW_PLACEHOLDER_TIMES = True
PLACEHOLDER_BASE_HOUR = 7
PLACEHOLDER_DURATION_MINUTES = 45
PLACEHOLDER_GAP_MINUTES = 15

DEFAULT_FREQUENCY = "Historical"
DEFAULT_CATEGORY = "Physical Activity"
MIGRATION_NOTE_PREFIX = "Historical Men's programme register import"
NO_SURNAME_LABEL = "Unknown"
SUSPICIOUS_LONG_NUMERIC_CARD_MIN_DIGITS = 7

INVALID_TEXT = {"", "#n/a", "#ref!", "#value!", "#name?", "none", "null", "nan", "0"}
YES_VALUES = {"yes", "y", "true", "1", "attended", "x", "✓", "✔"}
RESTRICTED_LIVE_SESSION_MARKERS = {
    "closed session high risk only",
}

CANCEL_MARKERS = {
    "cancelled", "canceled", "do not book", "bank holiday", "no class", "no session",
    "closed", "closure", "eid", "holiday", "staff training", "unavailable"
}
NON_PERSON_MARKERS = CANCEL_MARKERS | {
    "no one attended", "none attended", "no attendance", "party", "term time only"
}

ACTIVITY_RULES: list[tuple[list[str], str]] = [
    (["innerva"], "Innerva"),
    (["chair pilates"], "Chair Pilates"),
    (["chair based", "chair exercise", "chair based exercise", "chair based excercise", "women chair based"], "Chair Based Exercise"),
    (["mens exercise", "men's exercise", "mens session", "men s exercise", "mens chair based"], "Men's Exercise"),
    (["pilates"], "Pilates"),
    (["circuit"], "Circuit Training"),
    (["strength & stretch", "strength and stretch"], "Strength & Stretch"),
    (["yoga"], "Yoga"),
    (["art social", "saheli social", "social morning", "warm hub"], "Saheli Social"),
    (["crochet", "knit and crochet", "crochet and knitting"], "Crochet"),
    (["salsa"], "Salsa"),
    (["body conditioning"], "Body Conditioning"),
    (["walk & talk", "walk and talk", "walk " , "walk"], "Walk & Talk"),
    (["tennis"], "Tennis"),
    (["digital skills"], "Digital Skills"),
    (["standing exercise"], "Standing Exercise"),
    (["exercise class", "womens exercise class", "women exercise class"], "Exercise Class"),
    (["ladies education carb", "education carbs"], "Education - Carbs"),
    (["education stress"], "Education - Stress"),
    (["learn and live"], "Learn & Live"),
    (["self defence", "self defense"], "Self Defence"),
]

# -----------------------------------------------------------------------------
# NORMALISATION
# -----------------------------------------------------------------------------

def clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, bool):
        return "Yes" if value else None
    text = str(value).replace("\xa0", " ").strip()
    if normalize_header(text) in INVALID_TEXT:
        return None
    return re.sub(r"\s+", " ", text).strip() or None


def normalize_header(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).lower().replace("&", " and ")
    return re.sub(r"[^a-z0-9]+", "", text)


def normalize_words(value: Any) -> str:
    text = clean_text(value) or ""
    text = text.lower().replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def normalize_name_piece(value: Any) -> str:
    return normalize_words(value)


def normalize_full_name_key(value: Any) -> str:
    return normalize_words(value)


def split_name(full_name: Any) -> tuple[Optional[str], Optional[str]]:
    text = clean_text(full_name)
    if not text:
        return None, None
    # Keep real names but remove common title prefixes for Lite matching.
    text = re.sub(r"^(mr|mrs|ms|miss|dr)\.?\s+", "", text, flags=re.I)
    bits = text.split()
    if not bits:
        return None, None
    if len(bits) == 1:
        return bits[0], None
    return bits[0], " ".join(bits[1:])


def normalize_name_key(first: Any, last: Any) -> str:
    return f"{normalize_name_piece(first)}|{normalize_name_piece(last)}"


def normalize_card(value: Any) -> Optional[str]:
    text = clean_text(value)
    if not text:
        return None
    text = text.upper().strip()
    if text in {"ALREADY MEMBER", "EXISTING MEMBER", "MEMBER"}:
        return None
    text = re.sub(r"^SAH(?:ELI)?[-\s]*", "", text)
    text = text.strip(" .")
    if re.fullmatch(r"\d+(?:\.0+)?", text):
        return str(int(float(text)))
    if re.fullmatch(r"\d+", text):
        return text
    return text or None


def card_candidates(value: Any) -> list[str]:
    text = clean_text(value)
    if not text:
        return []
    # Composite cards are reviewable rather than guessed; split common separators.
    parts = re.split(r"[/,;&]+", text)
    out = []
    for p in parts:
        c = normalize_card(p)
        if c and normalize_words(c) not in {"already member", "existing member"}:
            if c not in out:
                out.append(c)
    return out


def normalize_card_key(value: Any) -> str:
    c = normalize_card(value)
    return (c or "").lower().replace(" ", "")


def looks_like_misfiled_wellbeing_number(value: Any) -> bool:
    c = normalize_card(value)
    return bool(c and c.isdigit() and len(c) >= SUSPICIOUS_LONG_NUMERIC_CARD_MIN_DIGITS)


def normalize_postcode(value: Any) -> str:
    return re.sub(r"\s+", "", (clean_text(value) or "").upper())


def normalize_phone(value: Any) -> str:
    text = clean_text(value) or ""
    digits = re.sub(r"\D", "", text)
    if digits.startswith("44") and len(digits) >= 12:
        digits = "0" + digits[2:]
    return digits


def is_yes(value: Any) -> bool:
    if value is True:
        return True
    if isinstance(value, (int, float)) and value == 1:
        return True
    return normalize_words(value) in YES_VALUES


def is_cancel_text(value: Any) -> bool:
    w = normalize_words(value)
    if not w:
        return False
    # ARCC Innerva uses "CLOSED SESSION - High Risk Only" to mean a
    # restricted live session.  Those slots can contain genuine Attended=Yes
    # records and must not be converted into cancelled sessions.
    if w in RESTRICTED_LIVE_SESSION_MARKERS:
        return False
    return any(marker in w for marker in CANCEL_MARKERS)


def canonical_activity(value: Any) -> Optional[str]:
    w = normalize_words(value)
    if not w:
        return None
    for aliases, canonical in ACTIVITY_RULES:
        for alias in aliases:
            a = normalize_words(alias)
            if w == a or a in w:
                return canonical
    # Strip obvious time fragments for a stable fallback.
    w = re.sub(r"\b\d{1,2}(?:[:.]\d{1,2})?\s*(?:am|pm|a m|p m)?\b", " ", w)
    w = re.sub(r"\s+", " ", w).strip()
    return w.title() if w else None


def activity_display(canonical: str, raw: Any = None) -> str:
    return canonical or (clean_text(raw) or "Historical Activity")


def normalize_venue(value: Any) -> str:
    return normalize_words(value)


def safe_dob(value: Any) -> Optional[date]:
    if isinstance(value, datetime):
        d = value.date()
        return d if date(1900,1,1) <= d <= date.today() else None
    if isinstance(value, date):
        return value if date(1900,1,1) <= value <= date.today() else None
    text = clean_text(value)
    if not text:
        return None
    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%d/%m/%y"):
        try:
            d = datetime.strptime(text, fmt).date()
            if date(1900,1,1) <= d <= date.today():
                return d
        except ValueError:
            pass
    return None


def parse_risk(value: Any) -> Optional[str]:
    w = normalize_words(value)
    if not w or w in {"yes", "no"}:
        return None
    if "high" in w: return "High"
    if "medium" in w or "moderate" in w: return "Medium"
    if "low" in w: return "Low"
    return (clean_text(value) or "")[:100] or None


def bool_from_yes(value: Any) -> Optional[bool]:
    w = normalize_words(value)
    if not w:
        return None
    if w in YES_VALUES: return True
    if w in {"no", "false", "0", "n a", "na"}: return False
    return None

MONTHS = {m.lower(): i for i,m in enumerate(
    ["", "January","February","March","April","May","June","July","August","September","October","November","December"]
) if m}


def sheet_month_year(title: str) -> tuple[Optional[int], Optional[int]]:
    w = normalize_words(title)
    month = None
    for name,num in MONTHS.items():
        if name.lower() in w:
            month = num
            break
    years = re.findall(r"\b(20\d{2}|\d{2})\b", w)
    year = None
    if years:
        y = int(years[-1]); year = 2000+y if y < 100 else y
    # Special old combined sheet title.
    if "oct 22 dec 22" in w:
        year = 2022
    return year, month


def parse_date_with_context(value: Any, year_hint: Optional[int]=None, month_hint: Optional[int]=None) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, datetime):
        d = value.date()
        if year_hint and d.year != year_hint:
            return None
        if month_hint and d.month != month_hint:
            # Excel regional ambiguity: 04/10 imported as 10-Apr; swap day/month if it repairs context.
            if d.day == month_hint and 1 <= d.month <= 31:
                try:
                    cand = date(year_hint or d.year, d.day, d.month)
                    if cand.month == month_hint:
                        return cand
                except ValueError:
                    pass
        return d
    if isinstance(value, date):
        return value
    text = clean_text(value)
    if not text:
        return None
    # Remove ordinal suffixes.
    text2 = re.sub(r"(\d)(st|nd|rd|th)\b", r"\1", text, flags=re.I)
    candidates = []
    for fmt in ("%d/%m/%Y", "%m/%d/%Y", "%d/%m/%y", "%m/%d/%y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            candidates.append(datetime.strptime(text2, fmt).date())
        except ValueError:
            pass
    for d in candidates:
        if year_hint and d.year != year_hint:
            continue
        if month_hint and d.month != month_hint:
            continue
        return d
    if candidates:
        return candidates[0]
    # Ordinal/day only, needs context.
    m = re.search(r"\b(\d{1,2})\b", text2)
    if m and year_hint and month_hint:
        try: return date(year_hint, month_hint, int(m.group(1)))
        except ValueError: return None
    return None


def parse_ordinal_date(day_value: Any, month_value: Any, year: int, fallback_month: Optional[int]=None) -> Optional[date]:
    day_text = clean_text(day_value)
    if not day_text:
        return None
    m = re.search(r"(\d{1,2})", day_text)
    if not m:
        return None
    day_num = int(m.group(1))
    month_num = None
    mw = normalize_words(month_value)
    for n,num in MONTHS.items():
        if n.lower() == mw or n.lower() in mw:
            month_num = num; break
    month_num = month_num or fallback_month
    if not month_num:
        return None
    try: return date(year, month_num, day_num)
    except ValueError: return None


def _parse_clock(piece: str) -> tuple[int,int,Optional[str]]:
    p = piece.strip().lower().replace(".", ":")
    p = p.replace("noon", "12:00pm")
    p = re.sub(r"\s+", "", p)
    mer = None
    if p.endswith("am"): mer="am"; p=p[:-2]
    elif p.endswith("pm"): mer="pm"; p=p[:-2]
    m = re.match(r"^(\d{1,2})(?::(\d{1,2}))?$", p)
    if not m: raise ValueError(piece)
    return int(m.group(1)), int(m.group(2) or 0), mer


def clock_to_time(h:int, minute:int, mer:Optional[str]) -> time:
    if mer == "pm" and h < 12: h += 12
    if mer == "am" and h == 12: h = 0
    if not (0 <= h <= 23 and 0 <= minute <= 59): raise ValueError
    return time(h, minute)


def parse_time_range(value: Any, raw_activity: Any=None) -> Optional[tuple[time,time]]:
    if isinstance(value, datetime):
        return None
    text = clean_text(value)
    if text and not is_cancel_text(text):
        # Excel sometimes stores a time-only value as 1900-xx-xx datetime; ignore as range.
        if re.search(r"\d", text):
            t = text.lower().replace("–", "-").replace("—", "-")
            t = re.sub(r"\s+", "", t)
            # normalize 10:30-11.30 etc
            m = re.search(r"(\d{1,2}(?:[:.]\d{1,2})?\s*(?:am|pm)?)\s*-\s*(\d{1,2}(?:[:.]\d{1,2})?\s*(?:am|pm)?)", t, re.I)
            if m:
                try:
                    h1,m1,mer1=_parse_clock(m.group(1)); h2,m2,mer2=_parse_clock(m.group(2))
                    if mer1 is None and mer2 is not None: mer1=mer2
                    st=clock_to_time(h1,m1,mer1); et=clock_to_time(h2,m2,mer2)
                    if et <= st and st.hour < 12 and et.hour <= 12:
                        # likely implicit PM on end or start/end around noon
                        if et.hour + 12 <= 23: et=time(et.hour+12, et.minute)
                    if et > st: return st,et
                except Exception:
                    pass
    # Extract a single start time from activity title; assume 1 hour.
    a = clean_text(raw_activity) or ""
    m = re.search(r"\b(\d{1,2}(?:[:.]\d{1,2})?)\s*(am|pm)\b", a, re.I)
    if m:
        try:
            h,mi,mer=_parse_clock(m.group(1)+m.group(2)); st=clock_to_time(h,mi,mer)
            dt=datetime.combine(date.today(),st)+timedelta(hours=1)
            return st,dt.time()
        except Exception: pass
    return None

# -----------------------------------------------------------------------------
# SOURCE MODELS
# -----------------------------------------------------------------------------

@dataclass
class PersonSource:
    raw_card: Any = None
    wellbeing_card: Optional[str] = None
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
    signed_induction: Optional[bool] = None

@dataclass
class SourceSession:
    source_file: str
    source_sheet: str
    source_ref: str
    session_date: date
    venue_name: str
    canonical_activity: str
    activity_name: str
    raw_activity: str
    start_time: Optional[time]
    end_time: Optional[time]
    time_quality: str
    people: list[tuple[PersonSource,str]] = field(default_factory=list)
    is_cancelled: bool = False
    cancel_reason: Optional[str] = None
    is_booking_required: bool = False
    lead: Optional[str] = None
    session_type: Optional[str] = None

    @property
    def source_key(self) -> str:
        return f"{self.source_file}|{self.source_sheet}|{self.source_ref}"

@dataclass
class DbSession:
    session_id: int
    session_date: date
    venue_name: str
    activity_name: str
    canonical_activity: Optional[str]
    start_time: time
    end_time: time
    is_cancelled: bool
    attendance_count: int

@dataclass
class DbParticipant:
    participant_id: int
    card: str
    full_name: Optional[str]

@dataclass
class DbLite:
    lite_id: str
    membership_id: str
    first_name: str
    last_name: str
    dob: Optional[date]
    postcode: Optional[str]
    phone: Optional[str]

# -----------------------------------------------------------------------------
# FILE DISCOVERY
# -----------------------------------------------------------------------------

def sha256(path: Path) -> str:
    h=hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda:f.read(1024*1024), b""): h.update(chunk)
    return h.hexdigest()


def version_score(path: Path) -> tuple[int,int,str]:
    nums=[int(x) for x in re.findall(r"\((\d+)\)", path.stem)]
    suffix=max(nums) if nums else 0
    return (suffix, path.stat().st_size, path.name.lower())


def choose_source(patterns: list[str], label: str) -> Optional[Path]:
    matches=[]
    for pat in patterns: matches.extend(BASE_DIR.glob(pat))
    # de-dupe same path
    unique={p.resolve():p for p in matches if p.is_file()}
    matches=list(unique.values())
    if not matches:
        return None
    # If files are identical, choose best-named/latest suffix. If not, choose highest version/size and report.
    groups=defaultdict(list)
    for p in matches: groups[sha256(p)].append(p)
    if len(groups)==1:
        return sorted(matches,key=version_score)[-1]
    chosen=sorted(matches,key=version_score)[-1]
    print(f"  NOTE {label}: multiple differing files found; using {chosen.name}")
    for p in sorted(matches,key=lambda x:x.name.lower()):
        if p != chosen: print(f"       ignored alternative: {p.name}")
    return chosen


def discover_files() -> dict[str,Optional[Path]]:
    return {
        "core_2023": choose_source(["*2023*Register*Exercise*ARCC*.xlsx"], "2023/2022-23 core register"),
        "core_2024": choose_source(["*2024*Register*Exercise*ARCC*.xlsx"], "2024 core register"),
        "core_2025": choose_source(["*2025*Register*Exercise*ARCC*.xlsx"], "2025 core register"),
        "core_2026": choose_source(["ARCC Activity Register*2026*.xlsx"], "2026 activity register"),
        "innerva": choose_source(["Innerva Booking Sheet*.xlsx"], "Innerva booking workbook"),
        "appointments": choose_source(["Appointment Booking Sheet*.xlsx"], "Appointment booking workbook"),
    }

# -----------------------------------------------------------------------------
# CORE WIDE REGISTER PARSER (2022-2025 MONTHLY)
# -----------------------------------------------------------------------------

META_ALIASES = {
    "card": {"sahelicardnumber","sahelicardno","sahelicard"},
    "wellbeing": {"wellbeingcardnumber","wellbeingcardno","wellbeingcard"},
    "name": {"name","fullname"},
    "dob": {"dob","dateofbirth"},
    "postcode": {"postcode"},
    "phone": {"phone","phonenumber","mobilenumber"},
    "emergency_name": {"emergencycontactname","emergencycontact"},
    "emergency_phone": {"emergencynumber","emergencycontactnumber","emergencyphone"},
    "risk": {"riskstratification","riskassessment","riskassesment"},
    "gender": {"gender"},
    "ethnicity": {"ethnicity"},
}


def locate_cols(headers: Iterable[Any]) -> dict[str,int]:
    result={}
    for idx,v in enumerate(headers,start=1):
        n=normalize_header(v)
        for field,aliases in META_ALIASES.items():
            if n in aliases and field not in result: result[field]=idx
    return result


def getv(row: tuple[Any,...], col: Optional[int]) -> Any:
    return row[col-1] if col and col-1 < len(row) else None


def person_from_row(row: tuple[Any,...], cols: dict[str,int]) -> PersonSource:
    return PersonSource(
        raw_card=getv(row,cols.get("card")), wellbeing_card=clean_text(getv(row,cols.get("wellbeing"))),
        full_name=clean_text(getv(row,cols.get("name"))), dob=safe_dob(getv(row,cols.get("dob"))),
        postcode=clean_text(getv(row,cols.get("postcode"))), phone=clean_text(getv(row,cols.get("phone"))),
        emergency_name=clean_text(getv(row,cols.get("emergency_name"))), emergency_phone=clean_text(getv(row,cols.get("emergency_phone"))),
        risk=parse_risk(getv(row,cols.get("risk"))), gender=clean_text(getv(row,cols.get("gender"))),
        ethnicity=clean_text(getv(row,cols.get("ethnicity"))),
    )


def wide_sheet_is_data(ws, header3: tuple[Any, ...]) -> bool:
    if normalize_words(ws.title) in {"sheet1","sheet2","breakdown"}: return False
    return any(normalize_header(v)=="name" for v in header3)


def parse_wide_core(path: Path, start_filter: Optional[date], end_filter: Optional[date]) -> list[SourceSession]:
    wb=load_workbook(path,read_only=True,data_only=True)
    out=[]
    for ws in wb.worksheets:
        year_hint,month_hint=sheet_month_year(ws.title)
        maxc=min(ws.max_column,250)
        hdr_rows=list(ws.iter_rows(min_row=2,max_row=3,max_col=maxc,values_only=True))
        if len(hdr_rows) < 2: continue
        h2=list(hdr_rows[0]); h3=list(hdr_rows[1])
        if not wide_sheet_is_data(ws, tuple(h3)): continue
        cols=locate_cols(h3)
        name_col=cols.get("name")
        if not name_col: continue
        sessions_by_col={}
        # determine real session columns by a usable date in row 2 and non-empty activity in row 3
        for c in range(1,maxc+1):
            raw_act=clean_text(h3[c-1])
            if not raw_act or c <= name_col: continue
            raw_date=h2[c-1]
            d=parse_date_with_context(raw_date,year_hint,month_hint)
            # The combined Oct-Dec 2022 sheet contains Excel datetimes produced by
            # regional dd/mm vs mm/dd conversion (e.g. 04/10 displayed by openpyxl
            # as 10-Apr). Repair only when the swapped candidate lands in Oct-Dec.
            if "oct 22 dec 22" in normalize_words(ws.title) and isinstance(raw_date,(datetime,date)):
                rd=raw_date.date() if isinstance(raw_date,datetime) else raw_date
                if rd.month not in {10,11,12} and rd.day in {10,11,12}:
                    try: d=date(2022,rd.day,rd.month)
                    except ValueError: pass
            if not d: continue
            if start_filter and d < start_filter: continue
            if end_filter and d > end_filter: continue
            can=canonical_activity(raw_act)
            if not can: continue
            tr=parse_time_range(None,raw_act)
            st,et=(tr if tr else (None,None))
            sessions_by_col[c]=SourceSession(path.name,ws.title,f"C{c}",d,ARCC_VENUE,can,activity_display(can,raw_act),raw_act,st,et,"SOURCE" if tr else "UNKNOWN")
        if not sessions_by_col: continue
        max_needed=max(max(sessions_by_col), max(cols.values(),default=1))
        for ridx,row in enumerate(ws.iter_rows(min_row=4,max_col=max_needed,values_only=True),start=4):
            person=person_from_row(row,cols)
            for c,sess in sessions_by_col.items():
                if is_yes(getv(row,c)):
                    # Attendance with neither a card nor usable name is not migratable safely; retain for REVIEW later.
                    sess.people.append((person,f"R{ridx}C{c}"))
        out.extend(sessions_by_col.values())
    return out

# -----------------------------------------------------------------------------
# 2025 ACTIVITY-CONTINUATION SHEETS
# -----------------------------------------------------------------------------

CORE_2025_MONTHLY_PREFIXES={"january","february","march","april","may","june","july","august","september"}


def parse_2025_continuation(path: Path, start_filter: Optional[date], end_filter: Optional[date]) -> list[SourceSession]:
    wb=load_workbook(path,read_only=True,data_only=True)
    out=[]
    for ws in wb.worksheets:
        titlew=normalize_words(ws.title)
        if titlew in {"sheet1","sheet2","breakdown"}: continue
        if any(titlew.startswith(m) for m in CORE_2025_MONTHLY_PREFIXES): continue
        # continuation layout: row 2 metadata in cols1-6, dates from col7 onward
        maxc=min(ws.max_column,250)
        row2=list(next(ws.iter_rows(min_row=2,max_row=2,max_col=maxc,values_only=True), ()))
        if not any(normalize_header(v)=="sahelicardnumber" for v in row2[:8]): continue
        can=canonical_activity(ws.title)
        if not can: continue
        # The first dated header in these continuation sheets often contains the
        # activity time (for example "9/9/2025 Chair Exercise 12.30pm") while
        # later headers contain only a date. Learn that time once for the sheet.
        sheet_default_tr=None
        for _v in row2[6:]:
            if isinstance(_v,str):
                _tr=parse_time_range(None,_v)
                if _tr:
                    sheet_default_tr=_tr
                    break
        sessions=[]
        for c in range(7,maxc+1):
            raw=row2[c-1]
            if raw is None: continue
            d=None; tr=None
            if isinstance(raw,(datetime,date)):
                d=raw.date() if isinstance(raw,datetime) else raw
            else:
                text=clean_text(raw)
                if text:
                    m=re.search(r"(\d{1,2}/\d{1,2}/20\d{2})",text)
                    if m: d=parse_date_with_context(m.group(1),2025,None)
                    tr=parse_time_range(None,text)
            if not d: continue
            if d.year != 2025: continue
            if start_filter and d < start_filter: continue
            if end_filter and d > end_filter: continue
            if not tr: tr=sheet_default_tr or parse_time_range(None,ws.title)
            st,et=tr if tr else (None,None)
            sessions.append((c,SourceSession(path.name,ws.title,f"C{c}",d,ARCC_VENUE,can,activity_display(can,ws.title),clean_text(ws.title) or can,st,et,"SOURCE" if tr else "UNKNOWN")))
        if not sessions: continue
        max_needed=max(c for c,_ in sessions)
        # Metadata: col2 card, col3 name, col4 emergency name, col5 emergency phone, col6 risk.
        # Some copied continuation sheets contain a PivotTable/helper block below the real
        # participant register (for example a "Row Labels / Count of ..." summary).
        # Those helper counts can contain numeric 1 values in session-date columns and must
        # never be interpreted as anonymous attendance. Once that helper block starts, stop
        # reading participant rows for this sheet.
        helper_section=False
        for ridx,row in enumerate(ws.iter_rows(min_row=3,max_col=max_needed,values_only=True),start=3):
            row_words=[normalize_words(v) for v in row if clean_text(v)]
            if any(v == "row labels" or v == "grand total" or v.startswith("count of ") for v in row_words):
                helper_section=True
            if helper_section:
                continue

            raw_card=getv(row,2)
            full_name=clean_text(getv(row,3))
            person=PersonSource(raw_card=raw_card, full_name=full_name, emergency_name=clean_text(getv(row,4)), emergency_phone=clean_text(getv(row,5)), risk=parse_risk(getv(row,6)))
            for c,sess in sessions:
                if is_yes(getv(row,c)):
                    sess.people.append((person,f"R{ridx}C{c}"))
        out.extend(s for _,s in sessions)
    return out

# -----------------------------------------------------------------------------
# 2026 VERTICAL ACTIVITY REGISTER
# -----------------------------------------------------------------------------

def parse_vertical_2026(path: Path, start_filter: Optional[date], end_filter: Optional[date]) -> list[SourceSession]:
    wb=load_workbook(path,read_only=True,data_only=True)
    out=[]
    skip={"sheet1","template"}
    for ws in wb.worksheets:
        if normalize_words(ws.title) in skip: continue
        if "omnia" in normalize_words(ws.title):
            # Omnia is a separate delivery location and is intentionally excluded
            # from the ARCC migration. It can be migrated separately later.
            continue
        venue=ARCC_VENUE
        current=None
        for ridx,row in enumerate(ws.iter_rows(min_row=2,max_col=min(ws.max_column,18),values_only=True),start=2):
            raw_activity=clean_text(getv(row,2))
            if raw_activity:
                d=parse_date_with_context(getv(row,4),2026,None)
                can=canonical_activity(raw_activity or ws.title)
                tr=parse_time_range(getv(row,6),raw_activity)
                if d and can and (not start_filter or d>=start_filter) and (not end_filter or d<=end_filter):
                    st,et=tr if tr else (None,None)
                    current=SourceSession(path.name,ws.title,f"R{ridx}",d,venue,can,activity_display(can,raw_activity),raw_activity,st,et,"SOURCE" if tr else "UNKNOWN")
                    out.append(current)
                else:
                    current=None
            if current is None: continue
            raw_card=getv(row,7); name=clean_text(getv(row,8))
            # Ignore pivot/helper rows and empty participant rows.
            if raw_card is None and not name: continue
            if normalize_words(name) in NON_PERSON_MARKERS: continue
            person=PersonSource(raw_card=raw_card, full_name=name, emergency_name=clean_text(getv(row,9)), emergency_phone=clean_text(getv(row,10)), risk=parse_risk(getv(row,11)), health_conditions=clean_text(getv(row,12)))
            # A row with a usable card or name is treated as an attendee in this activity register.
            current.people.append((person,f"R{ridx}"))
    return out

# -----------------------------------------------------------------------------
# INNERVA PARSER
# -----------------------------------------------------------------------------

INNERVA_MONTHLY_SHEETS = {
    "July 24": (2024,7), "August 24": (2024,8), "September 24": (2024,9), "October 24": (2024,10),
    "November 24": (2024,11), "December 24": (2024,12), "January 25": (2025,1), "February 25": (2025,2),
    "March 25": (2025,3), "April 25": (2025,4), "May 25": (2025,5), "June 25": (2025,6),
    "July 25": (2025,7), "August 25": (2025,8),
}


def find_header_map(row: tuple[Any,...]) -> dict[str,int]:
    mapping={}
    for i,v in enumerate(row,start=1):
        n=normalize_header(v)
        if n and n not in mapping: mapping[n]=i
    return mapping


def hcol(h:dict[str,int], *names:str) -> Optional[int]:
    for name in names:
        n=normalize_header(name)
        if n in h: return h[n]
    return None


def parse_innerva(path: Path, start_filter: Optional[date], end_filter: Optional[date]) -> list[SourceSession]:
    wb=load_workbook(path,read_only=True,data_only=True)
    out=[]
    wanted=[]
    normalized_sheet_names={normalize_words(sn):sn for sn in wb.sheetnames}
    for title,(y,m) in INNERVA_MONTHLY_SHEETS.items():
        actual=normalized_sheet_names.get(normalize_words(title))
        if actual: wanted.append((actual,y,m))
    actual=normalized_sheet_names.get("current")
    if actual: wanted.append((actual,2025,None))
    actual=normalized_sheet_names.get("2026")
    if actual: wanted.append((actual,2026,None))

    for sheet,y,fixed_month in wanted:
        ws=wb[sheet]
        header=next(ws.iter_rows(min_row=1,max_row=1,max_col=25,values_only=True))
        hm=find_header_map(header)
        c_spaces=hcol(hm,"Spaces") or 1
        c_lead=hcol(hm,"Lead")
        c_type=hcol(hm,"Session Type")
        c_day=hcol(hm,"Day")
        c_date=hcol(hm,"Session Date","Date","date","Session")
        # Some old sheets label the date column itself as "Session" and the time-range column also as Session.
        c_month=hcol(hm,"Month")
        c_induction=hcol(hm,"Induction Time")
        # Find all "session" header columns, choose the last one as time slot.
        session_cols=[i for i,v in enumerate(header,start=1) if normalize_header(v)=="session"]
        c_slot=session_cols[-1] if session_cols else hcol(hm,"Session")
        c_card=hcol(hm,"Saheli Card Number")
        c_name=hcol(hm,"Name")
        c_risk=hcol(hm,"Risk Stratification")
        c_att=hcol(hm,"Attended")
        c_signed=hcol(hm,"Signed Induction Paper")
        c_med=hcol(hm,"Medical condition affecting use of machine")
        c_dob=hcol(hm,"Date of Birth","Date of birth")

        current=None
        current_date=None
        max_needed=max([x for x in [c_spaces,c_lead,c_type,c_day,c_date,c_month,c_induction,c_slot,c_card,c_name,c_risk,c_att,c_signed,c_med,c_dob] if x] or [15])
        for ridx,row in enumerate(ws.iter_rows(min_row=2,max_col=max_needed,values_only=True),start=2):
            spaces=getv(row,c_spaces)
            try: is_block_start = int(float(spaces)) == 1
            except Exception: is_block_start = False
            if is_block_start:
                month_num=fixed_month
                if not month_num:
                    mw=normalize_words(getv(row,c_month))
                    for n,num in MONTHS.items():
                        if n.lower()==mw or (mw and n.lower() in mw): month_num=num; break
                # Old sheets: date token may be in a column named Session.  c_date can collide with c_slot,
                # so if c_date == c_slot, use col4/5 shape according to known format.
                date_value=getv(row,c_date)
                if c_date == c_slot:
                    # old 2024 has Day col3, date col4, induction col5, slot col6
                    date_value=getv(row,4)
                d=parse_ordinal_date(date_value,getv(row,c_month),y,month_num)
                if not d and isinstance(date_value,(date,datetime)):
                    d=parse_date_with_context(date_value,y,month_num)
                # For monthly sheets without Month column, use fixed sheet month.
                if not d and fixed_month:
                    d=parse_ordinal_date(date_value,None,y,fixed_month)
                slot_value=getv(row,c_slot)
                tr=parse_time_range(slot_value,"Innerva")
                induction=getv(row,c_induction)
                cancelled=is_cancel_text(induction) or is_cancel_text(slot_value) or is_cancel_text(getv(row,c_name))
                reason=clean_text(induction) if is_cancel_text(induction) else (clean_text(slot_value) if is_cancel_text(slot_value) else None)
                if d and tr and (not start_filter or d>=start_filter) and (not end_filter or d<=end_filter):
                    st,et=tr
                    stype=clean_text(getv(row,c_type))
                    lead=clean_text(getv(row,c_lead))
                    current=SourceSession(path.name,sheet,f"R{ridx}",d,ARCC_VENUE,"Innerva","Innerva","Innerva",st,et,"SOURCE",is_cancelled=cancelled,cancel_reason=reason,is_booking_required=True,lead=lead,session_type=stype)
                    out.append(current); current_date=d
                else:
                    current=None; current_date=d
            if current is None or current.is_cancelled: continue
            # Only explicit Yes counts as Innerva attendance.
            if not c_att or not is_yes(getv(row,c_att)): continue
            raw_card=getv(row,c_card); name=clean_text(getv(row,c_name))
            if not raw_card and not name: continue
            if normalize_words(name) in NON_PERSON_MARKERS: continue
            person=PersonSource(raw_card=raw_card,full_name=name,risk=parse_risk(getv(row,c_risk)),dob=safe_dob(getv(row,c_dob)),health_conditions=clean_text(getv(row,c_med)),signed_induction=bool_from_yes(getv(row,c_signed)))
            current.people.append((person,f"R{ridx}"))
    return out

# -----------------------------------------------------------------------------
# SOURCE CLEANUP / TIME RESOLUTION
# -----------------------------------------------------------------------------

def person_prekey(person: PersonSource) -> str:
    cards=card_candidates(person.raw_card)
    if cards: return "CARD:"+"/".join(sorted(normalize_card_key(c) for c in cards))
    return "NAME:"+normalize_full_name_key(person.full_name)


def dedupe_sources(sessions: list[SourceSession]) -> tuple[int,int,int]:
    """Merge duplicate source sessions and remove duplicate member/session marks."""
    merged={}
    merged_sessions=0
    for s in sessions:
        key=(normalize_venue(s.venue_name),s.session_date,s.canonical_activity,s.start_time,s.end_time,s.is_cancelled)
        if key not in merged:
            merged[key]=s
        else:
            target=merged[key]; target.people.extend(s.people)
            if not target.lead: target.lead=s.lead
            if not target.session_type: target.session_type=s.session_type
            target.source_ref += f"+{s.source_sheet}:{s.source_ref}"
            merged_sessions+=1
    removed=0
    for s in merged.values():
        seen=set(); kept=[]
        for p,ref in s.people:
            pk=person_prekey(p)
            if not pk or pk in {"NAME:","CARD:"}:
                kept.append((p,ref)); continue
            if pk in seen:
                removed+=1; continue
            seen.add(pk); kept.append((p,ref))
        s.people=kept
    return list(merged.values()),merged_sessions,removed


def assign_placeholder_times(sessions: list[SourceSession]) -> int:
    """Only for non-Innerva core sessions with missing source time."""
    missing=[s for s in sessions if not s.start_time or not s.end_time]
    if not missing: return 0
    # deterministic by date + activity, keep different activities separate through staggered slots
    by_day=defaultdict(list)
    for s in missing: by_day[(s.venue_name,s.session_date)].append(s)
    count=0
    for _,lst in by_day.items():
        lst.sort(key=lambda s:(s.canonical_activity,s.source_key))
        for idx,s in enumerate(lst):
            mins=PLACEHOLDER_BASE_HOUR*60 + idx*(PLACEHOLDER_DURATION_MINUTES+PLACEHOLDER_GAP_MINUTES)
            h=(mins//60)%24; m=mins%60
            st=time(h,m); end_dt=datetime.combine(date.today(),st)+timedelta(minutes=PLACEHOLDER_DURATION_MINUTES)
            s.start_time=st; s.end_time=end_dt.time(); s.time_quality="PLACEHOLDER"; count+=1
    return count


def parse_all_sources(files: dict[str,Optional[Path]], start_filter: Optional[date], end_filter: Optional[date]) -> tuple[list[SourceSession],dict[str,int]]:
    raw=[]; stats=Counter()
    for key in ("core_2023","core_2024","core_2025"):
        p=files.get(key)
        if p:
            x=parse_wide_core(p,start_filter,end_filter); raw.extend(x); stats[f"{key}_sessions"]+=len(x)
    p=files.get("core_2025")
    if p:
        x=parse_2025_continuation(p,start_filter,end_filter); raw.extend(x); stats["core_2025_continuation_sessions"]+=len(x)
    p=files.get("core_2026")
    if p:
        x=parse_vertical_2026(p,start_filter,end_filter); raw.extend(x); stats["core_2026_sessions"]+=len(x)
    p=files.get("innerva")
    if p:
        x=parse_innerva(p,start_filter,end_filter); raw.extend(x); stats["innerva_sessions"]+=len(x)
    # remove out-of-range and impossible dates one last time
    raw=[s for s in raw if (not start_filter or s.session_date>=start_filter) and (not end_filter or s.session_date<=end_filter)]
    merged,merged_count,removed_att=dedupe_sources(raw)
    stats["source_session_duplicates_merged"]=merged_count
    stats["source_attendance_duplicates_removed"]=removed_att
    stats["placeholder_times_assigned"]=assign_placeholder_times(merged) if ALLOW_PLACEHOLDER_TIMES else 0
    merged.sort(key=lambda s:(s.session_date,s.start_time or time(23,59),s.venue_name,s.canonical_activity,s.source_key))
    return merged,dict(stats)


# =============================================================================
# MEN'S PROGRAMME SOURCE OVERRIDES (V1)
# =============================================================================
# The ARCC engine below is reused for its tested SQL/participant/session safety
# logic. These definitions replace ARCC source discovery/parsing for the men's
# historical workbooks.

MEN_ACTIVITY_ALIASES = {
    "mens know your number": "Men's Know Your Number",
    "men s know your number": "Men's Know Your Number",
    "mens know your health numbers": "Men's Know Your Number",
    "mens know your health number": "Men's Know Your Number",
    "know your numbers": "Men's Know Your Number",
    "know your number": "Men's Know Your Number",
    "mens multisports": "Men's Multisports",
    "mens multi sports": "Men's Multisports",
    "men s multisports": "Men's Multisports",
    "mens exercise": "Men's Exercise",
    "men s exercise": "Men's Exercise",
    "mens exercise class": "Men's Exercise",
    "mens circuit exercise": "Men's Exercise",
    "men s circuit exercise": "Men's Exercise",
    "mens activity": "Men's Exercise",
    "mens lunch club": "Men's Lunch Club",
    "men s lunch club": "Men's Lunch Club",
    "lunch club": "Men's Lunch Club",
    "mens walk and talk": "Men's Walk & Talk",
    "mens walk talk": "Men's Walk & Talk",
    "men s walk and talk": "Men's Walk & Talk",
    "mens walk": "Men's Walk & Talk",
    "digital skills": "Digital Skills",
}


def canonical_activity(value: Any) -> str:
    n = normalize_words(value)
    if n in MEN_ACTIVITY_ALIASES:
        return MEN_ACTIVITY_ALIASES[n]
    # tolerant contains rules for existing CRM wording
    if "know your" in n and ("number" in n or "health" in n):
        return "Men's Know Your Number"
    if "multi" in n and "sport" in n and "men" in n:
        return "Men's Multisports"
    if "lunch" in n and "club" in n:
        return "Men's Lunch Club"
    if "walk" in n and "men" in n:
        return "Men's Walk & Talk"
    if "digital" in n and "skill" in n:
        return "Digital Skills"
    if "men" in n and ("exercise" in n or "circuit" in n or "activity" in n):
        return "Men's Exercise"
    return (clean_text(value) or "Historical Activity").strip()


def normalize_venue(value: Any) -> str:
    n = normalize_words(value)
    if n in {"arcc", "alum rock", "alum rock community centre"}:
        return "alum rock community centre"
    if n in {"calthorpe", "calthorpe wellbeing hub", "calthorpe centre", "calthorpe men"}:
        return "calthorpe wellbeing hub"
    if n in {"ward end park", "wardend", "ward end"}:
        return "ward end park"
    if n in {"bahu trust", "bahu trus", "sultan bahu", "bahu"}:
        return "bahu trust"
    if n in {"sparkbrook cc", "sparkbrook community centre", "sparkbrook c0mmunity centre", "sparkbrook c0mmunity centre cc"}:
        return "sparkbrook community centre"
    if n in {"strensham rd", "strensham road"}:
        return "strensham rd"
    if n in {"parkfield centre", "parkfield", "parkfield community", "parkfield community school"}:
        return "parkfield"
    if n in {"saltley centre", "saltley community centre"}:
        return "saltley centre"
    return n


def choose_source_excluding(patterns: list[str], label: str, exclude_words: tuple[str, ...]=()) -> Optional[Path]:
    matches=[]
    for pat in patterns:
        matches.extend(BASE_DIR.glob(pat))
    matches=[p for p in matches if p.is_file() and not any(w.lower() in p.name.lower() for w in exclude_words)]
    unique={p.resolve():p for p in matches}
    matches=list(unique.values())
    if not matches:
        return None
    groups=defaultdict(list)
    for p in matches:
        groups[sha256(p)].append(p)
    chosen=sorted(matches,key=version_score)[-1]
    if len(groups)>1:
        print(f"  NOTE {label}: multiple differing files found; using {chosen.name}")
        for p in sorted(matches,key=lambda x:x.name.lower()):
            if p != chosen:
                print(f"       ignored alternative: {p.name}")
    return chosen


def discover_files() -> dict[str,Optional[Path]]:
    # The non-Zaheer KYN workbook is an older overlapping snapshot. It is
    # deliberately discovered for audit visibility but never parsed.
    return {
        "kyn_snapshot": choose_source_excluding(
            ["Men's sessions (FFF Know your numbers)*.xlsx", "Mens sessions (FFF Know your numbers)*.xlsx"],
            "older KYN snapshot", ("zaheer",)
        ),
        "kyn_zaheer": choose_source(
            ["*Know your numbers*Zaheer*.xlsx", "*Know Your Numbers*Zaheer*.xlsx"],
            "latest Zaheer Know Your Numbers register"
        ),
        "mens_2026": choose_source(["Mens Sessions 2026*.xlsx", "Men's Sessions 2026*.xlsx"], "Men's Sessions 2026 workbook"),
    }


def valid_saheli_card(value: Any) -> Optional[str]:
    text=clean_text(value)
    if not text:
        return None
    text=text.upper().strip()
    text=re.sub(r"^SAH(?:ELI)?[-\s]*", "", text)
    if re.fullmatch(r"\d+(?:\.0+)?", text):
        card=str(int(float(text)))
        if card != "0" and 1 <= len(card) <= 6:
            return card
    return None


def mens_source_date(value: Any, year_hint: Optional[int]=None) -> Optional[date]:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, (int,float)) and not isinstance(value,bool):
        x=float(value)
        if 30000 <= x <= 60000:
            return (datetime(1899,12,30)+timedelta(days=x)).date()
    text=clean_text(value)
    if not text:
        return None
    text=text.strip()
    # Known source typo in the latest Zaheer continuation sheet.
    if re.fullmatch(r"19[/.-]05[/.-]206", text):
        return date(2026,5,19)
    for fmt in ("%d/%m/%Y","%d-%m-%Y","%d.%m.%Y","%d/%m/%y","%Y-%m-%d"):
        try:
            return datetime.strptime(text,fmt).date()
        except ValueError:
            pass
    m=re.fullmatch(r"(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]+)", text, flags=re.I)
    if m and year_hint:
        month_names={
            "jan":1,"january":1,"feb":2,"february":2,"mar":3,"march":3,"apr":4,"april":4,
            "may":5,"jun":6,"june":6,"jul":7,"july":7,"aug":8,"august":8,"sep":9,"sept":9,"september":9,
            "oct":10,"october":10,"nov":11,"november":11,"dec":12,"december":12,
        }
        mo=month_names.get(m.group(2).lower())
        if mo:
            try:
                return date(year_hint,mo,int(m.group(1)))
            except ValueError:
                return None
    return None


def kyn_venue(value: Any) -> Optional[str]:
    n=normalize_words(value)
    if not n:
        return None
    if n in {"sultan bahu","bahu trust","bahu trus"}:
        return "Bahu Trust"
    if n == "calthorpe men":
        return "Calthorpe Wellbeing Hub"
    if n in {"strensham rd","strensham road"}:
        return "Strensham Rd"
    if n.startswith("sparkbrook"):
        return "Sparkbrook Community Centre"
    if n == "parkfield centre":
        return "Parkfield Centre"
    if n == "saltley centre":
        return "Saltley Centre"
    return clean_text(value)


def make_kyn_person(row: tuple[Any, ...]) -> PersonSource:
    """Build a KYN person from an already-read worksheet row.

    Important: KYN workbooks are opened with read_only=True, so repeated
    ws.cell(row, col) random access is intentionally avoided.
    """
    return PersonSource(
        raw_card=None,
        full_name=clean_text(getv(row,6)),
        dob=safe_dob(getv(row,7)),
        postcode=clean_text(getv(row,8)),
        phone=clean_text(getv(row,9)),
        emergency_name=clean_text(getv(row,10)),
        emergency_phone=clean_text(getv(row,11)),
        gender=clean_text(getv(row,2)),
        ethnicity=clean_text(getv(row,12)),
        health_conditions=clean_text(getv(row,13)),
    )


def parse_kyn_main(path: Path, start_filter: Optional[date], end_filter: Optional[date]) -> list[SourceSession]:
    wb=load_workbook(path,data_only=True,read_only=True)
    try:
        target=None
        for name in wb.sheetnames:
            if "june 2025" in normalize_words(name) and "march 2026" in normalize_words(name):
                target=name; break
        if not target:
            raise RuntimeError(f"Could not find 'June 2025 - March 2026' sheet in {path.name}")
        ws=wb[target]
        sessions: dict[tuple[date,str],SourceSession]={}

        # Read the date header once, then stream participant rows once.
        # This avoids thousands of random ws.cell() lookups on a read-only sheet.
        header=next(ws.iter_rows(min_row=2,max_row=2,max_col=ws.max_column,values_only=True), ())
        date_columns: list[tuple[int,date]]=[]
        for col in range(24,len(header)+1):
            dt=mens_source_date(getv(header,col),2025)
            if not dt:
                continue
            if start_filter and dt < start_filter: continue
            if end_filter and dt > end_filter: continue
            date_columns.append((col,dt))

        if not date_columns:
            return []

        max_needed=max(max(col for col,_ in date_columns),13)
        for row_num,row in enumerate(ws.iter_rows(min_row=4,max_col=max_needed,values_only=True),start=4):
            name=clean_text(getv(row,6))
            venue=kyn_venue(getv(row,5))
            if not name or not venue:
                continue

            person=None
            venue_key=normalize_venue(venue)
            for col,dt in date_columns:
                mark=clean_text(getv(row,col))
                if not mark:
                    continue
                key=(dt,venue_key)
                s=sessions.get(key)
                if not s:
                    if venue_key=="bahu trust":
                        st,et,tq=time(13,30),time(14,30),"INFERRED"
                    else:
                        st=et=None; tq="MISSING"
                    s=SourceSession(
                        source_file=path.name,source_sheet=target,source_ref=f"COL-{get_column_letter(col)}",
                        session_date=dt,venue_name=venue,canonical_activity="Men's Know Your Number",
                        activity_name="Men's Know Your Number",raw_activity="Know Your Numbers",
                        start_time=st,end_time=et,time_quality=tq,is_booking_required=False,
                    )
                    sessions[key]=s
                if person is None:
                    person=make_kyn_person(row)
                s.people.append((person,f"{get_column_letter(col)}{row_num}"))
        return list(sessions.values())
    finally:
        wb.close()


def parse_kyn_continuation(path: Path, start_filter: Optional[date], end_filter: Optional[date]) -> tuple[list[SourceSession],int]:
    wb=load_workbook(path,data_only=True,read_only=True)
    try:
        target=None
        for name in wb.sheetnames:
            if "from april 2026" in normalize_words(name):
                target=name; break
        if not target:
            return [],0
        ws=wb[target]; sessions={}; corrected=0
        for row_num,row in enumerate(ws.iter_rows(min_row=2,max_col=13,values_only=True),start=2):
            raw_date=getv(row,4)
            raw_date_text=clean_text(raw_date)
            if raw_date_text and re.fullmatch(r"19[/.-]05[/.-]206",raw_date_text):
                corrected+=1
            dt=mens_source_date(raw_date)
            name=clean_text(getv(row,6))
            if not dt or not name:
                continue
            if start_filter and dt < start_filter: continue
            if end_filter and dt > end_filter: continue
            s=sessions.get(dt)
            if not s:
                s=SourceSession(
                    source_file=path.name,source_sheet=target,source_ref=f"DATE-{dt.isoformat()}",
                    session_date=dt,venue_name="Bahu Trust",canonical_activity="Men's Know Your Number",
                    activity_name="Men's Know Your Number",raw_activity="Know Your Numbers",
                    start_time=time(13,30),end_time=time(14,30),time_quality="INFERRED",
                ); sessions[dt]=s
            p=PersonSource(
                raw_card=None,full_name=name,dob=safe_dob(getv(row,7)),postcode=clean_text(getv(row,8)),
                phone=clean_text(getv(row,9)),emergency_name=clean_text(getv(row,10)),
                emergency_phone=clean_text(getv(row,11)),gender=clean_text(getv(row,2)),
                ethnicity=clean_text(getv(row,12)),health_conditions=clean_text(getv(row,13)),
            )
            s.people.append((p,f"ROW-{row_num}"))
        return list(sessions.values()),corrected
    finally:
        wb.close()


MENS_2026_SHEET_MAP = {
    "men s multisport calthorpe": ("Calthorpe Wellbeing Hub","Men's Multisports"),
    "mens multisport calthorpe": ("Calthorpe Wellbeing Hub","Men's Multisports"),
    "lunch club": ("Alum Rock Community Centre","Men's Lunch Club"),
    "men s exercise arcc": ("Alum Rock Community Centre","Men's Exercise"),
    "mens exercise arcc": ("Alum Rock Community Centre","Men's Exercise"),
    "digital skills": ("Alum Rock Community Centre","Digital Skills"),
    "mens walk": ("Ward End Park","Men's Walk & Talk"),
    "men s walk": ("Ward End Park","Men's Walk & Talk"),
}


def parse_mens_2026_time(raw: Any, activity: str) -> tuple[time,time,str]:
    # Source workbook supplies either a time range, a start-only label, or an
    # Excel time fraction. Use timetable-supported duration where only start is supplied.
    if activity == "Men's Multisports":
        parsed=parse_time_range(raw)
        if parsed: return parsed[0],parsed[1],"SOURCE"
        return time(10,30),time(11,15),"INFERRED"
    if activity == "Men's Exercise":
        return time(9,15),time(10,30),"INFERRED"
    if activity == "Men's Lunch Club":
        return time(12,0),time(13,30),"INFERRED"
    if activity == "Digital Skills":
        return time(10,0),time(11,0),"INFERRED"
    if activity == "Men's Walk & Talk":
        return time(9,30),time(10,30),"INFERRED"
    parsed=parse_time_range(raw)
    if parsed: return parsed[0],parsed[1],"SOURCE"
    raise ValueError(f"Could not resolve men's session time for activity={activity!r}, raw={raw!r}")


def parse_mens_2026(path: Path, start_filter: Optional[date], end_filter: Optional[date]) -> tuple[list[SourceSession],dict[str,int]]:
    wb=load_workbook(path,data_only=True,read_only=True)
    try:
        sessions=[]; stats=Counter()
        for sheet_name in wb.sheetnames:
            key=normalize_words(sheet_name)
            mapping=MENS_2026_SHEET_MAP.get(key)
            if not mapping:
                if key in {"men s circuit arcc","mens circuit arcc","over 50 s health club arcc","template"}:
                    stats["blank_or_template_sheets_skipped"]+=1
                continue
            venue,activity=mapping; ws=wb[sheet_name]
            grouped={}; carry={2:None,3:None,4:None,5:None}

            # Stream each worksheet once. B-E can be blank on continuation rows,
            # so carry forward the latest non-empty session metadata exactly as before.
            for row_num,row in enumerate(ws.iter_rows(min_row=2,max_col=10,values_only=True),start=2):
                for col in (2,3,4,5):
                    val=getv(row,col)
                    if clean_text(val): carry[col]=val
                dt=mens_source_date(carry[3])
                raw_card_value=getv(row,6)
                card=valid_saheli_card(raw_card_value)
                name=clean_text(getv(row,7))
                if normalize_words(name) in {"n a","na"}: name=None
                raw_card_text=clean_text(raw_card_value)
                if normalize_words(raw_card_text)=="crm" and not name:
                    stats["non_person_helper_rows_skipped"]+=1
                    continue
                if not dt or (not card and not name):
                    continue
                if start_filter and dt < start_filter: continue
                if end_filter and dt > end_filter: continue
                st,et,tq=parse_mens_2026_time(carry[5],activity)
                sk=(dt,st,et)
                s=grouped.get(sk)
                if not s:
                    s=SourceSession(
                        source_file=path.name,source_sheet=sheet_name,source_ref=f"DATE-{dt.isoformat()}-{st.strftime('%H%M')}",
                        session_date=dt,venue_name=venue,canonical_activity=activity,activity_name=activity,
                        raw_activity=sheet_name,start_time=st,end_time=et,time_quality=tq,
                    ); grouped[sk]=s
                p=PersonSource(
                    raw_card=card,full_name=name,emergency_name=clean_text(getv(row,8)),
                    emergency_phone=clean_text(getv(row,9)),risk=parse_risk(getv(row,10)),
                )
                # Preserve a non-Saheli source ID such as L-14 only in the notes field.
                if not card and raw_card_text and normalize_words(raw_card_text) not in {"0","crm","n a","na"}:
                    p.wellbeing_card=raw_card_text
                    stats["non_saheli_source_ids_reclassified_to_lite"]+=1
                s.people.append((p,f"ROW-{row_num}"))
            sessions.extend(grouped.values())
        return sessions,dict(stats)
    finally:
        wb.close()


def parse_all_sources(files: dict[str,Optional[Path]], start_filter: Optional[date], end_filter: Optional[date]) -> tuple[list[SourceSession],dict[str,int]]:
    raw=[]; stats=Counter()
    p=files.get("kyn_zaheer")
    if p:
        x=parse_kyn_main(p,start_filter,end_filter); raw.extend(x); stats["kyn_main_sessions"]+=len(x)
        y,corrected=parse_kyn_continuation(p,start_filter,end_filter); raw.extend(y); stats["kyn_continuation_sessions"]+=len(y); stats["source_date_typos_corrected"]+=corrected
    p=files.get("mens_2026")
    if p:
        x,s2=parse_mens_2026(p,start_filter,end_filter); raw.extend(x); stats["mens_2026_sessions"]+=len(x); stats.update(s2)
    raw=[s for s in raw if (not start_filter or s.session_date>=start_filter) and (not end_filter or s.session_date<=end_filter)]
    merged,merged_count,removed_att=dedupe_sources(raw)
    stats["source_session_duplicates_merged"]=merged_count
    stats["source_attendance_duplicates_removed"]=removed_att
    stats["placeholder_times_assigned"]=assign_placeholder_times(merged) if ALLOW_PLACEHOLDER_TIMES else 0
    merged.sort(key=lambda s:(s.session_date,s.start_time or time(23,59),s.venue_name,s.canonical_activity,s.source_key))
    return merged,dict(stats)


def print_source_audit(sessions:list[SourceSession],stats:dict[str,int],files:dict[str,Optional[Path]],start_filter,end_filter):
    total=sum(len(s.people) for s in sessions)
    print("\n=== MEN'S SOURCE AUDIT ===")
    print(f"Parsed source sessions        : {len(sessions):,}")
    print(f"Parsed attendance rows        : {total:,}")
    print(f"First source date             : {min((s.session_date for s in sessions),default=None)}")
    print(f"Last source date              : {max((s.session_date for s in sessions),default=None)}")
    print(f"Placeholder-time sessions     : {sum(1 for s in sessions if s.time_quality=='PLACEHOLDER'):,}")
    print(f"Unresolved-time sessions      : {sum(1 for s in sessions if not s.start_time or not s.end_time):,}")
    print(f"Source session duplicates merged: {stats.get('source_session_duplicates_merged',0):,}")
    print(f"Source attendance duplicates removed: {stats.get('source_attendance_duplicates_removed',0):,}")
    print(f"Source date typos corrected   : {stats.get('source_date_typos_corrected',0):,}")
    print(f"Non-person/helper rows skipped: {stats.get('non_person_helper_rows_skipped',0):,}")
    print(f"Non-Saheli IDs -> Lite        : {stats.get('non_saheli_source_ids_reclassified_to_lite',0):,}")
    print("\nAttendance by year/month:")
    bym=Counter((s.session_date.year,s.session_date.month) for s in sessions for _ in s.people)
    for (y,m),n in sorted(bym.items()): print(f"  {y}-{m:02d}: {n:,}")
    print("\nAttendance by activity:")
    bya=Counter(s.canonical_activity for s in sessions for _ in s.people)
    for k,v in bya.most_common(): print(f"  {k:28s} {v:,}")
    print("\nAttendance by venue:")
    byv=Counter(s.venue_name for s in sessions for _ in s.people)
    for k,v in byv.most_common(): print(f"  {k:32s} {v:,}")
    print("\nSource files selected:")
    for k,p in files.items(): print(f"  {k:14s}: {p.name if p else 'NOT FOUND'}")
    if files.get("kyn_snapshot"):
        print("\nOlder non-Zaheer Know Your Numbers workbook detected and EXCLUDED as an overlapping snapshot:")
        print(f"  {files['kyn_snapshot'].name}")

# -----------------------------------------------------------------------------
# DATABASE HELPERS
# -----------------------------------------------------------------------------

REQUIRED_COLUMNS={
    "Participants":{"ParticipantID","SaheliCardNumber","FullName","DateOfBirth","Postcode","MobileNumber","Site","Notes","CreatedAt"},
    "LiteMembers":{"Id","MembershipId","FirstName","LastName","DateOfBirth","Phone","Email","Address","Postcode","EmergencyName","EmergencyPhone","EmergencyRelation","HealthConditions","Gender","Ethnicity","CreatedAtUtc","CreatedByUserId"},
    "Sessions":{"SessionId","Frequency","Category","ActivityCategory","VenueName","ActivityName","Notes","IsRecurringWeekly","DayOfWeek","SessionDate","StartTime","EndTime","IsBookingRequired","IsCancelled","CreatedAtUtc"},
    "SessionAttendance":{"AttendanceId","SessionId","AttendanceMemberKind","ParticipantId","LiteMemberId","MemberDisplayId","SaheliCardNumber","MemberName","Phone","EmergencyName","EmergencyPhone","SessionName","SessionDay","SessionDate","SessionMonth","SessionStartTime","SessionEndTime","RiskStratification","Attended","Notes","SignedInductionPaper","MedicalCondition","CreatedAtUtc","UpdatedAtUtc"},
}


def validate_connection_string():
    if not CONNECTION_STRING:
        raise RuntimeError(
            "Database connection not configured. Set the SAHELI_SQL_CONNECTION_STRING environment variable "
            "before running database preview or --commit. Credentials are intentionally not stored in this script."
        )


def preflight_schema(cur):
    for table,expected in REQUIRED_COLUMNS.items():
        actual={r[0] for r in cur.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME=?",table).fetchall()}
        missing=sorted(expected-actual)
        if missing: raise RuntimeError(f"dbo.{table} missing required columns: {missing}")


def load_db_participants(cur):
    by_card={}; by_name=defaultdict(list)
    for row in cur.execute("SELECT ParticipantID,SaheliCardNumber,FullName FROM dbo.Participants"):
        p=DbParticipant(int(row[0]),clean_text(row[1]) or "",clean_text(row[2])); ck=normalize_card_key(p.card)
        if ck: by_card[ck]=p
        nk=normalize_full_name_key(p.full_name)
        if nk: by_name[nk].append(p)
    return by_card,by_name


def load_db_lites(cur):
    by_name=defaultdict(list); by_id={}
    for row in cur.execute("SELECT Id,MembershipId,FirstName,LastName,DateOfBirth,Postcode,Phone FROM dbo.LiteMembers"):
        dob=row[4].date() if isinstance(row[4],datetime) else row[4]
        l=DbLite(str(row[0]),str(row[1]),str(row[2]),str(row[3]),dob,clean_text(row[5]),clean_text(row[6]))
        by_name[normalize_name_key(l.first_name,l.last_name)].append(l); by_id[l.lite_id.lower()]=l
    return by_name,by_id


def db_date_bounds(sessions:list[SourceSession]) -> tuple[date,date]:
    return min(s.session_date for s in sessions),max(s.session_date for s in sessions)


def load_db_sessions(cur,sessions:list[SourceSession]) -> list[DbSession]:
    mn,mx=db_date_bounds(sessions)
    rows=cur.execute("""
        SELECT s.SessionId,s.SessionDate,s.VenueName,s.ActivityName,s.StartTime,s.EndTime,s.IsCancelled,COUNT(a.AttendanceId)
        FROM dbo.Sessions s LEFT JOIN dbo.SessionAttendance a ON a.SessionId=s.SessionId AND a.Attended=1
        WHERE s.SessionDate>=? AND s.SessionDate<=?
        GROUP BY s.SessionId,s.SessionDate,s.VenueName,s.ActivityName,s.StartTime,s.EndTime,s.IsCancelled
    """,mn,mx).fetchall()
    result=[]
    for r in rows:
        if normalize_venue(r[2]) not in ALL_VENUE_ALIASES: continue
        d=r[1].date() if isinstance(r[1],datetime) else r[1]; st=r[4].time() if isinstance(r[4],datetime) else r[4]; et=r[5].time() if isinstance(r[5],datetime) else r[5]
        result.append(DbSession(int(r[0]),d,str(r[2]),str(r[3]),canonical_activity(r[3]),st,et,bool(r[6]),int(r[7] or 0)))
    return result


def load_existing_attendance_keys(cur,sessions:list[SourceSession]):
    mn,mx=db_date_bounds(sessions); keys=set()
    rows=cur.execute("""SELECT a.SessionId,a.AttendanceMemberKind,a.ParticipantId,a.LiteMemberId,s.VenueName
        FROM dbo.SessionAttendance a JOIN dbo.Sessions s ON s.SessionId=a.SessionId WHERE s.SessionDate>=? AND s.SessionDate<=?""",mn,mx).fetchall()
    for r in rows:
        if normalize_venue(r[4]) not in ALL_VENUE_ALIASES: continue
        kind=(r[1] or "").upper(); mid=str(r[2]) if kind=="FULL" else (str(r[3]).lower() if r[3] else "")
        if mid: keys.add((int(r[0]),kind,mid))
    return keys


def next_lite_number(cur):
    rows=cur.execute("SELECT MembershipId FROM dbo.LiteMembers WITH (UPDLOCK,HOLDLOCK)").fetchall(); mx=0
    for (mid,) in rows:
        m=re.fullmatch(r"LITE-(\d+)",clean_text(mid) or "",re.I)
        if m: mx=max(mx,int(m.group(1)))
    return mx+1


def choose_lite_match(cands:list[DbLite],p:PersonSource):
    if not cands: return None,"NO_MATCH"
    if len(cands)==1: return cands[0],"EXACT_NAME"
    scored=[]
    for c in cands:
        score=0
        if p.dob and c.dob and p.dob==c.dob: score+=4
        if normalize_postcode(p.postcode) and normalize_postcode(c.postcode) and normalize_postcode(p.postcode)==normalize_postcode(c.postcode): score+=3
        if normalize_phone(p.phone) and normalize_phone(c.phone) and normalize_phone(p.phone)==normalize_phone(c.phone): score+=3
        scored.append((score,c))
    scored.sort(key=lambda x:(-x[0],x[1].membership_id))
    if scored[0][0]>0 and (len(scored)==1 or scored[0][0]>scored[1][0]): return scored[0][1],"EXACT_NAME_PLUS_DETAILS"
    return None,"AMBIGUOUS_NAME"


def insert_full(cur,card:str,p:PersonSource,venue:str):
    name=clean_text(p.full_name)
    cur.execute("""INSERT INTO dbo.Participants(SaheliCardNumber,FullName,DateOfBirth,Postcode,MobileNumber,Gender,Ethnicity,Site,Notes,CreatedAt)
        OUTPUT INSERTED.ParticipantID VALUES(?,?,?,?,?,?,?,?,?,SYSDATETIME())""",
        card[:50],name[:510] if name else None,p.dob,(clean_text(p.postcode) or "")[:40] or None,(clean_text(p.phone) or "")[:100] or None,
        (clean_text(p.gender) or "")[:40] or None,clean_text(p.ethnicity),venue,f"{MIGRATION_NOTE_PREFIX}; created from historical source"[:4000])
    return DbParticipant(int(cur.fetchone()[0]),card,name)


def insert_lite(cur,membership_id:str,p:PersonSource):
    first,last=split_name(p.full_name)
    if not first: raise ValueError("Cannot create LiteMember without usable name")
    last=last or NO_SURNAME_LABEL; lid=str(uuid.uuid4())
    cur.execute("""INSERT INTO dbo.LiteMembers(Id,MembershipId,FirstName,LastName,DateOfBirth,Phone,Email,Address,Postcode,EmergencyName,EmergencyPhone,EmergencyRelation,HealthConditions,Gender,Ethnicity,CreatedAtUtc,CreatedByUserId)
        VALUES(?,?,?,?,?,?,NULL,NULL,?,?,?,NULL,?,?,?,SYSUTCDATETIME(),NULL)""",
        lid,membership_id[:50],first[:100],last[:100],p.dob,(clean_text(p.phone) or "")[:30] or None,(clean_text(p.postcode) or "")[:30] or None,
        (clean_text(p.emergency_name) or "")[:200] or None,(clean_text(p.emergency_phone) or "")[:30] or None,clean_text(p.health_conditions),
        (clean_text(p.gender) or "")[:100] or None,(clean_text(p.ethnicity) or "")[:200] or None)
    return DbLite(lid,membership_id,first,last,p.dob,p.postcode,p.phone)


def time_seconds(t:time): return t.hour*3600+t.minute*60+t.second


def session_candidates(source:SourceSession,db_sessions:list[DbSession]):
    v=normalize_venue(source.venue_name)
    return [s for s in db_sessions if s.session_date==source.session_date and s.canonical_activity==source.canonical_activity and normalize_venue(s.venue_name)==v]


def choose_existing_session(source:SourceSession,db_sessions:list[DbSession]) -> Optional[DbSession]:
    c=session_candidates(source,db_sessions)
    if not c: return None
    # Innerva: exact date + slot is the business key. Do not merge different time slots.
    if source.canonical_activity=="Innerva":
        exact=[s for s in c if s.start_time==source.start_time and s.end_time==source.end_time]
        return sorted(exact,key=lambda s:(-s.attendance_count,s.is_cancelled,s.session_id))[0] if exact else None
    if source.start_time and source.end_time and source.time_quality in {"SOURCE","INFERRED"}:
        exact=[s for s in c if s.start_time==source.start_time and s.end_time==source.end_time]
        if exact: return sorted(exact,key=lambda s:(-s.attendance_count,s.is_cancelled,s.session_id))[0]
        same=[s for s in c if s.start_time==source.start_time]
        if len(same)==1: return same[0]
        close=sorted(c,key=lambda s:abs(time_seconds(s.start_time)-time_seconds(source.start_time)))
        if close and abs(time_seconds(close[0].start_time)-time_seconds(source.start_time))<=30*60: return close[0]
    if len(c)==1: return c[0]
    return sorted(c,key=lambda s:(-s.attendance_count,s.is_cancelled,s.session_id))[0]


def create_session(cur,source:SourceSession):
    if not source.start_time or not source.end_time: raise ValueError("Session time unresolved")
    if source.end_time<=source.start_time: raise ValueError("EndTime must be after StartTime")
    category=("Innerva" if source.canonical_activity=="Innerva" else DEFAULT_CATEGORY)[:30]
    notes=(f"{MIGRATION_NOTE_PREFIX}; source={source.source_file}/{source.source_sheet}/{source.source_ref}; source_activity={source.raw_activity}; "
           f"time_quality={source.time_quality}; lead={source.lead or ''}; session_type={source.session_type or ''}; cancelled={int(source.is_cancelled)}; reason={source.cancel_reason or ''}")[:1000]
    cur.execute("""INSERT INTO dbo.Sessions(Frequency,Category,SubCategory,ActivityCategory,VenueName,AssignedStaffId,SessionProviderId,ActivityName,Notes,IsRecurringWeekly,DayOfWeek,SessionDate,ArrivalTime,StartTime,EndTime,Capacity,IsBookingRequired,IsCancelled,CreatedAtUtc)
        OUTPUT INSERTED.SessionId VALUES(?,?,NULL,?,?,NULL,NULL,?,?,0,NULL,?,NULL,?,?,NULL,?,?,SYSUTCDATETIME())""",
        DEFAULT_FREQUENCY[:60],category,category,source.venue_name,source.activity_name[:300],notes,source.session_date,source.start_time,source.end_time,1 if source.is_booking_required else 0,1 if source.is_cancelled else 0)
    sid=int(cur.fetchone()[0])
    return DbSession(sid,source.session_date,source.venue_name,source.activity_name,source.canonical_activity,source.start_time,source.end_time,source.is_cancelled,0)


def insert_attendance(cur,dbs:DbSession,kind:str,member_id:str,display_id:str,member_name:Optional[str],p:PersonSource,source:SourceSession,row_ref:str):
    full=kind=="FULL"; pid=int(member_id) if full else None; lid=None if full else member_id; card=display_id if full else None
    note=(f"{MIGRATION_NOTE_PREFIX}; source={source.source_file}/{source.source_sheet}/{row_ref}; source_session={source.source_ref}; "
          f"time_quality={source.time_quality}; wellbeing_card={p.wellbeing_card or ''}; source_card={clean_text(p.raw_card) or ''}")[:1000]
    cur.execute("""INSERT INTO dbo.SessionAttendance(SessionId,AttendanceMemberKind,ParticipantId,LiteMemberId,MemberDisplayId,SaheliCardNumber,MemberName,Phone,EmergencyName,EmergencyPhone,SessionName,SessionDay,SessionDate,SessionMonth,SessionStartTime,SessionEndTime,RiskStratification,Attended,Notes,SignedInductionPaper,MedicalCondition,CreatedAtUtc,UpdatedAtUtc)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,?,?,?,SYSUTCDATETIME(),NULL)""",
        dbs.session_id,kind,pid,lid,display_id[:50],card[:50] if card else None,(clean_text(member_name) or "")[:200] or None,(clean_text(p.phone) or "")[:30] or None,
        (clean_text(p.emergency_name) or "")[:200] or None,(clean_text(p.emergency_phone) or "")[:30] or None,dbs.activity_name[:200],source.session_date.strftime("%A")[:20],source.session_date,
        source.session_date.strftime("%B")[:20],dbs.start_time,dbs.end_time,(p.risk or "")[:100] or None,note,p.signed_induction,(clean_text(p.health_conditions) or "")[:1000] or None)

# -----------------------------------------------------------------------------
# MEMBER RESOLUTION / LOG
# -----------------------------------------------------------------------------

class MigrationLog:
    def __init__(self): self.rows=[]; self.counts=Counter()
    def add(self,action,source=None,person=None,detail="",session_id=None,member_ref=""):
        self.counts[action]+=1
        self.rows.append({"Action":action,"Date":source.session_date.isoformat() if source else "","Venue":source.venue_name if source else "","Activity":source.activity_name if source else "","SourceFile":source.source_file if source else "","SourceSheet":source.source_sheet if source else "","SourceSessionRef":source.source_ref if source else "","SessionId":session_id or "","SourceCard":clean_text(person.raw_card) if person else "","SourceName":clean_text(person.full_name) if person else "","MemberRef":member_ref,"Detail":detail})
    def write(self,path:Path):
        with path.open("w",newline="",encoding="utf-8-sig") as f:
            w=csv.DictWriter(f,fieldnames=["Action","Date","Venue","Activity","SourceFile","SourceSheet","SourceSessionRef","SessionId","SourceCard","SourceName","MemberRef","Detail"]); w.writeheader(); w.writerows(self.rows)


def resolve_member(cur,p:PersonSource,participants_by_card,full_by_name,lites_by_name,lite_num_state,log,source):
    cards=card_candidates(p.raw_card)
    # A suspicious 7+ digit number that does not match a FULL participant is treated as a non-Saheli source ID -> Lite.
    if len(cards)==1:
        ck=normalize_card_key(cards[0])
        if ck in participants_by_card:
            dbp=participants_by_card[ck]; return "FULL",str(dbp.participant_id),dbp.card,dbp.full_name or p.full_name,"MATCHED_FULL"
        if looks_like_misfiled_wellbeing_number(cards[0]) and clean_text(p.full_name):
            log.add("SOURCE_LONG_CARD_RECLASSIFIED_TO_LITE",source,p,detail=f"source value {cards[0]} does not match FULL participant")
            cards=[]
    elif len(cards)>1:
        existing=[participants_by_card.get(normalize_card_key(c)) for c in cards if normalize_card_key(c) in participants_by_card]
        existing=[x for x in existing if x]
        if len(existing)==1:
            dbp=existing[0]; return "FULL",str(dbp.participant_id),dbp.card,dbp.full_name or p.full_name,"MATCHED_FULL_COMPOSITE"
        log.add("REVIEW_AMBIGUOUS_COMPOSITE_CARD",source,p,detail=f"card candidates={cards}")
        return None
    if cards:
        card=cards[0]
        # Reusing an existing FULL by card is safe even when the source lookup
        # name is blank/0. Creating a brand-new FULL without a usable name is not.
        if not clean_text(p.full_name):
            log.add("REVIEW_MISSING_FULL_NAME",source,p,detail=f"card={card}; source has no usable participant name")
            return None
        if not CREATE_MISSING_FULL_PARTICIPANTS:
            log.add("REVIEW_MISSING_FULL_PARTICIPANT",source,p,detail=f"card={card}"); return None
        try:
            dbp=insert_full(cur,card,p,source.venue_name); participants_by_card[normalize_card_key(card)]=dbp
            nk=normalize_full_name_key(dbp.full_name)
            if nk: full_by_name[nk].append(dbp)
            log.add("CREATED_FULL",source,p,detail=f"ParticipantID={dbp.participant_id}; card={card}",member_ref=f"FULL:{dbp.participant_id}")
            return "FULL",str(dbp.participant_id),card,dbp.full_name or p.full_name,"CREATED_FULL"
        except Exception as exc:
            log.add("REVIEW_FULL_CREATE_FAILED",source,p,detail=str(exc)); return None

    # No Saheli card -> Lite exactly as requested.
    first,last=split_name(p.full_name)
    if not first:
        log.add("REVIEW_CARDLESS_WITHOUT_NAME",source,p,detail="No Saheli card and no usable name"); return None
    last=last or NO_SURNAME_LABEL
    nk=normalize_name_key(first,last); candidates=lites_by_name.get(nk,[])
    lite,why=choose_lite_match(candidates,p)
    if lite:
        return "LITE",lite.lite_id,lite.membership_id,f"{lite.first_name} {lite.last_name}".strip(),f"MATCHED_LITE_{why}"
    if candidates and why=="AMBIGUOUS_NAME":
        log.add("REVIEW_AMBIGUOUS_LITE_NAME",source,p,detail=f"{len(candidates)} existing Lite members share this name")
        return None
    if not CREATE_MISSING_LITE_MEMBERS:
        log.add("REVIEW_MISSING_LITE",source,p,detail="No exact Lite first+last match"); return None
    mid=f"LITE-{lite_num_state[0]}"; lite_num_state[0]+=1
    try:
        lite=insert_lite(cur,mid,p); lites_by_name[nk].append(lite)
        log.add("CREATED_LITE",source,p,detail=f"MembershipId={mid}",member_ref=f"LITE:{lite.lite_id}")
        return "LITE",lite.lite_id,lite.membership_id,f"{lite.first_name} {lite.last_name}".strip(),"CREATED_LITE"
    except Exception as exc:
        log.add("REVIEW_LITE_CREATE_FAILED",source,p,detail=str(exc)); return None

# -----------------------------------------------------------------------------
# AUDIT / MIGRATION
# -----------------------------------------------------------------------------

def _legacy_arcc_print_source_audit_unused(sessions:list[SourceSession],stats:dict[str,int],files:dict[str,Optional[Path]],start_filter,end_filter):
    total=sum(len(s.people) for s in sessions)
    print("\n=== MEN'S SOURCE AUDIT ===")
    print(f"Parsed source sessions/slots  : {len(sessions):,}")
    print(f"Parsed attendance rows        : {total:,}")
    print(f"First source date             : {min((s.session_date for s in sessions),default=None)}")
    print(f"Last source date              : {max((s.session_date for s in sessions),default=None)}")
    print(f"Innerva sessions/slots        : {sum(1 for s in sessions if s.canonical_activity=='Innerva'):,}")
    print(f"Innerva attendance            : {sum(len(s.people) for s in sessions if s.canonical_activity=='Innerva'):,}")
    print(f"Cancelled sessions/slots      : {sum(1 for s in sessions if s.is_cancelled):,}")
    print(f"Zero-attendance non-cancelled : {sum(1 for s in sessions if not s.people and not s.is_cancelled):,}")
    print(f"Placeholder-time sessions     : {sum(1 for s in sessions if s.time_quality=='PLACEHOLDER'):,}")
    print(f"Unresolved-time sessions      : {sum(1 for s in sessions if not s.start_time or not s.end_time):,}")
    print(f"Source session duplicates merged: {stats.get('source_session_duplicates_merged',0):,}")
    print(f"Source attendance duplicates removed: {stats.get('source_attendance_duplicates_removed',0):,}")
    print("\nAttendance by year/month:")
    bym=Counter((s.session_date.year,s.session_date.month) for s in sessions for _ in s.people)
    for (y,m),n in sorted(bym.items()): print(f"  {y}-{m:02d}: {n:,}")
    print("\nAttendance by activity:")
    bya=Counter(s.canonical_activity for s in sessions for _ in s.people)
    for k,v in bya.most_common(30): print(f"  {k:28s} {v:,}")
    print("\nSource files selected:")
    for k,p in files.items(): print(f"  {k:12s}: {p.name if p else 'NOT FOUND'}")
    if files.get("appointments"):
        print("\nAppointment workbook detected and intentionally EXCLUDED from session attendance migration:")
        print(f"  {files['appointments'].name}")


def run_migration(commit:bool,start_filter:Optional[date],end_filter:Optional[date],audit_only:bool=False):
    files=discover_files()
    required=["kyn_zaheer","mens_2026"]
    print("Saheli CRM - Men's Full Historical Migration V1")
    print(f"Source directory: {BASE_DIR}")
    for k in required:
        print(f"  {'OK' if files.get(k) else 'MISSING'} {k}: {files[k].name if files.get(k) else ''}")
    missing_required=[k for k in required if not files.get(k)]
    if missing_required:
        print("\nERROR: required Men's source workbook(s) are missing:")
        for k in missing_required:
            print(f"  - {k}")
        print("Copy the missing workbook(s) into the same folder as this script and run again.")
        print("Database migration has NOT started.")
        return 2
    sessions,stats=parse_all_sources(files,start_filter,end_filter)
    if not sessions: raise RuntimeError("No Men's programme sessions parsed from source files")
    print_source_audit(sessions,stats,files,start_filter,end_filter)
    unresolved=sum(1 for s in sessions if not s.start_time or not s.end_time)
    if unresolved:
        print("\nERROR: unresolved session times remain. Database migration blocked."); return 2
    if audit_only: return 0

    validate_connection_string()
    try: import pyodbc
    except ImportError as exc: raise RuntimeError("pyodbc is required: pip install pyodbc openpyxl") from exc
    cn=pyodbc.connect(CONNECTION_STRING,autocommit=False); cur=cn.cursor(); cur.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
    log=MigrationLog()
    try:
        preflight_schema(cur)
        participants_by_card,full_by_name=load_db_participants(cur); lites_by_name,_=load_db_lites(cur)
        db_sessions=load_db_sessions(cur,sessions); existing_keys=load_existing_attendance_keys(cur,sessions); lite_state=[next_lite_number(cur)]
        print("\n=== DATABASE PREFLIGHT ===")
        print(f"Existing men's-programme venue sessions in date range: {len(db_sessions):,}")
        print(f"Existing attendance member/session keys          : {len(existing_keys):,}")
        print(f"Existing FULL participants loaded                : {len(participants_by_card):,}")
        print(f"Existing LITE name keys loaded                   : {len(lites_by_name):,}")
        print(f"Next reserved Lite membership number             : {lite_state[0]}")

        for source in sessions:
            # Placeholder times are synthetic only. If more than one CRM session
            # exists for the same date/activity/venue, do not guess which one to
            # reuse; force a review instead of silently attaching attendance to
            # the wrong session.
            if source.time_quality == "PLACEHOLDER":
                placeholder_candidates = session_candidates(source, db_sessions)
                if len(placeholder_candidates) > 1:
                    ids = ",".join(str(x.session_id) for x in sorted(placeholder_candidates, key=lambda z: z.session_id))
                    log.add("REVIEW_AMBIGUOUS_SESSION_MATCH", source, detail=f"placeholder source time; candidate SessionIds={ids}")
                    continue
            dbs=choose_existing_session(source,db_sessions)
            if dbs:
                log.add("EXISTING_SESSION",source,detail=f"Reused SessionId={dbs.session_id}; CRM time={dbs.start_time}-{dbs.end_time}",session_id=dbs.session_id)
                if source.is_cancelled:
                    if dbs.attendance_count>0:
                        log.add("REVIEW_CANCELLED_SESSION_HAS_CRM_ATTENDANCE",source,detail=f"Source cancelled but SessionId={dbs.session_id} has {dbs.attendance_count} attended rows",session_id=dbs.session_id)
                    elif not dbs.is_cancelled:
                        cur.execute("UPDATE dbo.Sessions SET IsCancelled=1 WHERE SessionId=?",dbs.session_id); dbs.is_cancelled=True
                        log.add("MARKED_EXISTING_SESSION_CANCELLED",source,detail=source.cancel_reason or "cancelled",session_id=dbs.session_id)
            else:
                try:
                    dbs=create_session(cur,source); db_sessions.append(dbs); log.add("NEW_SESSION",source,detail=f"Created SessionId={dbs.session_id}; booking={source.is_booking_required}; cancelled={source.is_cancelled}; time_quality={source.time_quality}",session_id=dbs.session_id)
                except Exception as exc:
                    log.add("REVIEW_SESSION_NOT_CREATED",source,detail=str(exc)); continue
            if source.is_cancelled:
                log.add("SOURCE_CANCELLED_SESSION",source,detail=source.cancel_reason or "cancelled",session_id=dbs.session_id); continue
            if not source.people:
                log.add("SOURCE_ZERO_ATTENDANCE_SESSION",source,detail="Valid session/booking slot with zero attendees",session_id=dbs.session_id)
            for p,rowref in source.people:
                resolved=resolve_member(cur,p,participants_by_card,full_by_name,lites_by_name,lite_state,log,source)
                if not resolved: continue
                kind,mid,display,name,resolution=resolved
                key=(dbs.session_id,kind,str(mid).lower() if kind=="LITE" else str(mid))
                if key in existing_keys:
                    if UPDATE_EXISTING_ATTENDANCE_TO_ATTENDED:
                        if kind=="FULL": cur.execute("UPDATE dbo.SessionAttendance SET Attended=1,UpdatedAtUtc=CASE WHEN Attended=0 THEN SYSUTCDATETIME() ELSE UpdatedAtUtc END WHERE SessionId=? AND AttendanceMemberKind='FULL' AND ParticipantId=?",dbs.session_id,int(mid))
                        else: cur.execute("UPDATE dbo.SessionAttendance SET Attended=1,UpdatedAtUtc=CASE WHEN Attended=0 THEN SYSUTCDATETIME() ELSE UpdatedAtUtc END WHERE SessionId=? AND AttendanceMemberKind='LITE' AND LiteMemberId=?",dbs.session_id,mid)
                    log.add("ALREADY_IN_CRM",source,p,detail=f"{resolution}; attendance exists",session_id=dbs.session_id,member_ref=f"{kind}:{mid}"); continue
                try:
                    insert_attendance(cur,dbs,kind,mid,display,name,p,source,rowref); existing_keys.add(key); log.add("NEW_ATTENDANCE",source,p,detail=resolution,session_id=dbs.session_id,member_ref=f"{kind}:{mid}")
                except Exception as exc:
                    log.add("REVIEW_ATTENDANCE_INSERT_FAILED",source,p,detail=str(exc),session_id=dbs.session_id,member_ref=f"{kind}:{mid}"); raise

        report=BASE_DIR/f"mens_migration_{'commit' if commit else 'preview'}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"; log.write(report)
        print("\n=== ACTION SUMMARY ===")
        for a,n in sorted(log.counts.items()): print(f"{a:40s} {n:,}")
        reviews=sum(n for a,n in log.counts.items() if a.startswith("REVIEW_"))
        print(f"\nReview-required rows/actions: {reviews:,}")
        print(f"Detailed migration report     : {report}")
        if commit:
            if reviews:
                cn.rollback(); print("\nCOMMIT BLOCKED: review-required items exist. Database unchanged."); return 3
            cn.commit(); print("\nMode: COMMITTED")
        else:
            cn.rollback(); print("\nMode: PREVIEW ONLY - transaction rolled back; database unchanged.")
            print("Run again with --commit only after the preview summary/report is correct.")
        return 0
    except Exception:
        cn.rollback(); raise
    finally:
        cur.close(); cn.close()

# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------

def parse_cli_date(v:Optional[str]) -> Optional[date]:
    return datetime.strptime(v,"%Y-%m-%d").date() if v else None


def main():
    ap=argparse.ArgumentParser(description="Men's programme full historical migration")
    ap.add_argument("--audit-only",action="store_true",help="Parse Excel only; do not connect to SQL")
    ap.add_argument("--commit",action="store_true",help="Commit after preview is reviewed and REVIEW_* is zero")
    ap.add_argument("--start",help="Optional inclusive start date YYYY-MM-DD")
    ap.add_argument("--end",help="Optional inclusive end date YYYY-MM-DD")
    args=ap.parse_args(); start=parse_cli_date(args.start) or DEFAULT_START_DATE; end=parse_cli_date(args.end) or DEFAULT_END_DATE
    if start and end and start>end: raise SystemExit("--start cannot be after --end")
    return run_migration(args.commit,start,end,args.audit_only)

if __name__=="__main__":
    try: raise SystemExit(main())
    except Exception as exc:
        print(f"\nFATAL: {exc}",file=sys.stderr)
        import traceback; traceback.print_exc(); raise SystemExit(1)
