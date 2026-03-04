# ============================================================
# Q FULL FILE: quarterly_report_meetings_to_ics.py
# ------------------------------------------------------------
# Creates ONE meeting invite per month (per Recipient Group)
# from an Excel reminders table, with multiple VALARM reminders.
#
# Key features:
# - Consolidates to ONE invite per (QuarterMonth, Recipient Group)
# - Unions attendee emails across all rows in that group/month
# - DEDUPES projects in the invite body (so same project doesn't repeat
#   for 3w/2w/10d/7d/3d/1d reminder rows)
# - Adds multiple VALARM reminders: 21/14/10/7/3/1 days before
#
# Output: .ics files in ./ics_out/
#
# Install:
#   pip install pandas openpyxl
# Run:
#   python quarterly_report_meetings_to_ics.py
# ============================================================

from __future__ import annotations

from pathlib import Path
from datetime import datetime, timedelta
import re
import uuid

import pandas as pd


# -------------------------
# CONFIG
# -------------------------
EXCEL_PATH = r"C:\Users\shonk\OneDrive\Desktop\Saheli Hub\Projects\Project_Reminders_Simple.xlsx"
SHEET_NAME = 0  # 0 = first sheet. (Avoids pandas returning dict when None)
OUTPUT_DIR = Path("ics_out")

TIMEZONE = "Europe/London"  # for description only; DTSTART/DTEND are "floating"
MEETING_START_HOUR = 10     # 10:00
MEETING_DURATION_MIN = 30   # 30 minutes

# Reminders: days before due date
REMINDER_DAYS = [21, 14, 10, 7, 3, 1]

# Column names expected (as per your header)
COL_PROJECT = "Project"
COL_QUARTER_END = "Quarter End"
COL_RECIPIENT_GROUP = "Recipient Group"
COL_RECIPIENT_EMAILS = "Recipients (emails)"
COL_NOTES = "Notes"


# -------------------------
# Helpers
# -------------------------
def norm_emails(cell: str) -> list[str]:
    """
    Split by ; or , normalize to lowercase and dedupe preserving order.
    """
    if pd.isna(cell) or not str(cell).strip():
        return []
    raw = re.split(r"[;,]", str(cell))
    cleaned: list[str] = []
    seen: set[str] = set()

    for e in raw:
        e = e.strip().lower()
        if not e:
            continue
        if e not in seen:
            seen.add(e)
            cleaned.append(e)

    return cleaned


def to_date(val) -> datetime:
    """
    Convert Excel date/datetime to datetime (date part used).
    """
    if isinstance(val, datetime):
        return val
    try:
        return pd.to_datetime(val).to_pydatetime()
    except Exception as ex:
        raise ValueError(f"Cannot parse date: {val!r}") from ex


def dt_floating(dt: datetime) -> str:
    """
    ICS floating datetime: YYYYMMDDTHHMMSS
    """
    return dt.strftime("%Y%m%dT%H%M%S")


def build_ics_event(
    title: str,
    start_dt: datetime,
    end_dt: datetime,
    attendees: list[str],
    description: str,
    location: str = "",
    alarms_days: list[int] | None = None,
) -> str:
    """
    Create an ICS VCALENDAR with a single VEVENT and multiple VALARM triggers.
    """
    alarms_days = alarms_days or []

    uid = f"{uuid.uuid4()}@saheli"
    dtstamp = dt_floating(datetime.utcnow())

    lines: list[str] = []
    lines += ["BEGIN:VCALENDAR"]
    lines += ["VERSION:2.0"]
    lines += ["PRODID:-//Saheli Hub//Quarterly Report Reminders//EN"]
    lines += ["CALSCALE:GREGORIAN"]
    lines += ["METHOD:REQUEST"]
    lines += ["BEGIN:VEVENT"]
    lines += [f"UID:{uid}"]
    lines += [f"DTSTAMP:{dtstamp}"]
    lines += [f"SUMMARY:{title}"]
    if location:
        lines += [f"LOCATION:{location}"]
    lines += [f"DTSTART:{dt_floating(start_dt)}"]
    lines += [f"DTEND:{dt_floating(end_dt)}"]

    # Outlook generally tolerates long DESCRIPTION lines, but we still escape newlines.
    safe_desc = description.replace("\r\n", "\n").replace("\r", "\n").replace("\n", "\\n")
    lines += [f"DESCRIPTION:{safe_desc}"]

    # Attendees
    for email in attendees:
        lines += [f"ATTENDEE;CN={email};ROLE=REQ-PARTICIPANT;RSVP=TRUE:MAILTO:{email}"]

    # Multiple alarms (VALARM)
    for d in alarms_days:
        minutes = d * 24 * 60
        lines += ["BEGIN:VALARM"]
        lines += ["ACTION:DISPLAY"]
        lines += [f"DESCRIPTION:Reminder - {title} ({d} day(s) before)"]
        lines += [f"TRIGGER:-PT{minutes}M"]
        lines += ["END:VALARM"]

    lines += ["END:VEVENT"]
    lines += ["END:VCALENDAR"]

    return "\n".join(lines) + "\n"


# -------------------------
# Main
# -------------------------
def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_NAME)

    # Normalize header whitespace (prevents "missing column" due to spaces)
    df.columns = [str(c).strip() for c in df.columns]

    # Validate required columns
    for col in [COL_PROJECT, COL_QUARTER_END, COL_RECIPIENT_GROUP, COL_RECIPIENT_EMAILS]:
        if col not in df.columns:
            raise KeyError(f"Missing column in Excel: {col}")

    # Normalize types
    df[COL_QUARTER_END] = df[COL_QUARTER_END].apply(to_date)
    df["QuarterMonth"] = pd.to_datetime(df[COL_QUARTER_END]).dt.to_period("M").astype(str)  # e.g. "2026-03"
    df["EmailsList"] = df[COL_RECIPIENT_EMAILS].apply(norm_emails)

    # Consolidate to avoid duplication:
    # One meeting per (QuarterMonth, RecipientGroup)
    grouped = df.groupby(["QuarterMonth", COL_RECIPIENT_GROUP], dropna=False)

    for (qmonth, group_name), g in grouped:
        g = g.copy()

        # Determine the due date (if multiple dates in same month, choose max)
        due_dt = max(g[COL_QUARTER_END])

        start_dt = due_dt.replace(hour=MEETING_START_HOUR, minute=0, second=0, microsecond=0)
        end_dt = start_dt + timedelta(minutes=MEETING_DURATION_MIN)

        # Union emails across all rows in this group/month
        all_emails: list[str] = []
        seen_emails: set[str] = set()
        for lst in g["EmailsList"]:
            for e in lst:
                if e not in seen_emails:
                    seen_emails.add(e)
                    all_emails.append(e)

        # ---- DEDUPE projects for the description ----
        cols_for_projects = [COL_PROJECT, COL_QUARTER_END]
        if COL_NOTES in g.columns:
            cols_for_projects.append(COL_NOTES)

        g_projects = (
            g[cols_for_projects]
            .drop_duplicates(subset=[COL_PROJECT, COL_QUARTER_END])
            .sort_values([COL_PROJECT, COL_QUARTER_END])
        )

        # Build subject
        month_label = datetime.strptime(qmonth + "-01", "%Y-%m-%d").strftime("%B %Y")
        title = f"Quarterly Reports Submission - {month_label} ({group_name})"

        # Build body
        lines: list[str] = []
        lines.append(f"Submission due date: {due_dt.strftime('%d %b %Y')}")
        lines.append("")
        lines.append(f"Total projects due: {len(g_projects)}")
        lines.append("")
        lines.append("Projects included in this submission window:")

        for _, row in g_projects.iterrows():
            proj = str(row[COL_PROJECT]).strip()
            qend = row[COL_QUARTER_END].strftime("%d %b %Y")

            note = ""
            if COL_NOTES in g_projects.columns:
                v = row.get(COL_NOTES)
                if not pd.isna(v):
                    note = str(v).strip()

            if note:
                lines.append(f" - {proj} (Quarter End: {qend}) | Notes: {note}")
            else:
                lines.append(f" - {proj} (Quarter End: {qend})")

        lines.append("")
        lines.append("Auto-reminders (relative to due date): " + ", ".join([f"{d} days" for d in REMINDER_DAYS]))
        lines.append("")
        lines.append("Timezone: " + TIMEZONE)

        description = "\n".join(lines)

        ics = build_ics_event(
            title=title,
            start_dt=start_dt,
            end_dt=end_dt,
            attendees=all_emails,
            description=description,
            location="",  # e.g. "Online / Teams" if you want
            alarms_days=REMINDER_DAYS,
        )

        safe_group = re.sub(r"[^A-Za-z0-9_-]+", "_", str(group_name or "Group"))
        out_name = f"{qmonth}_{safe_group}_QuarterlyReports.ics"
        out_path = OUTPUT_DIR / out_name
        out_path.write_text(ics, encoding="utf-8")
        print("Created:", out_path)

    print(f"\nDone. Files saved to: {OUTPUT_DIR.resolve()}")


if __name__ == "__main__":
    main()