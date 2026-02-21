import pandas as pd
from datetime import datetime, time, timedelta
import win32com.client as win32

EXCEL_PATH = r"C:\Users\shonk\OneDrive\Desktop\Saheli Hub\Projects\Project_Reminders_Simple.xlsx"
SHEET_NAME = "Reminders"

DATE_COL = "Reminder Date"
SENT_COL = "Sent?"
TO_COL = "Recipients (emails)"
PROJECT_COL = "Project"
TYPE_COL = "Reminder Type"
QEND_COL = "Quarter End"
GROUP_COL = "Recipient Group"
NOTES_COL = "Notes"

# Meeting settings
MEETING_START_HOUR = 9
MEETING_START_MINUTE = 0
DURATION_MINUTES = 15

# Popup reminder timing:
# 0 = at start time (09:00), 60 = 1 hour before, 1440 = 1 day before
REMINDER_MINUTES_BEFORE = 0

def today_uk_iso() -> str:
    return datetime.now().strftime("%Y-%m-%d")

def normalize_emails(s: str) -> list[str]:
    raw = str(s).replace("\n", ";").replace("\r", ";").replace(",", ";")
    parts = [p.strip() for p in raw.split(";") if p.strip()]
    return parts

def main():
    today = today_uk_iso()

    # Force all columns to string so Sent? can be "Yes"
    df = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_NAME, dtype=str).fillna("")

    # Convert Reminder Date to yyyy-mm-dd
    df_date_iso = pd.to_datetime(df[DATE_COL], errors="coerce").dt.strftime("%Y-%m-%d")

    sent_norm = df[SENT_COL].str.strip().str.lower()
    sent_is_empty = (sent_norm == "") | (sent_norm == "no") | (sent_norm == "nan")

    due_mask = (df_date_iso == today) & sent_is_empty
    due = df[due_mask].copy()

    if due.empty:
        print(f"No calendar reminders due today ({today}).")
        return

    outlook = win32.Dispatch("Outlook.Application")

    sent_count = 0
    for idx, row in due.iterrows():
        emails = normalize_emails(row.get(TO_COL, ""))
        if not emails:
            continue

        project = row.get(PROJECT_COL, "").strip()
        rtype = row.get(TYPE_COL, "").strip()
        qend = row.get(QEND_COL, "").strip()
        group = row.get(GROUP_COL, "").strip()
        notes = row.get(NOTES_COL, "").strip()

        subject = f"Quarterly Report Reminder – {project} – {rtype}"

        # Start today at 09:00
        start_dt = datetime.now().replace(
            hour=MEETING_START_HOUR,
            minute=MEETING_START_MINUTE,
            second=0,
            microsecond=0
        )
        end_dt = start_dt + timedelta(minutes=DURATION_MINUTES)

        body = (
            "Hi team,\n\n"
            "Automated Outlook reminder for quarterly monitoring.\n\n"
            f"Project: {project}\n"
            f"Quarter end (due): {qend}\n"
            f"Reminder type: {rtype}\n"
            f"Recipient group: {group}\n\n"
            "Please provide/confirm:\n"
            "- Targets vs Actuals for the quarter\n"
            "- Evidence links (registers/photos/case studies)\n"
            "- Key highlights, risks/issues, notes\n\n"
            f"Notes: {notes}\n"
        )

        appt = outlook.CreateItem(1)  # 1 = AppointmentItem
        appt.Subject = subject
        appt.Start = start_dt
        appt.End = end_dt
        appt.Body = body

        # Turn it into a meeting invite + add attendees
        appt.MeetingStatus = 1  # olMeeting
        for e in emails:
            appt.Recipients.Add(e)

        # Popup reminder
        appt.ReminderSet = True
        appt.ReminderMinutesBeforeStart = int(REMINDER_MINUTES_BEFORE)

        # Send meeting request
        appt.Send()

        # Mark Sent
        df.loc[idx, SENT_COL] = "Yes"
        sent_count += 1

    # Save back
    with pd.ExcelWriter(EXCEL_PATH, engine="openpyxl", mode="w") as writer:
        df.to_excel(writer, sheet_name=SHEET_NAME, index=False)

    print(f"Sent {sent_count} calendar invite(s) for {today} and marked Sent?=Yes.")

if __name__ == "__main__":
    main()
