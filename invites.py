from icalendar import Calendar, Event
from datetime import datetime, time

def create_invite(subject, description, date_str, filename):
    cal = Calendar()
    event = Event()
    
    # Set event details
    event.add('summary', subject)
    event.add('description', description)
    
    # Set date (9:00 AM start)
    event_date = datetime.strptime(date_str, '%Y-%m-%d').date()
    event.add('dtstart', datetime.combine(event_date, time(9, 0)))
    event.add('dtend', datetime.combine(event_date, time(10, 0)))
    
    cal.add_component(event)
    
    with open(filename, 'wb') as f:
        f.write(cal.to_ical())
    print(f"Created: {filename}")

# 1. Kick-off Invite (June 1st)
create_invite(
    "HS2 Claim 6: Initial Prep & Data Collection",
    "As requested by Naseem, starting preparation for July 1st deadline. Begin gathering Activity Forms and Timesheets.",
    "2026-06-01",
    "HS2_Step1_Kickoff.ics"
)

# 2. Internal Deadline (June 15th)
create_invite(
    "HS2 Claim 6: Internal Data Submission",
    "Internal deadline to send Activity Forms and Timesheets to Showan for cross-checking.",
    "2026-06-15",
    "HS2_Step2_Internal_Deadline.ics"
)

# 3. Final Submission Review (June 24th)
create_invite(
    "HS2 Claim 6: Final Review & Submission",
    "Final placeholder to review Budget Spreadsheet, Activity Report, and Timesheets with Aesha and Naseem.",
    "2026-06-24",
    "HS2_Step3_Final_Review.ics"
)