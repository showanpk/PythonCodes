"""Connect to Work deadline calendar invitations via classic Windows Outlook.

Preview: py connect_to_work_outlook_invitations_v2.py
Review invitations without sending: py connect_to_work_outlook_invitations_v2.py --drafts
Send after confirmation: py connect_to_work_outlook_invitations_v2.py --send
Requirements: Classic Outlook on Windows; pip install pywin32.
CAUTION: Sending twice creates duplicate invitations. Check calendar first.
"""
import argparse
import sys
from datetime import datetime, timedelta

PARTNERS = ['chetp@colebridge.org', 'umef@colebridge.org', 'Waheed.Saleem@wittonlodge.org.uk']
SAHELI = ['aesha@saheli.co.uk', 'showan@saheli.co.uk', 'muzz@saheli.co.uk']
# External dates supplied in the Connect to Work reporting schedule.
DEADLINES = [
    ('September provider report', '2026-10-22', PARTNERS + SAHELI),
    ('BCC October monthly report', '2026-11-05', SAHELI),
    ('October provider report', '2026-11-23', PARTNERS + SAHELI),
    ('BCC November monthly report', '2026-12-03', SAHELI),
]

def message(label, dt, recipients):
    return (f'Connect to Work — {label}\n'
            f'Report submission deadline: {dt:%A %d %B %Y}.\n\n'
            'Please complete the relevant MI reporting and supporting checks ahead of the deadline.\n'
            'Confirm any earlier internal handoff date with Aesha.\n\n'
            'This invitation is a deadline marker, NOT a meeting to attend.\n'
            'The organizer has a reminder 72 hours before this date. '
            'Each recipient controls their own Outlook reminder settings.\n')

def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    g = ap.add_mutually_exclusive_group()
    g.add_argument('--drafts', action='store_true', help='Display invitations for manual review; does not send')
    g.add_argument('--send', action='store_true', help='Send invitations after exact confirmation')
    args = ap.parse_args()
    print('CONNECT TO WORK — 4 DEADLINE INVITATIONS (PARTNER/BCC RECIPIENTS SEPARATED)')
    for label, ds, emails in DEADLINES:
        day = datetime.strptime(ds, '%Y-%m-%d')
        print(f'\n{day:%d %b %Y}: {label}\n  Organizer reminder: {day-timedelta(days=3):%d %b %Y}\n  To: {", ".join(emails)}')
    if not (args.drafts or args.send):
        print('\nPREVIEW ONLY. No Outlook items created or sent.')
        return
    try:
        import win32com.client
    except ImportError:
        sys.exit('Install pywin32: py -m pip install pywin32')
    outlook = win32com.client.Dispatch('Outlook.Application')
    print('\nOutlook profile:', outlook.Session.CurrentUser.Name)
    if args.send:
        print('WARNING: This sends 4 calendar invitations to the named recipients. Confirm no duplicates exist.')
        if input('Type SEND FOUR INVITATIONS to proceed: ').strip() != 'SEND FOUR INVITATIONS':
            print('Cancelled.'); return
    for label, ds, emails in DEADLINES:
        dt = datetime.strptime(ds, '%Y-%m-%d')
        item = outlook.CreateItem(1)  # olAppointmentItem
        item.MeetingStatus = 1  # olMeeting
        item.Subject = 'Connect to Work — Reporting Deadline: ' + label
        item.Start = dt
        item.AllDayEvent = True
        item.End = dt + timedelta(days=1)
        item.BusyStatus = 0  # Free
        item.Location = 'Deadline marker — no meeting'
        item.Body = message(label, dt, emails)
        item.ReminderSet = True
        item.ReminderMinutesBeforeStart = 4320  # 72h
        for email in emails:
            recipient = item.Recipients.Add(email)
            recipient.Type = 1
        if not item.Recipients.ResolveAll():
            print(f'FAILED to resolve recipients for {label}; skipped to avoid partial delivery.')
            continue
        if args.send:
            item.Send()
            print('SENT:', label)
        else:
            # Displays to review; do not send automatically. Avoid saving drafts until user decides.
            item.Display()
            print('OPENED (NOT SENT):', label)
    print('\nFinished. Check Outlook invitations and recipients.')

if __name__ == '__main__':
    main()
