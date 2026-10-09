"""Create Connect to Work deadline invitations in classic Outlook for Windows.

Requires: py -m pip install pywin32
Dry run (default): py connect_to_work_outlook_invitations.py
Open drafts for review: py connect_to_work_outlook_invitations.py --drafts
Send invitations: py connect_to_work_outlook_invitations.py --send

Outlook must be installed, configured, and running under the intended Saheli account.
Attendee reminders cannot be forced; Outlook reminders are set on the organizer copies.
"""
import argparse
from datetime import datetime, timedelta
import sys

ATTENDEES = [
    'chetp@colebridge.org',
    'umef@colebridge.org',
    'Waheed.Saleem@wittonlodge.org.uk',
    'aesha@saheli.co.uk',
    'showan@saheli.co.uk',
    'muzz@saheli.co.uk',
]

# Agreed submission deadlines. These are deadline markers, not training meetings.
DEADLINES = [
    ('September provider report', '2026-10-22'),
    ('BCC October monthly report', '2026-11-05'),
    ('October provider report', '2026-11-23'),
    ('BCC November monthly report', '2026-12-03'),
]

SUBJECT_PREFIX = 'Connect to Work – Reporting Deadline: '


def build_body(label, deadline):
    return (f'Connect to Work – {label}\n'
            f'Submission deadline: {deadline.strftime("%A %d %B %Y")}\n\n'
            'Please ensure the relevant MI reporting, supporting evidence, '
            'and checks are completed in time for submission.\n\n'
            'IMPORTANT: Confirm the internal partner handoff deadline with Aesha; '
            'the date shown is the stated external reporting deadline.\n\n'
            'This is a calendar deadline notification, not a meeting to attend.\n'
            'The organizer reminder is set three days before. Attendees may need '
            'to enable their own calendar reminders.\n')


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    actions = parser.add_mutually_exclusive_group()
    actions.add_argument('--drafts', action='store_true', help='Open each invitation for review; do not send automatically')
    actions.add_argument('--send', action='store_true', help='Send all four invitations after exact confirmation')
    args = parser.parse_args()

    print('CONNECT TO WORK – FOUR REPORTING DEADLINE INVITATIONS')
    for label, iso in DEADLINES:
        day = datetime.strptime(iso, '%Y-%m-%d')
        print(f'  {day:%d %b %Y} {label} | 3-day reminder: {day - timedelta(days=3):%d %b %Y}')
    print('\nRecipients: ' + ', '.join(ATTENDEES))
    if not args.send and not args.drafts:
        print('\nPREVIEW ONLY: nothing created or sent. Use --drafts to open drafts or --send to send.')
        return
    try:
        import win32com.client  # pywin32
    except ImportError:
        sys.exit('Missing pywin32. Install: py -m pip install pywin32')

    outlook = win32com.client.Dispatch('Outlook.Application')
    session = outlook.Session
    try:
        print('Outlook default account:', session.CurrentUser.Name)
    except Exception:
        pass

    if args.send:
        print('\nWARNING: --send emails actual meeting invitations to all recipients.')
        answer = input('Type exactly SEND FOUR INVITATIONS to continue: ')
        if answer != 'SEND FOUR INVITATIONS':
            print('Cancelled; nothing sent.')
            return

    for label, iso in DEADLINES:
        day = datetime.strptime(iso, '%Y-%m-%d')
        item = outlook.CreateItem(1)  # olAppointmentItem
        item.MeetingStatus = 1  # olMeeting
        item.Subject = SUBJECT_PREFIX + label
        item.Start = day
        item.AllDayEvent = True
        item.End = day + timedelta(days=1)  # exclusive end for all-day item
        item.BusyStatus = 0  # Free; deadline marker, not an actual meeting
        item.Location = 'Reporting submission – no meeting'
        item.Body = build_body(label, day)
        item.ReminderSet = True
        item.ReminderMinutesBeforeStart = 3 * 24 * 60
        for address in ATTENDEES:
            recipient = item.Recipients.Add(address)
            recipient.Type = 1  # required attendee
        if not item.Recipients.ResolveAll():
            print(f'ERROR: At least one recipient could not be resolved for {label}. No further invites processed.')
            if not args.send:
                item.Display()
            return
        if args.send:
            item.Send()
            print(f'SENT: {label} ({iso})')
        else:
            # Save to calendar as draft before display. User must click Send manually.
            item.Save()
            item.Display()
            print(f'OPENED FOR REVIEW: {label} ({iso}) – not sent by script')
    print('\nFinished. Verify all events and invited recipients in Outlook.')


if __name__ == '__main__':
    main()
