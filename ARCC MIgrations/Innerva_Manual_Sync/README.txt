SAHELI CRM - INNERVA-ONLY MANUAL SYNC (REVIEW-SAFE UPDATE)
=====================================================

KEEP YOUR EXISTING migrate_arcc_full_to_crm_v4.py UNCHANGED.
This package changes ONLY the Innerva-only script and its launchers.

HOW TO INSTALL
--------------
1. Make a backup of your existing Innerva_Manual_Sync folder.
2. Extract these files over that folder (or use this as a separate folder).
3. Put ONE Innerva Booking Sheet*.xlsx in the folder. Close it in Excel.
4. Run: py -3 -m pip install -r requirements.txt (first time only).
5. Double-click 0_TEST_SQL_LOGIN.bat (optional connection test).
6. Double-click 1_PREVIEW.bat. Enter your Azure SQL login when prompted.
7. Review ALL reports in reports/: preview, ready_to_import, needs_review.
8. DO NOT run Commit automatically. The safe approach is to send the preview
   to be reviewed before inserting anything into the production database.

WHAT IS FIXED
-------------
* Source: accepts only real Innerva workbook tabs such as '2026', '2027'
  and 'July 25'. Skips Report, Table2, Test Analysis and helper tabs.
* The ambiguous 'Current' tab is skipped unless --current-year is supplied;
  use this override ONLY after manually confirming the real year.
* Malformed time ranges such as 00:00-12:00 are excluded as REVIEW_BAD_SLOT.
* Matches sessions by exact date, start/end time and Alum Rock venue.
  Duplicate CRM sessions are NEVER chosen arbitrarily.
* Checks existing booking MemberName within the matched CRM session. If an
  Excel row has no Saheli card but that name is already booked as FULL in
  the SAME session, flags REVIEW_POSSIBLE_EXISTING_FULL_BOOKING instead
  of creating a potentially duplicate Lite booking. This does not use
  name matching to modify FULL participant profiles.
* If the row has no card and no same-session conflict: match Lite by name,
  verify DOB when possible; otherwise create one Lite member and insert a
  separate booking for each session. No existing member profile is updated.
* Review session capacity before adding missing bookings. Never force
  additions into an already-full/overfull session.
* Read-only Preview produces:
    reports/innerva_preview_TIMESTAMP.csv (ALL actions)
    reports/innerva_ready_to_import_TIMESTAMP.csv (actions on review-free slots)
    reports/innerva_needs_review_TIMESTAMP.csv (identity and slot problems)
* The ordinary 2_COMMIT.bat is BLOCKED if ANY REVIEW_* warnings exist.
* A separate 3_COMMIT_REVIEW_FREE_ONLY.bat exists for an explicitly approved
  partial import. This performs another fresh preview and asks you to type
  IMPORT SAFE. It completely excludes every slot with any REVIEW_* warning.
  Use this button ONLY after checking and approving the ready-to-import CSV.
  A SQL exception or newly detected review during the commit rolls back.

IMPORTANT BUSINESS RULES
------------------------
- Existing matched Innerva session -> reuse SessionId, don't create another.
- Missing uniquely identified slot -> create Booking Session (IsBookingRequired=1).
- Existing FULL member with supplied Saheli card -> reuse ParticipantId.
- Blank Excel card -> existing Lite name match -> reuse LiteMemberId.
- Blank Excel card and no matching Lite -> create Lite (except if same-session
  member conflict, DOB ambiguity, or capacity limit needs review).
- The member's existence does NOT mean they are already booked. Always check
  SessionAttendance(SessionId, MemberKind, MemberId) before inserting.
- Excel 'Yes' -> Attended=1. No/blank -> 0 for NEW bookings only.
- Never silently change existing attendance, remove bookings, or overwrite profiles.
- Other Saheli activities, booking modules, and sites remain untouched.

IMPORTANT: At-capacity source rows, duplicate times, ID conflicts, and
unparseable source rows stay in the review CSV; they are NOT imported.
Some existing CRM attendance may be recorded with another FULL/LITE identity;
please investigate and reconcile those manually before considering it missing.

COMMAND LINE EXAMPLES (if you already set SAHELI_SQL_CONNECTION_STRING)
------------------------------------------------------------------------
py -3 sync_innerva_to_crm.py --excel "Innerva Booking Sheet (1).xlsx" --preview
py -3 sync_innerva_to_crm.py --excel "Innerva Booking Sheet (1).xlsx" --commit
py -3 sync_innerva_to_crm.py --excel "Innerva Booking Sheet (1).xlsx" --commit --allow-safe-partial
py -3 sync_innerva_to_crm.py --excel "Innerva Booking Sheet (1).xlsx" --start 2026-10-01 --preview

By default dates after today are excluded. Use --all-dates only intentionally.
To include ambiguous 'Current', supply --current-year YEAR *only after verifying*.
Use --promote-attendance or --create-missing-members only after a separate audit;
they are OFF by default and are NOT necessary for adding normal Lite bookings.

SECURITY / PRODUCTION
---------------------
- Old V4 Python file has embedded Azure SQL credentials. Rotate that account's
  password and use the terminal prompt/environment variable, not source code.
- Keep preview CSVs private (participant names and card IDs).
- No live production SQL connection was available while building this package.
  Offline mock tests and workbook parsing do NOT replace a real CRM comparison.
- The package does not include, copy, or modify the participant Excel workbook.
- Run Preview and send the NEW CSV before approving any Commit.
