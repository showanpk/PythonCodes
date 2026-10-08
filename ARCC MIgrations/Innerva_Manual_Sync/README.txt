SAHELI CRM - MANUAL INNERVA-ONLY SYNC
===================================

What it does
------------
- Processes only Innerva Booking Sheet*.xlsx (not the ARCC exercise workbooks).
- Safely identifies Innerva slots from the workbook date + start/end time.
- Creates missing Innerva sessions with IsBookingRequired=1, Capacity=9.
- Reuses existing exact Innerva session matches; updates IsBookingRequired to 1
  only for uniquely matching Innerva sessions (not other activities).
- For Excel rows without a Saheli card, reuse an existing LiteMember by name
  (with DOB conflict checks). Otherwise create a new LiteMember and link its
  new booking; a matching FULL name does not override the cardless Lite rule.
- Adds missing booked members to dbo.SessionAttendance; attended Yes -> 1;
  No or blank -> 0, with original blank/No retained in Notes.
- Never deletes a session or attendee; never overwrites existing participant profiles.
- Flags identity mismatches, duplicate sessions, cancellation conflicts,
  capacity problems, and differing attendance statuses for manual review.
- Writes action-level CSV reports to reports/ after every run.
- Preview is truly read-only (only SELECT statements).
- Commit asks you to type IMPORT before any changes.

LOGIN TROUBLESHOOTING (SQL Error 18456)
--------------------------------------
Double-click 0_TEST_SQL_LOGIN.bat to test authentication without scanning Excel.
The test does NOT modify CRM and does NOT need an Excel workbook.
If it says LOGIN FAILED (18456), check the SQL username/password in SSMS
or a trusted SQL client against sahelihub.database.windows.net / SaheliHubCRM.
The ODBC "Invalid connection string attribute (0)" message can accompany
18456 and does not by itself prove the connection string is malformed.
Do not reset the production SQL admin login without also planning changes to
any services that use that account.

ONE-TIME SETUP
--------------
1) Rotate the SQL password that was previously embedded in
   migrate_arcc_full_to_crm_v4.py. Do not reuse or paste credentials into code.
2) On Windows, ensure Python 3.10+ and ODBC Driver 18 for SQL Server are installed.
3) In PowerShell from this folder, run:
     py -3 -m pip install -r requirements.txt
4) Put ONE copy of the latest 'Innerva Booking Sheet (1).xlsx' in this folder.
5) Test database credentials first: double-click 0_TEST_SQL_LOGIN.bat
6) Preview: double-click 1_PREVIEW.bat
7) Open the generated reports/innerva_preview_*.csv and resolve REVIEW_* rows.
8) Commit: double-click 2_COMMIT.bat, review counts and type IMPORT.
9) Run PREVIEW again; imported bookings should now be SKIP_EXISTING_BOOKING.

The Windows launchers ask for Azure SQL username/password temporarily. They
never save the password to disk. The script can also use an existing environment
variable SAHELI_SQL_CONNECTION_STRING if you configured one securely.

ADVANCED COMMAND LINE
---------------------
From PowerShell, set SAHELI_SQL_CONNECTION_STRING in the *current session*
and run:

  py -3 sync_innerva_to_crm.py --excel "Innerva Booking Sheet (1).xlsx" --preview
  py -3 sync_innerva_to_crm.py --excel "Innerva Booking Sheet (1).xlsx" --commit

When the workbook contains a sheet named 'Current' with only day/month dates,
the year is ambiguous. Supply --current-year 2026 (or 2027 as appropriate).
By default, only session dates <= today are synced. Supply --all-dates if you
need future booking slots; use --start 2026-10-01 or --end 2026-10-31 to limit.

  py -3 sync_innerva_to_crm.py --excel "Innerva Booking Sheet (1).xlsx" \
       --current-year 2026 --start 2026-10-01 --preview

Safe opt-in options after reviewing the CSV:
  --create-missing-members  (opt-in to create FULL participants when a supplied
                             card is missing in CRM; cardless LITE creation is automatic)
  --promote-attendance      (promote existing CRM Attended=0 on Excel Yes)
These options are deliberately OFF by default. Only use in --commit after
reviewing a preview run with the same options. Other attendance is untouched.

DATA MAPPING
------------
Excel date, Session slot -> Sessions.SessionDate, StartTime, EndTime
Excel Lead -> Sessions.Notes (no guessed staff assignment)
Excel Session Type -> new Sessions.SubCategory and Notes (Female/Male/Mixed)
Excel Induction Time -> Sessions.ArrivalTime when a clock time
Excel cancellation -> Sessions.IsCancelled (only if no CRM bookings conflict)
New Sessions -> VenueName='Alum Rock Community Centre', ActivityName='Innerva',
                Category='Innerva', IsBookingRequired=1, Capacity=9
Excel with Saheli Card Number -> match existing FULL member; otherwise review
    unless --create-missing-members is explicitly enabled.
Excel without Saheli Card Number -> match existing LITE by name and verify DOB
    when present; if no Lite name matches, CREATE a new Lite member automatically
    (even if a FULL person shares that name). Never overwrite a profile.
Booked valid member -> SessionAttendance linked to Sessions.SessionId
Excel Attended Yes/No/blank -> SessionAttendance.Attended 1/0/0
Excel signed induction, medical condition, risk -> SessionAttendance fields
Excel 'Any Issues during session' -> SessionAttendance.Notes
Cancelled sessions -> session only; no attendance imported

IMPORTANT LIMITS
----------------
- Actual current .xlsx was not uploaded with this package. The parser is based
  on the sample table and original V4 script. Verify the preview CSV against the
  current workbook BEFORE allowing production commit.
- Only Alum Rock Innerva is in scope. Other locations, Innerva-like activities,
  or recurring templates are not modified.
- Zero capacity isn't assumed. A booking block has at most nine valid positions.
- Legacy CRM sessions with >9 bookings, ambiguous duplicate slots, different
  session duration, or mismatched genders are flagged, not automatically repaired.
- Excel alone cannot prove when a booked slot was subsequently cancelled or
  released. There is no automatic removal of old CRM bookings.
- Attendance blank and No both become SQL bit 0; original source is noted.
- Keep the CSV reports private: they may include participant names/card numbers.
- SQL writes are transactional. A SQL error rolls back that import transaction.
- In a commit with some review rows, only other unambiguous records are changed.
  Check all REVIEW_* rows before repeating the commit.
