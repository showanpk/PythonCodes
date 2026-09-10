Saheli CRM - Omnia Medical Practice historical migration V1
============================================================

SOURCE FILE
-----------
Keep these files in the SAME folder:
  migrate_omnia_full_to_crm_v1.py
  Register for Exercise - Omnia.xlsx

The migration reads:
  - 2023 REGISTER (history starts 16-Nov-2022)
  - 2024 REGISTER
  - 2025 REGISTER (continues through 28-Jan-2026)

The Breakdown sheet is reporting-only and is not migrated.

SOURCE AUDIT RESULT
-------------------
Delivered sessions: 183
Empty placeholders skipped: 1
Clean attendance rows: 1744
Historical period: 2022-11-16 to 2026-01-28

Activity breakdown:
  Chair Based : 157 sessions / 1521 attendance
  Education   : 16 sessions / 177 attendance
  Crochet     : 9 sessions / 29 attendance
  Party       : 1 session / 17 attendance

Important activity rule:
  Chair Based is NOT Omnia Chair Exercise. They stay separate.
  Education, Crochet and Party are also preserved as separate historic ActivityName values.

DATE RECONSTRUCTION
-------------------
The workbook contains mixed UK/Excel date interpretation.
The runner contains explicit, audited day/month corrections:
  - 34 corrected session columns in 2023 REGISTER
  - 3 corrected session columns in 2024 REGISTER
  - 2025 REGISTER dates are used as stored

Every correction is written to the audit/action CSV and new Session Notes.

PARTICIPANT LOGIC
-----------------
- Valid Saheli Card Number is primary identity.
- A valid card row is retained even when Name is blank.
- Only skip a participant source row when both card and name are unusable.
- Existing FULL participants are reused by card after card/name validation.
- A no-card source row can inherit a card from another year only when source identity evidence is safe.
- Exact name + matching DOB is used for safe cross-year source reconciliation.
- Farzand Bi (DOB 03/10/1949) is source-resolved to Farzand Begum / card 285.
- Same-name / different-DOB people are not merged blindly.
- No-card people are matched to existing FULL only by exact name + DOB/postcode profile.
- Otherwise existing LiteMembers are reused where safe or a new Lite member is created.
- Existing CRM profile data is never overwritten.

KNOWN SOURCE REVIEW ITEM
------------------------
Abida Bi appears as:
  earlier source DOB: 15/12/1976 (no card)
  later source DOB:   15/12/1967 (Saheli Card 210)

The year digits appear reversed. V1 deliberately leaves this as REVIEW_SOURCE_DOB_CONFLICT.
Do not commit until this is reviewed against the live CRM/source evidence.

SESSION LOGIC
-------------
Default venue: Omnia Medical Practice

Final historic ActivityName values:
  Chair Based
  Education
  Crochet
  Party

Sessions are matched/reused by:
  Venue + SessionDate + ActivityName + StartTime

Source start times are preserved.
Source has no end times, so V1 uses +60 minutes and records this inference in Notes.

For required Session.Category metadata, V1 only copies from approved existing templates:
  Chair Based -> Chair Based, fallback Chair Based Exercise
  Education   -> Education, fallback Workshops, then Omnia
  Crochet     -> Crochet, fallback Crochet for Beginners
  Party       -> Party, fallback Saheli Social, then Omnia

The Category template never changes the final historical ActivityName.
If a safe template cannot be found, commit is blocked rather than guessed.

If a same-date/start session already exists under a related alternate ActivityName,
V1 blocks with REVIEW_SESSION_ACTIVITY_ALIAS_COLLISION rather than creating a duplicate.

ATTENDANCE LOGIC
----------------
- Attended source cells are imported as Attended=1.
- Existing CRM attendance is reused/skipped.
- Source duplicates are removed before DB work.
- Existing Attended=0 rows are never overwritten automatically.
- Rerunning after a successful migration should create 0 sessions/members/attendance.

INSTALL
-------
Install Python packages:
  py -m pip install -r requirements_omnia_migration.txt

or:
  py -m pip install openpyxl pyodbc

Microsoft ODBC Driver 18 for SQL Server must also be installed.

SQL CONNECTION
--------------
Recommended PowerShell environment variable:

  $env:SAHELI_SQL_CONNECTION_STRING="Driver={ODBC Driver 18 for SQL Server};Server=tcp:YOUR_SERVER.database.windows.net,1433;Database=YOUR_DATABASE;Uid=YOUR_USERNAME;Pwd=YOUR_PASSWORD;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"

Use the same Saheli CRM Azure SQL connection string used for the Handsworth/Calthorpe migrations.

RUN ORDER
---------
1) SOURCE AUDIT ONLY - no database connection:

  py .\migrate_omnia_full_to_crm_v1.py --audit-only

Expected source totals:
  Delivered sessions: 183
  Empty placeholders skipped: 1
  Clean attendance rows: 1744
  Period: 2022-11-16 to 2026-01-28

2) DATABASE PREVIEW - DEFAULT MODE, transaction ROLLS BACK:

  py .\migrate_omnia_full_to_crm_v1.py

This can perform hypothetical INSERTs inside the transaction for accurate identity/session testing,
but rolls everything back at the end. CRM is not changed.

3) REVIEW generated files:

  omnia_migration_preview_YYYYMMDD_HHMMSS.csv
  omnia_migration_summary_YYYYMMDD_HHMMSS.txt
  omnia_participant_review_YYYYMMDD_HHMMSS.csv   (only when participant reviews exist)

Do NOT commit while Review blockers > 0.

4) COMMIT ONLY AFTER A CLEAN PREVIEW:

  py .\migrate_omnia_full_to_crm_v1.py --commit

The runner refuses the commit and rolls back if any REVIEW/BLOCKER item remains.

5) IDEMPOTENCY CHECK
After a successful commit, run normal preview once more:

  py .\migrate_omnia_full_to_crm_v1.py

Expected final behaviour:
  CREATE_SESSION: 0
  CREATE_ATTENDANCE: 0
  CREATE_LITE: 0
  CREATE_FULL_FROM_VALID_CARD: 0
  REVIEW blockers: 0
  All 183 sessions reused
  All 1744 clean attendance rows accounted for by existing/skipped attendance

AZURE SQL FIREWALL
------------------
If Azure returns "Client with IP address ... is not allowed to access the server",
add your CURRENT public IP to the Azure SQL Server networking/firewall rules and rerun.

NO DATABASE SCHEMA CHANGE IS REQUIRED.
