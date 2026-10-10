# Saheli Hub Monthly Performance Reporting V1.2

This version creates **one monthly Excel workbook** for management review.
It deliberately focuses on **location/service performance**, not funding-project reporting, because CRM session-to-project links are not yet complete.

## Workbook structure

The generated workbook contains:

1. **SUMMARY** – full management roll-up
   - headline KPIs
   - previous-month comparison
   - every location
   - top activities
   - registrations by location
   - demographic snapshot
   - outcomes snapshot
   - data-quality snapshot
   - biggest location changes
2. **DEMOGRAPHICS** – detailed gender, ethnicity, age and disability/health-condition tables
3. **TOP ACTIVITIES** – all current-month activities ranked by attendance
4. **REGISTRATION INSIGHTS** – registrations and data-quality detail by location
5. **OUTCOMES** – assessment activity and paired outcomes by participant site
6. **DATA QUALITY** – core registration data gaps by location
7. **Location sheets** – ARCC, CALTHORPE, HANDSWORTH and every other CRM location

No separate Excel file is created for each location.

## Important location rule

- Session delivery uses `Sessions.VenueName`.
- Registration and assessment reporting uses participant / assessment `Site`.
- `location_aliases.csv` maps different CRM spellings to one reporting name.

Example:

```csv
Source,CanonicalLocation
ARCC,Alum Rock Community Centre
Alum Rock CC,Alum Rock Community Centre
Alum Rock Community Centre,Alum Rock Community Centre
```

## Reporting period

By default the script reports the **last complete calendar month** and compares it with the previous month.

Example: running in October 2026 generates September 2026 vs August 2026.

To run a specific month:

```powershell
python main.py --report-month 2026-09
```

## Setup

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
Copy-Item .env.example .env
```

Put the normal SQL connection string in `.env`:

```text
SAHELI_SQL_CONNECTION_STRING=Driver={ODBC Driver 18 for SQL Server};Server=YOUR_SERVER.database.windows.net;Database=YOUR_DATABASE;Uid=YOUR_USERNAME;Pwd=YOUR_PASSWORD;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;
```

Never commit the real `.env` file to GitHub.

## Run

```powershell
python main.py --report-month 2026-09
```

Output:

```text
output\2026-09\Saheli_Monthly_Performance_2026-09.xlsx
```

## Key fixes in V1.2

- Fixed `Total Attendance` showing as zero on SUMMARY.
- Fixed blank location figures on SUMMARY.
- Added complete management roll-up to SUMMARY.
- Added dedicated OUTCOMES and DATA QUALITY tabs.
- Added safer boolean handling for attendance/cancelled fields.
- Kept all locations inside a single workbook.

## Safety

The reporting script is read-only against CRM source data. It does not update, delete, migrate or alter production records.

Automatic email sending remains disabled until the report figures are validated.

## Charts in V1.3

The generated workbook now includes visual charts to make the monthly report quicker to read:

- SUMMARY: location attendance comparison, top activities, paired outcomes and registrations
- TOP ACTIVITIES: top 10 activities by attendance
- REGISTRATION INSIGHTS: previous vs current month registrations by location
- OUTCOMES: previous vs current assessment activity by location
- DATA QUALITY: registrations with core data gaps by location
- Each location sheet: top activities by attendance

Charts are generated automatically from the same tables used in the report, so they update every month when the script is run.
