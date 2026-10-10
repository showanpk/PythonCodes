# Saheli Hub Monthly Reporting V1

This first version deliberately focuses on **location/service performance**, not funding-project reporting.

It uses the current Saheli CRM reporting/data structure to generate:

- organisation-wide month-on-month performance
- location/venue performance
- category and activity breakdowns
- registrations by site
- demographics
- health assessment activity
- paired assessment outcomes
- data quality checks
- a separate Excel workbook for each location

## Important reporting rule

Session delivery uses `Sessions.VenueName`.

Participant registration and assessment reporting uses participant/assessment `Site`.

`location_aliases.csv` can be used to map different CRM spellings to one canonical location.

## Reporting period

By default the script reports the **last complete calendar month** and compares it with the month before.

Example:
- Run during October 2026
- Report month = September 2026
- Previous month = August 2026

You can also run a specific month:

```powershell
python main.py --report-month 2026-09
```

## Setup on Windows

```powershell
py -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
```

Edit `.env` with the Azure SQL connection details.

For Microsoft Entra interactive login:

```text
SQL_AUTH=interactive
```

Then run:

```powershell
python main.py
```

Output is written to:

```text
output/YYYY-MM/
```

## Safety

This script is read-only against the CRM. It does not update, delete, migrate, or alter production data.

Email sending is intentionally not enabled in V1. First validate the generated figures for at least one reporting cycle. Email automation can then be added as V2.
