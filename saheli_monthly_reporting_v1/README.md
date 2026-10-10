# Saheli Hub Monthly Reporting V1.1

This version creates **one Excel workbook only**.

## Workbook layout

- `SUMMARY`
- `DEMOGRAPHICS`
- `TOP ACTIVITIES`
- `REGISTRATION INSIGHTS`
- one worksheet for every CRM location / venue, e.g. `CALTHORPE`, `ARCC`, `HANDSWORTH`

Each location tab contains:

- headline metrics
- previous month vs current month
- category performance
- top activities
- outcomes
- data quality
- demographics

It deliberately does **not** use funding-project links yet because those are not complete in the CRM.

## Run

```powershell
python main.py --report-month 2026-09
```

The output is:

```text
output/2026-09/Saheli_Monthly_Performance_2026-09.xlsx
```

No separate location Excel files are created.
