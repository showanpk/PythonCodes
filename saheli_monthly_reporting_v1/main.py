from __future__ import annotations

import argparse
from pathlib import Path
import sys

from database import connect
from periods import get_period
from queries import (
    load_sessions,
    load_attendance,
    load_registrations,
    load_assessments,
)
from transform import (
    load_location_aliases,
    prepare_data,
    overall_summary,
    location_summary,
    category_summary,
    activity_summary,
    registration_summary,
    demographic_tables,
    assessment_activity_summary,
    outcome_summary,
    data_quality,
)
from excel_report import write_single_workbook


ROOT = Path(__file__).resolve().parent


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate one Saheli Hub monthly performance workbook."
    )
    parser.add_argument(
        "--report-month",
        help="Month to report in YYYY-MM format. Default: last complete month.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    period = get_period(args.report_month)

    print("=" * 70)
    print("SAHELI HUB MONTHLY PERFORMANCE REPORT")
    print(f"Report month   : {period.report_label}")
    print(f"Previous month : {period.previous_label}")
    print("=" * 70)

    aliases = load_location_aliases(ROOT / "location_aliases.csv")

    print("Connecting to Azure SQL...")
    with connect() as conn:
        print("Loading sessions...")
        sessions = load_sessions(
            conn, period.previous_start, period.report_end_exclusive
        )

        print("Loading attendance...")
        attendance = load_attendance(
            conn, period.previous_start, period.report_end_exclusive
        )

        print("Loading registrations...")
        registrations = load_registrations(
            conn, period.previous_start, period.report_end_exclusive
        )

        print("Loading assessment history...")
        assessments = load_assessments(conn, period.report_end_exclusive)

    print("Calculating performance...")
    sessions, attendance, registrations, assessments = prepare_data(
        sessions,
        attendance,
        registrations,
        assessments,
        period,
        aliases,
    )

    overall = overall_summary(
        sessions, attendance, registrations, assessments, period
    )
    locations = location_summary(sessions, attendance)
    categories = category_summary(sessions, attendance)
    activities = activity_summary(sessions, attendance)
    registrations_summary = registration_summary(registrations)
    demographics = demographic_tables(registrations)
    assessments_summary = assessment_activity_summary(assessments, period)
    outcomes = outcome_summary(assessments, period)
    quality = data_quality(registrations)

    output = ROOT / "output" / period.folder_label
    output.mkdir(parents=True, exist_ok=True)

    report_path = output / f"Saheli_Monthly_Performance_{period.folder_label}.xlsx"

    print("Writing one Excel workbook...")
    sheet_map = write_single_workbook(
        report_path,
        period.report_label,
        period.previous_label,
        overall,
        locations,
        categories,
        activities,
        registrations_summary,
        demographics,
        assessments_summary,
        outcomes,
        quality,
    )

    print()
    print("COMPLETED")
    print(f"Report: {report_path}")
    print(f"Location sheets: {len(sheet_map)}")
    print("No separate location Excel files were created.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print()
        print("REPORT GENERATION FAILED")
        print(str(exc))
        sys.exit(1)
