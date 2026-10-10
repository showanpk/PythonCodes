\
from __future__ import annotations

from pathlib import Path
import re
import pandas as pd
from openpyxl import load_workbook
from openpyxl.chart import BarChart, Reference
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter


HEADER_FILL = "E6007E"
DARK_FILL = "4B1748"
LIGHT_FILL = "FBEAF4"
TEAL_FILL = "2F9D8F"
WHITE = "FFFFFF"


def safe_filename(value: str) -> str:
    value = re.sub(r'[<>:"/\\|?*]+', "_", str(value))
    value = re.sub(r"\s+", "_", value.strip())
    return value[:120] or "Unknown"


def _write_df(writer, sheet_name: str, df: pd.DataFrame, startrow=0):
    clean = df.copy()
    clean.to_excel(writer, sheet_name=sheet_name[:31], index=False, startrow=startrow)


def _autosize(ws):
    for col_idx in range(1, ws.max_column + 1):
        max_len = 0
        for row_idx in range(1, min(ws.max_row, 500) + 1):
            value = ws.cell(row=row_idx, column=col_idx).value
            if value is not None:
                max_len = max(max_len, len(str(value)))
        ws.column_dimensions[get_column_letter(col_idx)].width = min(max(max_len + 2, 11), 45)


def _style_table(ws):
    if ws.max_row < 1:
        return

    thin = Side(style="thin", color="DDDDDD")
    for cell in ws[1]:
        cell.fill = PatternFill("solid", fgColor=HEADER_FILL)
        cell.font = Font(color=WHITE, bold=True)
        cell.alignment = Alignment(vertical="center")
        cell.border = Border(bottom=thin)

    ws.freeze_panes = "A2"
    ws.auto_filter.ref = ws.dimensions
    _autosize(ws)


def _add_summary_chart(ws):
    # Expects Executive Summary with Metric / Previous Month / Current Month.
    if ws.max_row < 3 or ws.max_column < 3:
        return
    chart = BarChart()
    chart.type = "col"
    chart.style = 10
    chart.title = "Previous month vs current month"
    chart.y_axis.title = "Count"
    chart.x_axis.title = "Metric"
    data = Reference(ws, min_col=2, max_col=3, min_row=1, max_row=ws.max_row)
    cats = Reference(ws, min_col=1, min_row=2, max_row=ws.max_row)
    chart.add_data(data, titles_from_data=True)
    chart.set_categories(cats)
    chart.height = 8
    chart.width = 15
    ws.add_chart(chart, "H2")


def _finish_workbook(path: Path):
    wb = load_workbook(path)

    for ws in wb.worksheets:
        _style_table(ws)

    if "Executive Summary" in wb.sheetnames:
        _add_summary_chart(wb["Executive Summary"])

    wb.save(path)


def write_master_report(
    path: Path,
    overall: pd.DataFrame,
    locations: pd.DataFrame,
    categories: pd.DataFrame,
    activities: pd.DataFrame,
    registrations: pd.DataFrame,
    demographics: tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame],
    assessment_activity: pd.DataFrame,
    outcomes: pd.DataFrame,
    quality: pd.DataFrame,
):
    gender, ethnicity, age, disability = demographics

    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        _write_df(writer, "Executive Summary", overall)
        _write_df(writer, "Location Summary", locations)
        _write_df(writer, "Category Breakdown", categories)
        _write_df(writer, "Activity Breakdown", activities)
        _write_df(writer, "Registrations", registrations)
        _write_df(writer, "Gender", gender)
        _write_df(writer, "Ethnicity", ethnicity)
        _write_df(writer, "Age", age)
        _write_df(writer, "Disability", disability)
        _write_df(writer, "Assessments", assessment_activity)
        _write_df(writer, "Outcomes", outcomes)
        _write_df(writer, "Data Quality", quality)

    _finish_workbook(path)


def write_location_reports(
    folder: Path,
    report_label: str,
    locations: pd.DataFrame,
    categories: pd.DataFrame,
    activities: pd.DataFrame,
    registrations: pd.DataFrame,
    demographics: tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame],
    assessment_activity: pd.DataFrame,
    outcomes: pd.DataFrame,
    quality: pd.DataFrame,
):
    folder.mkdir(parents=True, exist_ok=True)
    gender, ethnicity, age, disability = demographics

    all_locations = set()
    for df in [
        locations, categories, activities, registrations,
        gender, ethnicity, age, disability,
        assessment_activity, outcomes, quality,
    ]:
        if df is not None and not df.empty and "Location" in df.columns:
            all_locations.update(df["Location"].dropna().astype(str).tolist())

    paths = []

    for location in sorted(all_locations):
        filename = f"{safe_filename(location)}_{safe_filename(report_label)}.xlsx"
        path = folder / filename

        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            loc_summary = locations[locations["Location"].eq(location)].copy()
            _write_df(writer, "Summary", loc_summary)

            def write_filtered(name, df):
                if df is None or df.empty or "Location" not in df.columns:
                    _write_df(writer, name, pd.DataFrame())
                else:
                    _write_df(writer, name, df[df["Location"].eq(location)].copy())

            write_filtered("Categories", categories)
            write_filtered("Activities", activities)
            write_filtered("Registrations", registrations)
            write_filtered("Gender", gender)
            write_filtered("Ethnicity", ethnicity)
            write_filtered("Age", age)
            write_filtered("Disability", disability)
            write_filtered("Assessments", assessment_activity)
            write_filtered("Outcomes", outcomes)
            write_filtered("Data Quality", quality)

        _finish_workbook(path)
        paths.append(path)

    return paths
