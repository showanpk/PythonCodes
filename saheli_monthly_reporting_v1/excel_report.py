from __future__ import annotations

from pathlib import Path
import re
import math
import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

MAGENTA = "D41474"
MAGENTA_LIGHT = "F8DCEB"
YELLOW = "FFC000"
NAVY = "0F4C81"
NAVY_DARK = "17365D"
WHITE = "FFFFFF"
LIGHT_GREY = "F3F4F6"
MID_GREY = "D9E1F2"
GREEN = "E2F0D9"
RED = "FCE4D6"
AMBER = "FFF2CC"
TEXT = "222222"


def safe_filename(value: str) -> str:
    value = re.sub(r'[<>:"/\\|?*]+', "_", str(value))
    value = re.sub(r"\s+", "_", value.strip())
    return value[:120] or "Unknown"


def _safe_sheet_name(value: str, existing: set[str]) -> str:
    raw = str(value or "Unknown").strip()
    low = raw.casefold()

    if "calthorpe" in low:
        base = "CALTHORPE"
    elif "alum rock" in low or low == "arcc":
        base = "ARCC"
    elif "handsworth" in low:
        base = "HANDSWORTH"
    elif "unknown" in low:
        base = "UNKNOWN"
    else:
        base = re.sub(r"[\[\]:*?/\\]", "", raw).upper()
        base = re.sub(r"\s+", " ", base).strip()
        if len(base) > 28:
            base = base[:28].rstrip()
        if not base:
            base = "LOCATION"

    candidate = base[:31]
    counter = 2
    while candidate in existing:
        suffix = f" {counter}"
        candidate = f"{base[:31-len(suffix)]}{suffix}"
        counter += 1
    existing.add(candidate)
    return candidate


def _write_df(writer, sheet_name: str, df: pd.DataFrame, startrow: int = 0, startcol: int = 0):
    if df is None:
        df = pd.DataFrame()
    df.to_excel(
        writer,
        sheet_name=sheet_name[:31],
        index=False,
        startrow=startrow,
        startcol=startcol,
    )


def _as_number(v):
    if pd.isna(v):
        return None
    if hasattr(v, "item"):
        v = v.item()
    return v


def _metric_value(df: pd.DataFrame, metric: str, column: str):
    if df is None or df.empty:
        return 0
    row = df[df["Metric"].eq(metric)]
    if row.empty:
        return 0
    return _as_number(row.iloc[0][column]) or 0


def _style_title(ws, title: str, subtitle: str, end_col: int = 8):
    ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=end_col)
    c = ws.cell(1, 1, title)
    c.fill = PatternFill("solid", fgColor=MAGENTA)
    c.font = Font(color=WHITE, bold=True, size=17)
    c.alignment = Alignment(vertical="center")
    ws.row_dimensions[1].height = 26

    ws.merge_cells(start_row=2, start_column=1, end_row=2, end_column=end_col)
    s = ws.cell(2, 1, subtitle)
    s.fill = PatternFill("solid", fgColor=MAGENTA_LIGHT)
    s.font = Font(color=TEXT, italic=True, size=10)
    s.alignment = Alignment(vertical="center")
    ws.row_dimensions[2].height = 22


def _section_header(ws, row: int, col_start: int, col_end: int, text: str):
    ws.merge_cells(start_row=row, start_column=col_start, end_row=row, end_column=col_end)
    c = ws.cell(row, col_start, text)
    c.fill = PatternFill("solid", fgColor=YELLOW)
    c.font = Font(bold=True, color=TEXT, size=11)
    c.alignment = Alignment(vertical="center")
    ws.row_dimensions[row].height = 20


def _table_header(ws, row: int, col_start: int, headers: list[str]):
    for idx, header in enumerate(headers, start=col_start):
        c = ws.cell(row, idx, header)
        c.fill = PatternFill("solid", fgColor=NAVY)
        c.font = Font(color=WHITE, bold=True)
        c.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)


def _apply_thin_borders(ws, min_row: int, max_row: int, min_col: int, max_col: int):
    thin = Side(style="thin", color="D9D9D9")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)
    for row in ws.iter_rows(min_row=min_row, max_row=max_row, min_col=min_col, max_col=max_col):
        for cell in row:
            cell.border = border


def _autowidth(ws, max_row_scan: int = 250):
    for col_idx in range(1, ws.max_column + 1):
        max_len = 0
        for row_idx in range(1, min(ws.max_row, max_row_scan) + 1):
            value = ws.cell(row_idx, col_idx).value
            if value is not None:
                max_len = max(max_len, len(str(value)))
        width = min(max(max_len + 2, 11), 34)
        ws.column_dimensions[get_column_letter(col_idx)].width = width


def _format_data_header_row(ws, row: int, start_col: int, end_col: int):
    for c in ws.iter_cols(min_col=start_col, max_col=end_col, min_row=row, max_row=row):
        cell = c[0]
        cell.fill = PatternFill("solid", fgColor=NAVY)
        cell.font = Font(color=WHITE, bold=True)
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)


def _trend_fill(cell, value):
    text = str(value or "").upper()
    if text == "UP":
        cell.fill = PatternFill("solid", fgColor=GREEN)
    elif text == "DOWN":
        cell.fill = PatternFill("solid", fgColor=RED)
    elif text == "STABLE":
        cell.fill = PatternFill("solid", fgColor=AMBER)


def _build_summary_sheet(
    ws,
    overall: pd.DataFrame,
    locations: pd.DataFrame,
    report_label: str,
    previous_label: str,
):
    _style_title(
        ws,
        f"Saheli Hub Monthly Performance Report – {report_label}",
        f"Reporting period: {report_label} | Comparison: {previous_label}",
        end_col=8,
    )

    _section_header(ws, 4, 1, 2, "Headline metrics")
    _section_header(ws, 4, 4, 8, "Month-on-month comparison")

    headline_metrics = [
        "Sessions Delivered",
        "Total Attendance",
        "Unique Participants",
        "New Registrations",
        "Health Assessments",
        "Follow-up Assessments",
    ]

    for i, metric in enumerate(headline_metrics, start=5):
        ws.cell(i, 1, metric)
        value = _metric_value(overall, metric, "Current Month")
        c = ws.cell(i, 2, value)
        c.font = Font(color=MAGENTA, bold=True)
        ws.cell(i, 1).fill = PatternFill("solid", fgColor=MAGENTA_LIGHT)
        ws.cell(i, 2).fill = PatternFill("solid", fgColor=MAGENTA_LIGHT)

    headers = ["Measure", previous_label, report_label, "Difference", "% Change"]
    _table_header(ws, 5, 4, headers)

    compare_metrics = [
        "Sessions Delivered",
        "Total Attendance",
        "Unique Participants",
        "New Registrations",
        "Health Assessments",
        "Follow-up Assessments",
    ]

    for r, metric in enumerate(compare_metrics, start=6):
        prev = _metric_value(overall, metric, "Previous Month")
        curr = _metric_value(overall, metric, "Current Month")
        diff = curr - prev
        pct = None if not prev else round(diff / prev * 100, 1)
        vals = [metric, prev, curr, diff, pct]
        for j, val in enumerate(vals, start=4):
            ws.cell(r, j, val)
        if pct is not None:
            ws.cell(r, 8).number_format = '0.0%'
            ws.cell(r, 8, pct / 100)

    _apply_thin_borders(ws, 5, 11, 4, 8)

    start_row = 14
    _section_header(ws, start_row, 1, 8, "Location performance")
    location_cols = [
        "Location",
        "Previous Sessions",
        "Current Sessions",
        "Previous Attendance",
        "Current Attendance",
        "Previous Unique Participants",
        "Current Unique Participants",
        "Trend",
    ]
    _table_header(ws, start_row + 1, 1, location_cols)

    if locations is not None and not locations.empty:
        for i, row in enumerate(locations.itertuples(index=False), start=start_row + 2):
            record = row._asdict()
            vals = [record.get(col) for col in location_cols]
            for j, val in enumerate(vals, start=1):
                ws.cell(i, j, _as_number(val))
            _trend_fill(ws.cell(i, 8), record.get("Trend"))
        _apply_thin_borders(ws, start_row + 1, start_row + 1 + len(locations), 1, 8)

    ws.freeze_panes = "A5"
    _autowidth(ws)
    ws.column_dimensions["A"].width = 32
    ws.column_dimensions["D"].width = 24


def _build_demographics_sheet(ws, demographics, report_label: str):
    gender, ethnicity, age, disability = demographics
    _style_title(ws, f"Demographics – {report_label}", "New registrations during the reporting month", end_col=7)

    blocks = [
        ("Gender", gender),
        ("Ethnicity", ethnicity),
        ("Age profile", age),
        ("Disability / health condition", disability),
    ]

    row = 4
    for title, df in blocks:
        _section_header(ws, row, 1, 7, title)
        row += 1
        if df is None or df.empty:
            ws.cell(row, 1, "No data")
            row += 3
            continue
        headers = list(df.columns)
        _table_header(ws, row, 1, headers)
        for ridx, rec in enumerate(df.itertuples(index=False), start=row + 1):
            for cidx, val in enumerate(rec, start=1):
                ws.cell(ridx, cidx, _as_number(val))
        _apply_thin_borders(ws, row, row + len(df), 1, len(headers))
        row += len(df) + 3

    ws.freeze_panes = "A4"
    _autowidth(ws)


def _build_top_activities_sheet(ws, activities: pd.DataFrame, report_label: str):
    _style_title(ws, f"Top Activities – {report_label}", "Current-month delivery ranked by attendance", end_col=7)
    _section_header(ws, 4, 1, 7, "Activity performance across all locations")

    if activities is None or activities.empty:
        ws.cell(5, 1, "No activity data")
        return

    cols = [
        "Location",
        "ActivityCategoryResolved",
        "ActivityName",
        "Sessions",
        "Attendance",
        "UniqueParticipants",
        "Average Attendance / Session",
    ]
    view = activities[cols].copy().sort_values("Attendance", ascending=False)
    headers = [
        "Location",
        "Category",
        "Activity",
        "Sessions",
        "Attendance",
        "Unique participants",
        "Avg attendance / session",
    ]
    _table_header(ws, 5, 1, headers)
    for ridx, row in enumerate(view.itertuples(index=False), start=6):
        for cidx, val in enumerate(row, start=1):
            ws.cell(ridx, cidx, _as_number(val))
    _apply_thin_borders(ws, 5, 5 + len(view), 1, 7)
    ws.freeze_panes = "A6"
    _autowidth(ws)


def _build_registration_sheet(ws, registrations, quality, report_label: str, previous_label: str):
    _style_title(ws, f"Registration Insights – {report_label}", f"New registrations and data quality compared with {previous_label}", end_col=8)

    _section_header(ws, 4, 1, 5, "Registrations by location")
    if registrations is not None and not registrations.empty:
        headers = list(registrations.columns)
        _table_header(ws, 5, 1, headers)
        for ridx, row in enumerate(registrations.itertuples(index=False), start=6):
            for cidx, val in enumerate(row, start=1):
                ws.cell(ridx, cidx, _as_number(val))
        _apply_thin_borders(ws, 5, 5 + len(registrations), 1, len(headers))

    qstart = 8 + (len(registrations) if registrations is not None else 0)
    _section_header(ws, qstart, 1, 8, "Data quality for new registrations")
    if quality is not None and not quality.empty:
        headers = list(quality.columns)
        _table_header(ws, qstart + 1, 1, headers)
        for ridx, row in enumerate(quality.itertuples(index=False), start=qstart + 2):
            for cidx, val in enumerate(row, start=1):
                ws.cell(ridx, cidx, _as_number(val))
        _apply_thin_borders(ws, qstart + 1, qstart + 1 + len(quality), 1, len(headers))

    ws.freeze_panes = "A5"
    _autowidth(ws)


def _write_small_table(ws, start_row: int, title: str, df: pd.DataFrame, max_cols: int = 8):
    _section_header(ws, start_row, 1, max_cols, title)
    header_row = start_row + 1
    if df is None or df.empty:
        ws.cell(header_row, 1, "No data")
        return header_row + 2

    headers = list(df.columns)[:max_cols]
    _table_header(ws, header_row, 1, headers)
    for ridx, row in enumerate(df[headers].itertuples(index=False), start=header_row + 1):
        for cidx, val in enumerate(row, start=1):
            ws.cell(ridx, cidx, _as_number(val))
    _apply_thin_borders(ws, header_row, header_row + len(df), 1, len(headers))
    return header_row + len(df) + 2


def _build_location_sheet(
    ws,
    location: str,
    report_label: str,
    previous_label: str,
    locations: pd.DataFrame,
    categories: pd.DataFrame,
    activities: pd.DataFrame,
    registrations: pd.DataFrame,
    demographics,
    assessment_activity: pd.DataFrame,
    outcomes: pd.DataFrame,
    quality: pd.DataFrame,
):
    _style_title(
        ws,
        f"{location} – Monthly Performance",
        f"{report_label} performance compared with {previous_label}",
        end_col=8,
    )

    loc = locations[locations["Location"].eq(location)].copy() if locations is not None and not locations.empty else pd.DataFrame()

    _section_header(ws, 4, 1, 2, "Headline metrics")
    _section_header(ws, 4, 4, 8, "Month-on-month comparison")

    if not loc.empty:
        r = loc.iloc[0]
        headline = [
            ("Sessions delivered", r.get("Current Sessions", 0)),
            ("Attendance", r.get("Current Attendance", 0)),
            ("Unique participants", r.get("Current Unique Participants", 0)),
        ]
    else:
        headline = [
            ("Sessions delivered", 0),
            ("Attendance", 0),
            ("Unique participants", 0),
        ]

    reg = registrations[registrations["Location"].eq(location)].copy() if registrations is not None and not registrations.empty else pd.DataFrame()
    cur_reg = int(reg.iloc[0].get("Current Registrations", 0)) if not reg.empty else 0
    headline.append(("New registrations", cur_reg))

    ass = assessment_activity[assessment_activity["Location"].eq(location)].copy() if assessment_activity is not None and not assessment_activity.empty else pd.DataFrame()
    cur_ass = int(ass.iloc[0].get("Current Assessments", 0)) if not ass.empty else 0
    cur_follow = int(ass.iloc[0].get("Current Follow-ups", 0)) if not ass.empty else 0
    headline.append(("Health assessments", cur_ass))
    headline.append(("Follow-up assessments", cur_follow))

    for i, (name, val) in enumerate(headline, start=5):
        ws.cell(i, 1, name)
        ws.cell(i, 2, _as_number(val))
        ws.cell(i, 1).fill = PatternFill("solid", fgColor=MAGENTA_LIGHT)
        ws.cell(i, 2).fill = PatternFill("solid", fgColor=MAGENTA_LIGHT)
        ws.cell(i, 2).font = Font(color=MAGENTA, bold=True)

    _table_header(ws, 5, 4, ["Measure", previous_label, report_label, "Difference", "Trend"])

    compare_rows = []
    if not loc.empty:
        lr = loc.iloc[0]
        compare_rows.extend([
            ("Sessions", lr.get("Previous Sessions", 0), lr.get("Current Sessions", 0)),
            ("Attendance", lr.get("Previous Attendance", 0), lr.get("Current Attendance", 0)),
            ("Unique participants", lr.get("Previous Unique Participants", 0), lr.get("Current Unique Participants", 0)),
        ])
    else:
        compare_rows.extend([("Sessions", 0, 0), ("Attendance", 0, 0), ("Unique participants", 0, 0)])

    prev_reg = int(reg.iloc[0].get("Previous Registrations", 0)) if not reg.empty else 0
    compare_rows.append(("New registrations", prev_reg, cur_reg))
    prev_ass = int(ass.iloc[0].get("Previous Assessments", 0)) if not ass.empty else 0
    compare_rows.append(("Health assessments", prev_ass, cur_ass))
    prev_follow = int(ass.iloc[0].get("Previous Follow-ups", 0)) if not ass.empty else 0
    compare_rows.append(("Follow-up assessments", prev_follow, cur_follow))

    for ridx, (measure, prev, curr) in enumerate(compare_rows, start=6):
        diff = curr - prev
        trend = "UP" if diff > 0 else "DOWN" if diff < 0 else "STABLE"
        vals = [measure, prev, curr, diff, trend]
        for cidx, val in enumerate(vals, start=4):
            ws.cell(ridx, cidx, _as_number(val))
        _trend_fill(ws.cell(ridx, 8), trend)
    _apply_thin_borders(ws, 5, 11, 4, 8)

    next_row = 14

    cat = categories[categories["Location"].eq(location)].copy() if categories is not None and not categories.empty else pd.DataFrame()
    if not cat.empty:
        cat = cat.rename(columns={
            "ActivityCategoryResolved": "Activity Category",
            "CategoryResolved": "Category",
            "SubCategoryResolved": "Subcategory",
        })
        wanted = [
            "Activity Category", "Category", "Subcategory",
            "Previous Sessions", "Current Sessions",
            "Previous Attendance", "Current Attendance",
            "Previous UniqueParticipants", "Current UniqueParticipants",
            "Attendance Change", "Trend",
        ]
        wanted = [c for c in wanted if c in cat.columns]
        cat = cat[wanted]
    next_row = _write_small_table(ws, next_row, "Category performance", cat, max_cols=11)

    act = activities[activities["Location"].eq(location)].copy() if activities is not None and not activities.empty else pd.DataFrame()
    if not act.empty:
        act = act.rename(columns={
            "ActivityCategoryResolved": "Category",
            "UniqueParticipants": "Unique participants",
        })
        wanted = [
            "Category", "ActivityName", "Sessions", "Attendance",
            "Unique participants", "Average Attendance / Session",
        ]
        wanted = [c for c in wanted if c in act.columns]
        act = act[wanted].sort_values("Attendance", ascending=False)
    next_row = _write_small_table(ws, next_row, "Top activities", act, max_cols=8)

    out = outcomes[outcomes["Location"].eq(location)].copy() if outcomes is not None and not outcomes.empty and "Location" in outcomes.columns else pd.DataFrame()
    next_row = _write_small_table(ws, next_row, "Outcomes", out, max_cols=10)

    q = quality[quality["Location"].eq(location)].copy() if quality is not None and not quality.empty else pd.DataFrame()
    next_row = _write_small_table(ws, next_row, "Data quality", q, max_cols=10)

    gender, ethnicity, age, disability = demographics
    demo_frames = []
    for label, df, col in [
        ("Gender", gender, "Gender"),
        ("Age band", age, "Age Band"),
        ("Ethnicity", ethnicity, "Ethnicity"),
        ("Disability / health condition", disability, "Disability"),
    ]:
        if df is None or df.empty:
            continue
        x = df[df["Location"].eq(location)].copy()
        if x.empty:
            continue
        category_col = [c for c in x.columns if c not in {"Location", "Participants"}]
        if not category_col:
            continue
        x = x[[category_col[0], "Participants"]].copy()
        x.insert(0, "Breakdown", label)
        x = x.rename(columns={category_col[0]: "Category"})
        demo_frames.append(x)
    demo = pd.concat(demo_frames, ignore_index=True) if demo_frames else pd.DataFrame()
    _write_small_table(ws, next_row, "Demographics", demo, max_cols=5)

    ws.freeze_panes = "A5"
    _autowidth(ws)
    ws.column_dimensions["A"].width = 28


def write_single_workbook(
    path: Path,
    report_label: str,
    previous_label: str,
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
    """
    Create ONE Excel workbook containing:
      SUMMARY
      DEMOGRAPHICS
      TOP ACTIVITIES
      REGISTRATION INSIGHTS
      one sheet for every location
    """
    path.parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        # Create placeholder sheets so we can style them after the writer closes.
        pd.DataFrame().to_excel(writer, sheet_name="SUMMARY", index=False)
        pd.DataFrame().to_excel(writer, sheet_name="DEMOGRAPHICS", index=False)
        pd.DataFrame().to_excel(writer, sheet_name="TOP ACTIVITIES", index=False)
        pd.DataFrame().to_excel(writer, sheet_name="REGISTRATION INSIGHTS", index=False)

        existing = {"SUMMARY", "DEMOGRAPHICS", "TOP ACTIVITIES", "REGISTRATION INSIGHTS"}
        location_sheet_map = {}
        all_locations = []
        if locations is not None and not locations.empty:
            all_locations = locations["Location"].dropna().astype(str).tolist()
        for location in all_locations:
            sheet_name = _safe_sheet_name(location, existing)
            location_sheet_map[location] = sheet_name
            pd.DataFrame().to_excel(writer, sheet_name=sheet_name, index=False)

    wb = load_workbook(path)

    _build_summary_sheet(
        wb["SUMMARY"], overall, locations, report_label, previous_label
    )
    _build_demographics_sheet(wb["DEMOGRAPHICS"], demographics, report_label)
    _build_top_activities_sheet(wb["TOP ACTIVITIES"], activities, report_label)
    _build_registration_sheet(
        wb["REGISTRATION INSIGHTS"], registrations, quality, report_label, previous_label
    )

    for location, sheet_name in location_sheet_map.items():
        _build_location_sheet(
            wb[sheet_name],
            location,
            report_label,
            previous_label,
            locations,
            categories,
            activities,
            registrations,
            demographics,
            assessment_activity,
            outcomes,
            quality,
        )

    # Clean any blank A1 values left by pandas placeholders.
    for ws in wb.worksheets:
        if ws["A1"].value is None and ws.max_row == 1 and ws.max_column == 1:
            ws.delete_rows(1, 1)

    wb.save(path)
    return location_sheet_map
