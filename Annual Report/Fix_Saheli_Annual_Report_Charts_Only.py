from pathlib import Path

def apply_saheli_branding(input_path, output_path):
    """
    Clean, presentation-first Saheli report layout.

    Design rule: fewer, larger charts; no overlapping pie labels; long category names
    are shown inside bar-chart data labels so they remain readable in Excel/PDF.
    Data values are NOT changed here.
    """
    from pathlib import Path
    from openpyxl import load_workbook
    from openpyxl.chart import BarChart, DoughnutChart, Reference
    from openpyxl.chart.label import DataLabelList
    from openpyxl.chart.marker import DataPoint
    from openpyxl.styles import Alignment, Border, Font, PatternFill, Side

    MAGENTA = "C2185B"
    PINK = "E91E63"
    PALE_PINK = "FCE4EC"
    YELLOW = "FFC107"
    ORANGE = "F59E0B"
    DARK = "2B2B2B"
    GREY = "6B7280"
    LIGHT_GREY = "F3F4F6"
    WHITE = "FFFFFF"
    BLUE = "1F4E78"
    THIN = Side(style="thin", color="D1D5DB")

    wb = load_workbook(input_path)

    def title_band(ws, title, subtitle, end_col=16):
        # Avoid double-merging if the function is re-run.
        for merged in list(ws.merged_cells.ranges):
            if merged.min_row <= 2:
                ws.unmerge_cells(str(merged))
        ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=end_col)
        ws["A1"] = f"SAHELI HUB  |  {title}"
        ws["A1"].font = Font(name="Aptos Display", size=20, bold=True, color=WHITE)
        ws["A1"].fill = PatternFill("solid", fgColor=MAGENTA)
        ws["A1"].alignment = Alignment(vertical="center")
        ws.row_dimensions[1].height = 34
        ws.merge_cells(start_row=2, start_column=1, end_row=2, end_column=end_col)
        ws["A2"] = subtitle
        ws["A2"].font = Font(name="Aptos", size=10, italic=True, color=DARK)
        ws["A2"].fill = PatternFill("solid", fgColor=YELLOW)
        ws["A2"].alignment = Alignment(vertical="center")
        ws.row_dimensions[2].height = 22
        ws.sheet_view.showGridLines = False

    def style_header(ws, row, start_col, end_col):
        for col in range(start_col, end_col + 1):
            cell = ws.cell(row, col)
            cell.font = Font(name="Aptos", bold=True, color=WHITE)
            cell.fill = PatternFill("solid", fgColor=MAGENTA)
            cell.alignment = Alignment(vertical="center", wrap_text=True)
            cell.border = Border(bottom=Side(style="medium", color=YELLOW))
        ws.row_dimensions[row].height = 28

    def section(ws, row, text, end_col):
        ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=end_col)
        c = ws.cell(row, 1)
        c.value = text
        c.fill = PatternFill("solid", fgColor=MAGENTA)
        c.font = Font(name="Aptos Display", size=12, bold=True, color=WHITE)
        c.alignment = Alignment(vertical="center")
        ws.row_dimensions[row].height = 24

    def make_bar_chart(ws, data_col, cat_col, header_row, first_data_row, last_data_row,
                       title, anchor, width=17.5, height=8.5,
                       horizontal=True, percent=False, show_cat_in_labels=True):
        chart = BarChart()
        chart.type = "bar" if horizontal else "col"
        chart.style = 10
        chart.width = width
        chart.height = height
        chart.title = title
        chart.legend = None
        chart.add_data(
            Reference(ws, min_col=data_col, min_row=header_row, max_row=last_data_row),
            titles_from_data=True,
        )
        chart.set_categories(
            Reference(ws, min_col=cat_col, min_row=first_data_row, max_row=last_data_row)
        )
        try:
            chart.gapWidth = 45 if horizontal else 65
        except Exception:
            pass
        if chart.series:
            chart.series[0].graphicalProperties.solidFill = MAGENTA
            chart.series[0].graphicalProperties.line.solidFill = MAGENTA
        chart.dLbls = DataLabelList()
        chart.dLbls.showVal = True
        chart.dLbls.showCatName = bool(show_cat_in_labels)
        chart.dLbls.separator = "  "
        if percent:
            chart.dLbls.numFmt = "0.0%"
        else:
            chart.dLbls.numFmt = "#,##0"
        try:
            chart.x_axis.delete = False
            chart.y_axis.delete = False
            chart.x_axis.tickLblPos = "nextTo"
            chart.y_axis.tickLblPos = "nextTo"
            chart.x_axis.majorGridlines = None
        except Exception:
            pass
        ws.add_chart(chart, anchor)
        return chart

    def make_donut(ws, value_col, cat_col, header_row, first_data_row, last_data_row,
                   title, anchor, width=10.5, height=6.8, colors=None):
        chart = DoughnutChart()
        chart.style = 10
        chart.width = width
        chart.height = height
        chart.title = title
        chart.holeSize = 62
        chart.add_data(
            Reference(ws, min_col=value_col, min_row=header_row, max_row=last_data_row),
            titles_from_data=True,
        )
        chart.set_categories(
            Reference(ws, min_col=cat_col, min_row=first_data_row, max_row=last_data_row)
        )
        # IMPORTANT: no slice labels. They were the main source of unreadable overlap.
        chart.dLbls = None
        if chart.legend is not None:
            chart.legend.position = "r"
        if colors and chart.series:
            points = []
            for idx, color in enumerate(colors):
                p = DataPoint(idx=idx)
                p.graphicalProperties.solidFill = color
                p.graphicalProperties.line.solidFill = WHITE
                points.append(p)
            chart.series[0].dPt = points
        ws.add_chart(chart, anchor)
        return chart

    def set_report_columns(ws, widths):
        for col, width in widths.items():
            ws.column_dimensions[col].width = width

    def add_kpi_card(ws, label, value, start_col, start_row, span=3):
        ws.merge_cells(start_row=start_row, start_column=start_col,
                       end_row=start_row, end_column=start_col + span - 1)
        ws.merge_cells(start_row=start_row + 1, start_column=start_col,
                       end_row=start_row + 2, end_column=start_col + span - 1)
        l = ws.cell(start_row, start_col)
        v = ws.cell(start_row + 1, start_col)
        l.value = label
        v.value = value
        l.fill = PatternFill("solid", fgColor=MAGENTA)
        l.font = Font(name="Aptos", bold=True, color=WHITE, size=10)
        l.alignment = Alignment(horizontal="center", vertical="center")
        v.fill = PatternFill("solid", fgColor=PALE_PINK)
        v.font = Font(name="Aptos Display", bold=True, color=MAGENTA, size=24)
        v.number_format = "#,##0"
        v.alignment = Alignment(horizontal="center", vertical="center")
        for rr in range(start_row, start_row + 3):
            for cc in range(start_col, start_col + span):
                ws.cell(rr, cc).border = Border(left=THIN, right=THIN, top=THIN, bottom=THIN)

    # ------------------------------------------------------------------
    # SUMMARY — simple dashboard, not chart-heavy
    # ------------------------------------------------------------------
    ws = wb["SUMMARY"]
    source_values = {ws.cell(r, 1).value: ws.cell(r, 2).value for r in range(2, ws.max_row + 1)}
    ws.delete_rows(1, ws.max_row)
    title_band(ws, "ANNUAL REPORT 2025/26",
               "Verified delivery, reach and registration overview  |  1 April 2025 – 31 March 2026", 16)

    cards = [
        ("Total verified attendance", source_values.get("Total verified annual attendance", 0)),
        ("CRM attendance", source_values.get("CRM attendance portion", 0)),
        ("Verified source-only", source_values.get("Verified source-only additional attendance", 0)),
        ("Registered participants", source_values.get("REGISTERED PARTICIPANTS", 0)),
        ("New FULL registrations", source_values.get("New FULL registrations", 0)),
        ("Sessions delivered", source_values.get("Sessions delivered", 0)),
    ]
    for (label, value), (col, row) in zip(cards, [(1,4),(6,4),(11,4),(1,8),(6,8),(11,8)]):
        add_kpi_card(ws, label, value, col, row, span=4)

    section(ws, 13, "YEAR-ON-YEAR HEADLINES", 5)
    rows = [
        ["Metric", "2025/26", "Previous published", "Difference", "Status"],
        ["Attendance", cards[0][1], 21777, cards[0][1] - 21777, "LOWER"],
        ["Registered participants*", cards[3][1], 1897, cards[3][1] - 1897, "HIGHER"],
        ["New registrations", cards[4][1], 598, cards[4][1] - 598, "HIGHER"],
    ]
    for r, row in enumerate(rows, 14):
        for c, val in enumerate(row, 1):
            ws.cell(r, c, val)
    style_header(ws, 14, 1, 5)
    for r in range(15, 18):
        ws.cell(r, 2).number_format = "#,##0"
        ws.cell(r, 3).number_format = "#,##0"
        ws.cell(r, 4).number_format = "+#,##0;-#,##0;0"
        if ws.cell(r, 5).value == "HIGHER":
            ws.cell(r, 5).fill = PatternFill("solid", fgColor="E8F5E9")
        else:
            ws.cell(r, 5).fill = PatternFill("solid", fgColor="FFF3E0")
        ws.cell(r, 5).font = Font(bold=True, color=DARK)
    ws.merge_cells("A19:E20")
    ws["A19"] = "*Registered participants are current FULL + Lite CRM profiles; the previous published participant definition may differ."
    ws["A19"].font = Font(size=9, italic=True, color=GREY)
    ws["A19"].alignment = Alignment(wrap_text=True, vertical="top")

    # Attendance comparison only — avoids mixing 21k, 2.8k and 611 on one scale.
    ws["G14"] = "Period"
    ws["H14"] = "Attendance"
    ws["G15"] = "2025/26"
    ws["H15"] = cards[0][1]
    ws["G16"] = "Previous published"
    ws["H16"] = 21777
    style_header(ws, 14, 7, 8)
    comp = make_bar_chart(ws, 8, 7, 14, 15, 16,
                          "Attendance: current vs previous published",
                          "J13", width=10.5, height=6.0,
                          horizontal=False, show_cat_in_labels=False)
    if len(comp.series) > 0:
        comp.series[0].graphicalProperties.solidFill = MAGENTA

    section(ws, 23, "HOW VERIFIED ATTENDANCE IS CONSTRUCTED", 5)
    evidence = [
        ["Evidence", "Attendances"],
        ["CRM attendance", cards[1][1]],
        ["Verified source-only", cards[2][1]],
    ]
    for r, row in enumerate(evidence, 24):
        for c, val in enumerate(row, 1):
            ws.cell(r, c, val)
    style_header(ws, 24, 1, 2)
    make_donut(ws, 2, 1, 24, 25, 26,
               "Attendance evidence", "G22", width=9.5, height=6.5,
               colors=[MAGENTA, YELLOW])
    ws.merge_cells("A29:P30")
    ws["A29"] = (
        "Bellboat: 114 documented working participation records are reported separately and are not added to "
        "the organisation-wide verified attendance total."
    )
    ws["A29"].fill = PatternFill("solid", fgColor=YELLOW)
    ws["A29"].font = Font(bold=True, color=DARK)
    ws["A29"].alignment = Alignment(wrap_text=True, vertical="center")
    set_report_columns(ws, {"A":28,"B":15,"C":18,"D":15,"E":14,"F":3,"G":20,"H":16,"I":3,"J":16,"K":16,"L":16,"M":16,"N":16,"O":16,"P":16})
    ws.freeze_panes = "A4"

    # ------------------------------------------------------------------
    # DEMOGRAPHICS
    # ------------------------------------------------------------------
    ws = wb["DEMOGRAPHICS"]
    # Remove existing charts only; preserve data tables.
    ws._charts = []
    ws.insert_rows(1, 4)
    title_band(ws, "DEMOGRAPHICS", "Canonical annual attendees; missing values remain explicitly recorded", 16)

    # Gender table is now rows 6:10 after insertion.
    section(ws, 4, "GENDER", 4)
    style_header(ws, 6, 1, 3)
    make_donut(ws, 2, 1, 6, 7, 10,
               "Gender of annual attendees", "F4", width=10.5, height=6.8,
               colors=[MAGENTA, YELLOW, ORANGE, "BDBDBD"])
    # Clear headline card instead of overlapping donut labels.
    ws.merge_cells("A13:D14")
    ws["A13"] = "Recorded Female / Male: 81.2% Female  |  18.8% Male"
    ws["A13"].fill = PatternFill("solid", fgColor=PALE_PINK)
    ws["A13"].font = Font(name="Aptos Display", size=15, bold=True, color=MAGENTA)
    ws["A13"].alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

    # Find age table header after insertion.
    age_header = None
    for row in ws.iter_rows(min_row=1, max_row=min(ws.max_row, 80)):
        for cell in row:
            if cell.value == "Age Band":
                age_header = cell.row
                break
        if age_header:
            break
    if age_header:
        section(ws, age_header - 1, "AGE PROFILE", 4)
        style_header(ws, age_header, 1, 4)
        age_end = age_header
        while age_end + 1 <= ws.max_row and ws.cell(age_end + 1, 1).value not in (None, ""):
            age_end += 1
        # Exclude Unknown from chart; it stays visible in the table.
        chart_end = age_end - 1 if str(ws.cell(age_end,1).value).strip().lower() == "unknown" else age_end
        age_chart = make_bar_chart(ws, 4, 1, age_header, age_header + 1, chart_end,
                                   "Age profile (% of participants with valid DOB)",
                                   f"F{age_header - 1}", width=11.5, height=7.0,
                                   horizontal=False, percent=True, show_cat_in_labels=False)
        try:
            age_chart.y_axis.numFmt = "0%"
            age_chart.y_axis.scaling.min = 0
        except Exception:
            pass

    # Ethnicity: use cleaned group table if present; show a simple readable bar chart.
    eth_header = None
    for row in ws.iter_rows(min_row=1, max_row=ws.max_row):
        for cell in row:
            if cell.value == "Ethnicity Group":
                eth_header = cell.row
                break
        if eth_header:
            break
    if eth_header:
        section(ws, eth_header - 1, "CLEAN ETHNICITY", 4)
        style_header(ws, eth_header, 1, 3)
        eth_end = eth_header
        while eth_end + 1 <= ws.max_row and ws.cell(eth_end + 1, 1).value not in (None, ""):
            eth_end += 1
        # Build helper list of usable ethnicity groups only.
        helper_col_cat = 10
        helper_col_val = 11
        ws.cell(eth_header, helper_col_cat, "Ethnicity")
        ws.cell(eth_header, helper_col_val, "Participants")
        hrow = eth_header + 1
        for rr in range(eth_header + 1, eth_end + 1):
            cat = ws.cell(rr, 1).value
            val = ws.cell(rr, 2).value
            if cat in (None, "", "Not recorded", "Unclear / review"):
                continue
            ws.cell(hrow, helper_col_cat, cat)
            ws.cell(hrow, helper_col_val, val)
            hrow += 1
        if hrow > eth_header + 1:
            make_bar_chart(ws, helper_col_val, helper_col_cat, eth_header,
                           eth_header + 1, hrow - 1,
                           "Ethnicity of participants with usable records",
                           f"F{eth_header - 1}", width=12.0, height=7.5,
                           horizontal=True, show_cat_in_labels=True)
        ws.column_dimensions["J"].hidden = True
        ws.column_dimensions["K"].hidden = True

    set_report_columns(ws, {"A":38,"B":16,"C":20,"D":28,"E":3,"F":16,"G":16,"H":16,"I":16})
    ws.freeze_panes = "A6"

    # ------------------------------------------------------------------
    # TOP ACTIVITIES — full-width chart below table
    # ------------------------------------------------------------------
    ws = wb["TOP ACTIVITIES"]
    ws._charts = []
    ws.insert_rows(1, 4)
    title_band(ws, "TOP ACTIVITIES", "Top 10 activities from the combined verified-delivery ledger", 16)
    style_header(ws, 5, 1, 4)
    ws.merge_cells("A17:D18")
    ws["A17"] = "The chart below uses the same verified attendance figures shown in the table."
    ws["A17"].fill = PatternFill("solid", fgColor=LIGHT_GREY)
    ws["A17"].font = Font(italic=True, color=GREY)
    ws["A17"].alignment = Alignment(wrap_text=True, vertical="center")
    make_bar_chart(ws, 2, 1, 5, 6, 15,
                   "Top 10 activities by verified attendance",
                   "A20", width=20.5, height=10.5,
                   horizontal=True, show_cat_in_labels=True)
    set_report_columns(ws, {"A":42,"B":16,"C":14,"D":26,"E":3,"F":16,"G":16,"H":16,"I":16,"J":16,"K":16,"L":16,"M":16,"N":16,"O":16,"P":16})
    ws.freeze_panes = "A6"

    # ------------------------------------------------------------------
    # REGISTRATION INSIGHTS — chart only top 8 reasons; no donut labels
    # ------------------------------------------------------------------
    ws = wb["REGISTRATION INSIGHTS"]
    ws._charts = []
    ws.insert_rows(1, 4)
    title_band(ws, "REGISTRATION INSIGHTS", "Why new FULL members joined and how they heard about Saheli", 16)
    section(ws, 4, "TOP REASONS PEOPLE JOIN", 4)
    style_header(ws, 6, 1, 3)

    heard_header = None
    for row in ws.iter_rows(min_row=7, max_row=ws.max_row):
        if ws.cell(row[0].row, 1).value == "Source":
            heard_header = row[0].row
            break
    if heard_header is None:
        heard_header = ws.max_row + 1

    reason_end = heard_header - 2
    # Helper area contains only the top 8 rows, which avoids tiny 0.2% bars.
    helper_row = 6
    ws["J6"] = "Reason"
    ws["K6"] = "Percentage"
    write_row = 7
    for rr in range(7, min(reason_end, 14) + 1):
        ws.cell(write_row, 10, ws.cell(rr, 1).value)
        ws.cell(write_row, 11, ws.cell(rr, 3).value)
        write_row += 1
    if write_row > 7:
        make_bar_chart(ws, 11, 10, 6, 7, write_row - 1,
                       "Top reasons people join",
                       "E5", width=13.5, height=8.2,
                       horizontal=True, percent=True, show_cat_in_labels=True)
    ws.column_dimensions["J"].hidden = True
    ws.column_dimensions["K"].hidden = True

    if heard_header <= ws.max_row:
        section(ws, heard_header - 1, "HOW PEOPLE HEARD ABOUT SAHELI", 4)
        style_header(ws, heard_header, 1, 3)
        heard_end = heard_header
        while heard_end + 1 <= ws.max_row and ws.cell(heard_end + 1, 1).value not in (None, ""):
            heard_end += 1
        make_donut(ws, 2, 1, heard_header, heard_header + 1, heard_end,
                   "How people heard about Saheli",
                   f"E{heard_header - 1}", width=10.5, height=6.8,
                   colors=[MAGENTA, PINK, YELLOW, ORANGE, "8E24AA", "6D4C41", "BDBDBD"])

    set_report_columns(ws, {"A":44,"B":16,"C":26,"D":3,"E":16,"F":16,"G":16,"H":16,"I":16})
    ws.freeze_panes = "A6"

    # ------------------------------------------------------------------
    # BELLBOAT — clear 2x2 chart grid
    # ------------------------------------------------------------------
    if "BELLBOAT" in wb.sheetnames:
        del wb["BELLBOAT"]
    ws = wb.create_sheet("BELLBOAT", 4)
    title_band(ws, "BELLBOAT REPORT", "Documented working participation — data available from current sources", 16)

    bell_kpis = [
        ("Working participation", 114),
        ("Unique named participants", 83),
        ("Sessions / events", 11),
        ("Unique detailed profiles", 52),
    ]
    for (label, value), col in zip(bell_kpis, (1,5,9,13)):
        add_kpi_card(ws, label, value, col, 4, span=3)

    ws.merge_cells("A8:P10")
    ws["A8"] = (
        "Bellboat working participation is based on documented delivery records. The source file does not contain a separate "
        "Attended Yes/No field, so the 114 participation figure remains a documented working participation total."
    )
    ws["A8"].fill = PatternFill("solid", fgColor=YELLOW)
    ws["A8"].font = Font(bold=True, color=DARK)
    ws["A8"].alignment = Alignment(wrap_text=True, vertical="center")

    monthly = [("Month","Participation"),("May 2025",46),("Jun 2025",8),("Jul 2025",10),("Sep 2025",50)]
    delivery = [("Delivery type","Participation","Sessions"),("Bellboating",73,9),("Bellboating & Kayaking Combined",36,1),("Kayaking",5,1)]
    gender = [("Gender","Profiles"),("Female",46),("Male",6)]
    age = [("Age band","Profiles"),("Under 16",15),("16-25",6),("26-35",4),("36-45",5),("46-55",8),("56-65",2),("66-75",1),("Unknown",11)]

    # Put source tables in compact helper blocks under each chart.
    for start_row, rows in ((13,monthly),(13,delivery),(36,gender),(36,age)):
        pass
    for r_off, row in enumerate(monthly):
        for c_off, v in enumerate(row): ws.cell(13+r_off, 1+c_off, v)
    style_header(ws, 13, 1, 2)
    for r_off, row in enumerate(delivery):
        for c_off, v in enumerate(row): ws.cell(13+r_off, 9+c_off, v)
    style_header(ws, 13, 9, 11)
    for r_off, row in enumerate(gender):
        for c_off, v in enumerate(row): ws.cell(36+r_off, 1+c_off, v)
    style_header(ws, 36, 1, 2)
    for r_off, row in enumerate(age):
        for c_off, v in enumerate(row): ws.cell(36+r_off, 9+c_off, v)
    style_header(ws, 36, 9, 10)

    make_bar_chart(ws, 2, 1, 13, 14, 17,
                   "Bellboat participation by month", "A19",
                   width=10.8, height=6.8, horizontal=False, show_cat_in_labels=False)
    make_bar_chart(ws, 10, 9, 13, 14, 16,
                   "Bellboat and kayaking delivery", "I19",
                   width=10.8, height=6.8, horizontal=True, show_cat_in_labels=True)
    make_donut(ws, 2, 1, 36, 37, 38,
               "Bellboat profile gender", "A42",
               width=9.8, height=6.3, colors=[MAGENTA, YELLOW])
    make_bar_chart(ws, 10, 9, 36, 37, 44,
                   "Bellboat age profile", "I42",
                   width=10.8, height=6.8, horizontal=False, show_cat_in_labels=False)

    ws.merge_cells("A60:P61")
    ws["A60"] = (
        "Bellboat 114 is reported separately and is not included in the organisation-wide verified attendance total because "
        "source-only reconciliation is not proven."
    )
    ws["A60"].font = Font(italic=True, color=GREY)
    ws["A60"].alignment = Alignment(wrap_text=True)
    set_report_columns(ws, {chr(64+i):16 for i in range(1,17)})
    ws.column_dimensions["A"].width = 30
    ws.column_dimensions["I"].width = 36
    ws.freeze_panes = "A4"

    # ------------------------------------------------------------------
    # Tidy audit/raw sheets; no decorative charts.
    # ------------------------------------------------------------------
    for sheet_name in ("DELIVERY SOURCE AUDIT", "RAW VERIFIED DELIVERY", "RAW PARTICIPANTS"):
        if sheet_name not in wb.sheetnames:
            continue
        ws = wb[sheet_name]
        ws.sheet_view.showGridLines = False
        ws.freeze_panes = "A2"
        for cell in ws[1]:
            cell.fill = PatternFill("solid", fgColor=BLUE)
            cell.font = Font(bold=True, color=WHITE)
            cell.alignment = Alignment(wrap_text=True, vertical="center")
        ws.row_dimensions[1].height = 28

    # Print-friendly report sheets.
    for sheet_name in ("SUMMARY", "DEMOGRAPHICS", "TOP ACTIVITIES", "REGISTRATION INSIGHTS", "BELLBOAT"):
        if sheet_name not in wb.sheetnames:
            continue
        ws = wb[sheet_name]
        ws.sheet_properties.pageSetUpPr.fitToPage = True
        ws.page_setup.fitToWidth = 1
        ws.page_setup.fitToHeight = 0
        ws.page_margins.left = 0.25
        ws.page_margins.right = 0.25
        ws.page_margins.top = 0.35
        ws.page_margins.bottom = 0.35

    wb.save(output_path)


if __name__ == "__main__":
    base = Path(__file__).resolve().parent
    source = base / "Saheli_Hub_Annual_Report_2025_26_COMPLETE.xlsx"
    output = base / "Saheli_Hub_Annual_Report_2025_26_COMPLETE_BRANDED_CLEAN.xlsx"
    if not source.exists():
        raise SystemExit(f"Source workbook not found: {source}")
    print(f"Rebuilding charts from: {source}")
    apply_saheli_branding(source, output)
    print(f"DONE: {output}")
