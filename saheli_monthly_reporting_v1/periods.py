\
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import calendar


@dataclass(frozen=True)
class ReportPeriod:
    report_start: date
    report_end_exclusive: date
    previous_start: date
    previous_end_exclusive: date

    @property
    def report_label(self) -> str:
        return self.report_start.strftime("%B %Y")

    @property
    def previous_label(self) -> str:
        return self.previous_start.strftime("%B %Y")

    @property
    def folder_label(self) -> str:
        return self.report_start.strftime("%Y-%m")


def _month_start(year: int, month: int) -> date:
    return date(year, month, 1)


def _shift_month(d: date, delta: int) -> date:
    month_index = d.year * 12 + (d.month - 1) + delta
    year, month_zero = divmod(month_index, 12)
    return date(year, month_zero + 1, 1)


def get_period(report_month: str | None = None) -> ReportPeriod:
    if report_month:
        year_text, month_text = report_month.split("-", 1)
        report_start = _month_start(int(year_text), int(month_text))
    else:
        today = date.today()
        this_month_start = date(today.year, today.month, 1)
        report_start = _shift_month(this_month_start, -1)

    report_end = _shift_month(report_start, 1)
    previous_start = _shift_month(report_start, -1)

    return ReportPeriod(
        report_start=report_start,
        report_end_exclusive=report_end,
        previous_start=previous_start,
        previous_end_exclusive=report_start,
    )
