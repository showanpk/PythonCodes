"""Safe offline tests using mock Excel and mock SQL; no production DB access."""
import sys
import unittest
from datetime import date, time
from collections import defaultdict
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent))
import sync_innerva_to_crm as app

HEAD = ["Spaces", "Lead", "Session Type", "Day", "date", "Month", "Induction Time",
        "Session", "Saheli Card Number", "Name", "Attended", "Signed Induction Paper",
        "Medical condition affecting use of machine", "Any Issues during session",
        "Date of birth", "Risk Stratification", "Age", "Age bracket", "CRM"]


def row(*cells):
    return tuple(cells) + (None,) * (len(HEAD) - len(cells))


def follow_row(position, card, name, attended, signed='Yes'):
    values = [None] * len(HEAD)
    values[0] = position
    values[8] = card
    values[9] = name
    values[10] = attended
    values[11] = signed
    return tuple(values)


class FakeSheet:
    def __init__(self, name, rows):
        self.title, self.data = name, rows
        self.max_row, self.max_column = len(rows), max(map(len, rows))

    def iter_rows(self, min_row, max_row=None, max_col=None, values_only=True):
        for r in self.data[min_row - 1:max_row]:
            yield tuple(r[:max_col])


class FakeBook:
    def __init__(self, sheets):
        self.worksheets = sheets

    def close(self):
        pass


class FakeCursor:
    def __init__(self):
        self.queries = []
        self.results = []
        self.fake_sid = 200

    def execute(self, sql, *args):
        self.queries.append(sql)
        if "INFORMATION_SCHEMA.COLUMNS" in sql:
            names = ("SessionId SessionDate VenueName ActivityName StartTime EndTime "
                     "IsBookingRequired IsCancelled Capacity Category SubCategory ActivityCategory "
                     "Frequency Notes IsRecurringWeekly DayOfWeek CreatedAtUtc "
                     "AttendanceMemberKind ParticipantId LiteMemberId MemberDisplayId "
                     "SaheliCardNumber MemberName SessionName SessionDay SessionMonth "
                     "SessionStartTime SessionEndTime Attended SignedInductionPaper "
                     "MedicalCondition RiskStratification ParticipantID FullName DateOfBirth "
                     "Site CreatedAt Id MembershipId FirstName LastName").split()
            self.results = [(s,) for s in names]
        elif "FROM dbo.Sessions" in sql:
            self.results = [(111, date(2026, 1, 12), app.VENUE, "Innerva",
                             time(11), time(12), False, False, 9)]
        elif "FROM dbo.SessionAttendance a" in sql:
            self.results = [(111, "FULL", 1, None, True)]
        elif "FROM dbo.Participants" in sql:
            self.results = [(1, "320", "Nasreen Akhtar", date(1971, 10, 10)),
                            (2, "152", "Jennifer Mason", date(1938, 1, 25)),
                            (3, "142", "Azra Bibi", date(1967, 2, 28))]
        elif "FROM dbo.LiteMembers" in sql:
            self.results = []
        else:
            self.results = []
        return self

    def __iter__(self):
        return iter(self.results)

    def fetchone(self):
        return (self.fake_sid,)


class SyncTests(unittest.TestCase):
    def args(self):
        return SimpleNamespace(start=None, end=date(2026, 10, 8), current_year=None,
                               create_missing_members=False, promote_attendance=False)

    def fixture(self):
        return FakeBook([FakeSheet("2026", [row(*HEAD),
            row(1, "Fozia", "Female", "Saturday", "10th", "January", "CANCELLED",
                "9:45-10:45", "192", "Fozia Ali", "", "Yes"),
            follow_row(2, "1322", "Rina Rahman", "Yes"),
            row(1, "Fozia", "Female", "Monday", "12th", "January", "NO INDUCTION",
                "11:00-12:00", "320", "Nasreen Akhtar", "Yes", "Yes", "", "",
                "10/10/1971", "High"),
            follow_row(2, "152", "Jennifer Mason", "Yes"),
            follow_row(3, "142", "Azra Bibi", 0),
        ])])

    def test_source_parser_and_cancel(self):
        rep = app.Report()
        with patch.object(app, 'load_workbook', return_value=self.fixture()):
            slots = app.parse_workbook(Path('nonexistent.xlsx'), self.args(), rep)
        self.assertEqual(2, len(slots))
        self.assertEqual(0, len(slots[0].people))
        self.assertTrue(slots[0].is_cancelled)
        self.assertFalse(slots[1].is_cancelled)
        self.assertEqual([True, True, False], [p.attended for p in slots[1].people])
        self.assertEqual("Female", slots[1].kind)

    def test_preview_is_select_only_and_existing_ids_reused(self):
        rep = app.Report()
        with patch.object(app, 'load_workbook', return_value=self.fixture()):
            slots = app.parse_workbook(Path('nonexistent.xlsx'), self.args(), rep)
        db = FakeCursor()
        app.sync(db, slots, rep, self.args(), False)
        self.assertEqual(1, rep.count['ENABLE_BOOKING'])
        self.assertEqual(2, rep.count['INSERT_BOOKING'])
        self.assertEqual(1, rep.count['SKIP_EXISTING_BOOKING'])
        self.assertEqual(1, rep.count['CREATE_SESSION'])  # cancelled slot
        self.assertTrue(all(q.strip().upper().startswith('SELECT') for q in db.queries))

    def test_mock_commit_writes_only_innerva_tables(self):
        rep = app.Report()
        with patch.object(app, 'load_workbook', return_value=self.fixture()):
            slots = app.parse_workbook(Path('nonexistent.xlsx'), self.args(), rep)
        db = FakeCursor()
        app.sync(db, slots, rep, self.args(), True)
        writes = [q.strip().upper() for q in db.queries if not q.strip().upper().startswith('SELECT')]
        self.assertEqual(1, sum('INSERT DBO.SESSIONS' in q for q in writes))
        self.assertEqual(2, sum('INSERT DBO.SESSIONATTENDANCE' in q for q in writes))
        self.assertEqual(1, sum('UPDATE DBO.SESSIONS' in q for q in writes))
        self.assertFalse(any('DELETE' in q for q in writes))
        self.assertFalse(any('UPDATE DBO.PARTICIPANTS' in q for q in writes))


    def test_existing_lite_member_can_book_even_when_full_name_also_exists(self):
        """Cardless booking reuses LITE identity, regardless of matching FULL name."""
        slot = app.Slot(date(2026, 1, 12), time(11), time(12), "Female", "Fozia",
                        None, False, "2026", 8)
        person = app.Member(9, "", "Example Name", date(1965, 1, 1), False,
                            "Low", None, "")
        existing_lite = {"id": "00000000-0000-0000-0000-000000000123",
                         "mid": "LITE-123", "name": "Example Name", "dob": date(1965, 1, 1)}
        db = {"full_names": {"example name": [{"id": 5, "name": "Example Name"}]},
              "lite": {"example name": [existing_lite]}}
        report = app.Report()
        resolved = app.resolve_member(FakeCursor(), person, db, slot, report,
                                      write=False, create_missing=False, lite_counter=[0])
        self.assertEqual(("LITE", existing_lite["id"], "LITE-123", "Example Name"), resolved)
        self.assertEqual(0, sum(v for k, v in report.count.items() if k.startswith("REVIEW_")))

    def test_existing_lite_missing_session_booking_creates_attendance_not_profile(self):
        slot = app.Slot(date(2026, 1, 12), time(11), time(12), "Female", "Fozia",
                        None, False, "2026", 8)
        slot.people = [app.Member(9, "", "Example Name", date(1965, 1, 1), False,
                                  "Low", None, "")]

        class LiteCursor(FakeCursor):
            def execute(self, sql, *args):
                ret = super().execute(sql, *args)
                if "FROM dbo.Participants" in sql:
                    self.results = [(5, "500", "Example Name", date(1965, 1, 1))]
                elif "FROM dbo.LiteMembers" in sql:
                    self.results = [("00000000-0000-0000-0000-000000000123", "LITE-123",
                                     "Example", "Name", date(1965, 1, 1))]
                return ret

        db = LiteCursor()
        rep = app.Report()
        app.sync(db, [slot], rep, self.args(), write=True)
        self.assertEqual(1, rep.count["INSERT_BOOKING"])
        self.assertEqual(0, rep.count["CREATE_LITE_MEMBER"])
        self.assertEqual(0, rep.count["CREATE_FULL_MEMBER"])
        writes = [q.upper() for q in db.queries if not q.upper().strip().startswith("SELECT")]
        self.assertEqual(1, sum("INSERT DBO.SESSIONATTENDANCE" in q for q in writes))
        self.assertFalse(any("INSERT DBO.LITEMEMBERS" in q for q in writes))

    def test_new_cardless_name_creates_lite_despite_matching_full(self):
        """Explicit rule: no Excel card + no Lite match -> new Lite."""
        slot = app.Slot(date(2026, 1, 12), time(11), time(12), "Female", "Fozia",
                        None, False, "2026", 8)
        person = app.Member(9, "", "Example Name", date(1965, 1, 1), False,
                            "Low", None, "")
        db = {"full_names": {"example name": [{"id": 5, "name": "Example Name"}]},
              "lite": defaultdict(list)}
        report = app.Report()
        result = app.resolve_member(FakeCursor(), person, db, slot, report,
                                    write=False, create_missing=False, lite_counter=[0])
        self.assertEqual("LITE", result[0])
        self.assertEqual("LITE-1", result[2])
        self.assertEqual(1, report.count["CREATE_LITE_MEMBER"])
        self.assertEqual(0, report.count["REVIEW_FULL_MEMBER_WITHOUT_CARD"])

    def test_cardless_lite_created_once_and_booked_twice(self):
        """Repeated Excel name creates one Lite profile, a booking per session."""
        s1 = app.Slot(date(2026, 1, 12), time(11), time(12), "Female", "Fozia",
                      None, False, "2026", 8)
        s2 = app.Slot(date(2026, 1, 13), time(11), time(12), "Female", "Fozia",
                      None, False, "2026", 18)
        for slot in (s1, s2):
            slot.people = [app.Member(slot.first_row + 1, "", "Example Name",
                                      date(1965, 1, 1), False, "Low", None, "")]

        class FullNameCursor(FakeCursor):
            def execute(self, sql, *params):
                ret = super().execute(sql, *params)
                if "FROM dbo.Participants" in sql:
                    self.results = [(5, "500", "Example Name", date(1965, 1, 1))]
                return ret

        for write in (False, True):
            db = FullNameCursor()
            report = app.Report()
            app.sync(db, [s1, s2], report, self.args(), write=write)
            self.assertEqual(1, report.count["CREATE_LITE_MEMBER"])
            self.assertEqual(2, report.count["INSERT_BOOKING"])
            writes = [q.upper() for q in db.queries
                      if not q.strip().upper().startswith("SELECT")]
            self.assertEqual(int(write), sum("INSERT DBO.LITEMEMBERS" in q for q in writes))
            self.assertEqual(2 * int(write), sum("INSERT DBO.SESSIONATTENDANCE" in q for q in writes))
            self.assertFalse(any("UPDATE DBO.PARTICIPANTS" in q for q in writes))

    def test_new_lite_not_created_if_session_full(self):
        slot = app.Slot(date(2026, 1, 12), time(11), time(12), "Female", "Fozia",
                        None, False, "2026", 8)
        person = app.Member(9, "", "Another Name", None, False, "Low", None, "")
        db = {"full_names": {}, "lite": defaultdict(list)}
        report = app.Report()
        result = app.resolve_member(FakeCursor(), person, db, slot, report,
                                    write=False, create_missing=False, lite_counter=[0],
                                    allow_new_lite=False)
        self.assertIsNone(result)
        self.assertEqual(0, report.count["CREATE_LITE_MEMBER"])
        self.assertEqual(1, report.count["REVIEW_SESSION_FULL"])

    def test_legacy_year_and_times(self):
        self.assertEqual(date(2024, 7, 15), app.slot_date('15th', None, 'July 24', None))
        self.assertEqual((time(9, 45), time(10, 45)), app.time_range('9:45-10:45'))
        self.assertTrue(app.cancelled('BANK HOLIDAY'))
        self.assertFalse(app.cancelled('CLOSED SESSION - High Risk Only'))
        self.assertFalse(app.cancelled('NO INDUCTION'))
        self.assertIsNone(app.attendance(''))
        self.assertFalse(app.attendance(0))
        self.assertTrue(app.attendance('Yes'))
        self.assertEqual('mens innerva', app.norm("Men's Innerva"))


if __name__ == '__main__':
    unittest.main()
