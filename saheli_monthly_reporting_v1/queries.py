\
from __future__ import annotations

import pandas as pd


def read_sql(conn, sql: str, params=()) -> pd.DataFrame:
    return pd.read_sql_query(sql, conn, params=params)


def load_sessions(conn, previous_start, report_end) -> pd.DataFrame:
    sql = """
    SELECT
        SessionId,
        Category,
        SubCategory,
        ActivityCategory,
        VenueName,
        ActivityName,
        SessionDate,
        StartTime,
        EndTime,
        IsCancelled
    FROM dbo.Sessions
    WHERE SessionDate >= ?
      AND SessionDate < ?;
    """
    return read_sql(conn, sql, (previous_start, report_end))


def load_attendance(conn, previous_start, report_end) -> pd.DataFrame:
    # The CRM reporting view is used to stay aligned with existing reporting logic.
    # Attendance totals still explicitly require Attended = 1 in the Python calculations.
    sql = """
    SELECT
        AttendanceId,
        SessionId,
        ParticipantId,
        LiteMemberId,
        AttendanceMemberKind,
        MemberDisplayId,
        SaheliCardNumber,
        ParticipantFullName,
        MemberName,
        SessionDate,
        VenueName,
        ActivityName,
        Attended
    FROM dbo.vw_Report_AttendanceFlat
    WHERE SessionDate >= ?
      AND SessionDate < ?;
    """
    return read_sql(conn, sql, (previous_start, report_end))


def load_registrations(conn, previous_start, report_end) -> pd.DataFrame:
    sql = """
    SELECT
        ParticipantId,
        SaheliCardNumber,
        FullName,
        Age,
        DateOfBirth,
        Gender,
        Ethnicity,
        Postcode,
        MobileNumber,
        HasHealthConditionOrDisability,
        Site,
        RegistrationDate,
        CreatedAt
    FROM Participants
    WHERE COALESCE(RegistrationDate, CONVERT(date, CreatedAt)) >= ?
      AND COALESCE(RegistrationDate, CONVERT(date, CreatedAt)) < ?;
    """
    return read_sql(conn, sql, (previous_start, report_end))


def load_assessments(conn, report_end) -> pd.DataFrame:
    # Full history up to the report end is needed to reproduce paired-outcome logic:
    # latest assessment in the report month versus the earliest valid earlier assessment.
    sql = """
    SELECT
        ParticipantID AS ParticipantId,
        SaheliCardNumber,
        FullName,
        Age,
        Gender,
        Postcode,
        MobileNumber,
        AssessmentID AS AssessmentId,
        AssessmentNumber,
        AssessmentDate,
        StaffMember,
        Site,
        CAST(Bmivalue AS decimal(18, 2)) AS BmiValue,
        CAST(WeightKg AS decimal(18, 2)) AS WeightKg,
        SystolicBp,
        DiastolicBp,
        WemwbsTotal,
        ConfidenceToJoin,
        FeelingConfident,
        Movement,
        FeelIsolated,
        ActiveDaysPerWeek
    FROM dbo.vw_Report_AssessmentOutcomeFlat
    WHERE AssessmentDate < ?;
    """
    return read_sql(conn, sql, (report_end,))
