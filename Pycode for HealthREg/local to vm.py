import pyodbc
from datetime import datetime

# ==========================================
# CONFIG
# ==========================================

# SOURCE: LocalDB
SOURCE_CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    r"SERVER=MIGHTYSUPERMAN q;"
    "DATABASE=SahelihubCRM;"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
)

# TARGET: VM SQL Server
TARGET_CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=20.68.160.100,1433;"
    "DATABASE=SahelihubCRM;"
    "UID=saheli_app;"
    "PWD=309183;"
    "TrustServerCertificate=yes;"
)

BATCH_SIZE = 500

SOURCE_SELECT_SQL = """
SELECT
    AttendanceId,
    SessionId,
    ParticipantId,
    SessionName,
    SessionDay,
    SessionDate,
    SessionMonth,
    SessionStartTime,
    SessionEndTime,
    SaheliCardNumber,
    RiskStratification,
    Attended,
    CheckInTime,
    CheckOutTime,
    Notes,
    CreatedAtUtc,
    UpdatedAtUtc,
    AttendanceMemberKind,
    LiteMemberId,
    MemberDisplayId,
    MemberName,
    Phone,
    EmergencyName,
    EmergencyPhone
FROM dbo.SessionAttendance
ORDER BY AttendanceId;
"""

TARGET_EXISTS_SQL = """
SELECT 1
FROM dbo.SessionAttendance
WHERE AttendanceId = ?
"""

TARGET_INSERT_SQL = """
INSERT INTO dbo.SessionAttendance
(
    AttendanceId,
    SessionId,
    ParticipantId,
    SessionName,
    SessionDay,
    SessionDate,
    SessionMonth,
    SessionStartTime,
    SessionEndTime,
    SaheliCardNumber,
    RiskStratification,
    Attended,
    CheckInTime,
    CheckOutTime,
    Notes,
    CreatedAtUtc,
    UpdatedAtUtc,
    AttendanceMemberKind,
    LiteMemberId,
    MemberDisplayId,
    MemberName,
    Phone,
    EmergencyName,
    EmergencyPhone
)
VALUES
(
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
)
"""

def log(message: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")

def test_connection(conn_str: str, name: str) -> None:
    conn = pyodbc.connect(conn_str)
    cur = conn.cursor()
    cur.execute("SELECT DB_NAME()")
    db_name = cur.fetchone()[0]
    log(f"{name} connected successfully. Database: {db_name}")
    cur.close()
    conn.close()

def main():
    inserted_count = 0
    skipped_count = 0
    processed_count = 0

    log("Testing source connection...")
    test_connection(SOURCE_CONN_STR, "Source")

    log("Testing target connection...")
    test_connection(TARGET_CONN_STR, "Target")

    log("Opening source connection...")
    source_conn = pyodbc.connect(SOURCE_CONN_STR)
    source_cursor = source_conn.cursor()

    log("Opening target connection...")
    target_conn = pyodbc.connect(TARGET_CONN_STR)
    target_conn.autocommit = False
    target_cursor = target_conn.cursor()

    # Speeds up executemany
    target_cursor.fast_executemany = True

    log("Reading source rows...")
    source_cursor.execute(SOURCE_SELECT_SQL)

    batch_to_insert = []

    while True:
        rows = source_cursor.fetchmany(BATCH_SIZE)
        if not rows:
            break

        for row in rows:
            processed_count += 1
            attendance_id = row.AttendanceId

            target_cursor.execute(TARGET_EXISTS_SQL, attendance_id)
            exists = target_cursor.fetchone()

            if exists:
                skipped_count += 1
                continue

            batch_to_insert.append((
                row.AttendanceId,
                row.SessionId,
                row.ParticipantId,
                row.SessionName,
                row.SessionDay,
                row.SessionDate,
                row.SessionMonth,
                row.SessionStartTime,
                row.SessionEndTime,
                row.SaheliCardNumber,
                row.RiskStratification,
                row.Attended,
                row.CheckInTime,
                row.CheckOutTime,
                row.Notes,
                row.CreatedAtUtc,
                row.UpdatedAtUtc,
                row.AttendanceMemberKind,
                row.LiteMemberId,
                row.MemberDisplayId,
                row.MemberName,
                row.Phone,
                row.EmergencyName,
                row.EmergencyPhone
            ))

        if batch_to_insert:
            target_cursor.executemany(TARGET_INSERT_SQL, batch_to_insert)
            target_conn.commit()
            inserted_count += len(batch_to_insert)
            log(f"Committed batch: {len(batch_to_insert)} inserted.")
            batch_to_insert.clear()

        log(f"Processed so far: {processed_count}, Inserted: {inserted_count}, Skipped: {skipped_count}")

    log("Migration completed.")
    log(f"Total processed: {processed_count}")
    log(f"Total inserted: {inserted_count}")
    log(f"Total skipped: {skipped_count}")

    source_cursor.close()
    source_conn.close()
    target_cursor.close()
    target_conn.close()

if __name__ == "__main__":
    main()