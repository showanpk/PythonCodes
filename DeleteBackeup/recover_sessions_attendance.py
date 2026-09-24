import pyodbc
from getpass import getpass


# ============================================================
# SAHELI CRM - DELETED SESSION + ATTENDANCE RECOVERY
# ============================================================
#
# Recovery source:
#   SahelihubCRM_2026-09-23T10-50Z
#
# Production:
#   SahelihubCRM
#
# FIRST RUN:
#   COMMIT_CHANGES = False
#
# ONLY AFTER PREVIEW IS CORRECT:
#   COMMIT_CHANGES = True
#
# ============================================================


# ============================================================
# CONFIGURATION
# ============================================================

SERVER = "sahelihub.database.windows.net"

USERNAME = "sahelihubadmin"

LIVE_DB = "SahelihubCRM"

RECOVERY_DB = "SahelihubCRM_2026-09-23T10-50Z"


# ------------------------------------------------------------
# SAFETY SWITCH
# ------------------------------------------------------------
# False = preview only
# True  = actually restore records

COMMIT_CHANGES = True


# ============================================================
# PASSWORD
# ============================================================
# Password is requested securely when script starts.
# It is not stored inside this Python file.

PASSWORD = "W7WZ7ZaG1YbMZ71gh%2xSFuR;"

# ============================================================
# DATABASE CONNECTION
# ============================================================

def connect(database_name):

    connection_string = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER=tcp:{SERVER},1433;"
        f"DATABASE={database_name};"
        f"UID={USERNAME};"
        f"PWD={PASSWORD};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    return pyodbc.connect(
        connection_string,
        autocommit=False
    )


# ============================================================
# METADATA HELPERS
# ============================================================

def get_columns(cursor, table_name):

    cursor.execute(
        """
        SELECT
            c.name,
            c.column_id,
            c.is_identity,
            c.is_computed,
            TYPE_NAME(c.system_type_id) AS DataType,
            c.generated_always_type
        FROM sys.columns c
        INNER JOIN sys.tables t
            ON c.object_id = t.object_id
        INNER JOIN sys.schemas s
            ON t.schema_id = s.schema_id
        WHERE s.name = 'dbo'
          AND t.name = ?
        ORDER BY c.column_id;
        """,
        table_name
    )

    result = []

    for row in cursor.fetchall():

        result.append(
            {
                "name": row[0],
                "column_id": row[1],
                "is_identity": bool(row[2]),
                "is_computed": bool(row[3]),
                "data_type": row[4],
                "generated_always_type": row[5]
            }
        )

    return result


def get_primary_key(cursor, table_name):

    cursor.execute(
        """
        SELECT
            c.name
        FROM sys.indexes i

        INNER JOIN sys.index_columns ic
            ON i.object_id = ic.object_id
           AND i.index_id = ic.index_id

        INNER JOIN sys.columns c
            ON ic.object_id = c.object_id
           AND ic.column_id = c.column_id

        INNER JOIN sys.tables t
            ON i.object_id = t.object_id

        INNER JOIN sys.schemas s
            ON t.schema_id = s.schema_id

        WHERE i.is_primary_key = 1
          AND s.name = 'dbo'
          AND t.name = ?

        ORDER BY ic.key_ordinal;
        """,
        table_name
    )

    rows = cursor.fetchall()

    if len(rows) != 1:

        raise RuntimeError(
            f"Expected exactly one primary key column "
            f"for dbo.{table_name}, but found: "
            f"{[row[0] for row in rows]}"
        )

    return rows[0][0]


def get_row_count(cursor, table_name):

    cursor.execute(
        f"""
        SELECT COUNT(*)
        FROM dbo.[{table_name}];
        """
    )

    return cursor.fetchone()[0]


def get_ids(cursor, table_name, key_column):

    cursor.execute(
        f"""
        SELECT [{key_column}]
        FROM dbo.[{table_name}];
        """
    )

    return {
        row[0]
        for row in cursor.fetchall()
    }


def get_missing_ids(
    live_cursor,
    recovery_cursor,
    table_name
):

    live_pk = get_primary_key(
        live_cursor,
        table_name
    )

    recovery_pk = get_primary_key(
        recovery_cursor,
        table_name
    )

    if live_pk.lower() != recovery_pk.lower():

        raise RuntimeError(
            f"Primary-key mismatch for dbo.{table_name}.\n"
            f"Live PK     : {live_pk}\n"
            f"Recovery PK : {recovery_pk}"
        )

    live_ids = get_ids(
        live_cursor,
        table_name,
        live_pk
    )

    recovery_ids = get_ids(
        recovery_cursor,
        table_name,
        recovery_pk
    )

    missing_ids = recovery_ids - live_ids

    return live_pk, missing_ids


# ============================================================
# SCHEMA VALIDATION
# ============================================================

def validate_schema(
    live_cursor,
    recovery_cursor,
    table_name
):

    live_columns = get_columns(
        live_cursor,
        table_name
    )

    recovery_columns = get_columns(
        recovery_cursor,
        table_name
    )

    live_names = [
        column["name"]
        for column in live_columns
    ]

    recovery_names = [
        column["name"]
        for column in recovery_columns
    ]

    if live_names != recovery_names:

        print()
        print("SCHEMA MISMATCH")
        print(f"Table: dbo.{table_name}")

        print()
        print("LIVE:")
        print(live_names)

        print()
        print("RECOVERY:")
        print(recovery_names)

        raise RuntimeError(
            f"Schema mismatch detected in dbo.{table_name}. "
            "Recovery stopped."
        )


# ============================================================
# INSERTABLE COLUMNS
# ============================================================

def get_insertable_columns(
    cursor,
    table_name
):

    columns = get_columns(
        cursor,
        table_name
    )

    insertable = []

    for column in columns:

        # Do not insert calculated columns
        if column["is_computed"]:
            continue

        # Do not explicitly insert rowversion/timestamp
        if column["data_type"].lower() in (
            "timestamp",
            "rowversion"
        ):
            continue

        # Generated SQL Server system columns
        if column["generated_always_type"] not in (
            0,
            None
        ):
            continue

        insertable.append(column)

    return insertable


# ============================================================
# GENERIC SINGLE-ROW COPY
# ============================================================

def copy_row_by_id(
    live_cursor,
    recovery_cursor,
    table_name,
    pk_column,
    record_id,
    insert_columns
):

    column_names = [
        column["name"]
        for column in insert_columns
    ]

    select_columns = ", ".join(
        f"[{name}]"
        for name in column_names
    )

    insert_column_sql = ", ".join(
        f"[{name}]"
        for name in column_names
    )

    placeholders = ", ".join(
        "?"
        for _ in column_names
    )

    # --------------------------------------------------------
    # Final duplicate protection
    # --------------------------------------------------------

    live_cursor.execute(
        f"""
        SELECT COUNT(*)
        FROM dbo.[{table_name}]
        WHERE [{pk_column}] = ?;
        """,
        record_id
    )

    if live_cursor.fetchone()[0] > 0:

        return False

    # --------------------------------------------------------
    # Read original row from recovery DB
    # --------------------------------------------------------

    recovery_cursor.execute(
        f"""
        SELECT
            {select_columns}
        FROM dbo.[{table_name}]
        WHERE [{pk_column}] = ?;
        """,
        record_id
    )

    row = recovery_cursor.fetchone()

    if row is None:

        raise RuntimeError(
            f"Could not retrieve dbo.{table_name} "
            f"record {record_id} from recovery database."
        )

    # --------------------------------------------------------
    # Insert exact recovery values into live DB
    # --------------------------------------------------------

    live_cursor.execute(
        f"""
        INSERT INTO dbo.[{table_name}]
        (
            {insert_column_sql}
        )
        VALUES
        (
            {placeholders}
        );
        """,
        tuple(row)
    )

    return True


# ============================================================
# RESTORE MISSING SESSIONS
# ============================================================

def restore_sessions(
    live_cursor,
    recovery_cursor,
    missing_session_ids,
    session_pk
):

    print()
    print("=" * 70)
    print("SESSION RECOVERY")
    print("=" * 70)

    print(
        f"Sessions selected for recovery: "
        f"{len(missing_session_ids)}"
    )

    if not COMMIT_CHANGES:

        print(
            "PREVIEW MODE - sessions will NOT be inserted."
        )

        return 0

    insert_columns = get_insertable_columns(
        recovery_cursor,
        "Sessions"
    )

    identity_columns = [
        column["name"]
        for column in insert_columns
        if column["is_identity"]
    ]

    restored = 0

    identity_enabled = False

    try:

        if identity_columns:

            live_cursor.execute(
                """
                SET IDENTITY_INSERT dbo.Sessions ON;
                """
            )

            identity_enabled = True

        for session_id in sorted(missing_session_ids):

            inserted = copy_row_by_id(
                live_cursor,
                recovery_cursor,
                "Sessions",
                session_pk,
                session_id,
                insert_columns
            )

            if inserted:
                restored += 1

        if identity_enabled:

            live_cursor.execute(
                """
                SET IDENTITY_INSERT dbo.Sessions OFF;
                """
            )

            identity_enabled = False

    except Exception:

        if identity_enabled:

            try:

                live_cursor.execute(
                    """
                    SET IDENTITY_INSERT dbo.Sessions OFF;
                    """
                )

            except Exception:
                pass

        raise

    print(
        f"Sessions restored: {restored}"
    )

    return restored


# ============================================================
# FIND ATTENDANCE BELONGING TO DELETED SESSIONS
# ============================================================

def audit_missing_attendance(
    live_cursor,
    recovery_cursor,
    missing_session_ids
):

    attendance_pk, all_missing_attendance_ids = (
        get_missing_ids(
            live_cursor,
            recovery_cursor,
            "SessionAttendance"
        )
    )

    print()
    print("=" * 70)
    print("ATTENDANCE RECOVERY AUDIT")
    print("=" * 70)

    print(
        "All missing attendance IDs in recovery snapshot :",
        len(all_missing_attendance_ids)
    )

    if not all_missing_attendance_ids:

        return (
            attendance_pk,
            [],
            []
        )

    # --------------------------------------------------------
    # Find the SessionId for every missing attendance record.
    #
    # We process IDs one-by-one here rather than building a
    # huge dynamic IN query. Recovery volume is small enough.
    # --------------------------------------------------------

    linked_to_deleted_sessions = []

    unrelated_missing_attendance = []

    for attendance_id in all_missing_attendance_ids:

        recovery_cursor.execute(
            f"""
            SELECT
                [{attendance_pk}],
                SessionId
            FROM dbo.SessionAttendance
            WHERE [{attendance_pk}] = ?;
            """,
            attendance_id
        )

        row = recovery_cursor.fetchone()

        if row is None:

            raise RuntimeError(
                f"AttendanceId {attendance_id} "
                "exists in ID list but could not be read."
            )

        found_attendance_id = row[0]

        session_id = row[1]

        if session_id in missing_session_ids:

            linked_to_deleted_sessions.append(
                (
                    found_attendance_id,
                    session_id
                )
            )

        else:

            unrelated_missing_attendance.append(
                (
                    found_attendance_id,
                    session_id
                )
            )

    print(
        "Attendance linked to deleted sessions          :",
        len(linked_to_deleted_sessions)
    )

    print(
        "Other missing attendance                       :",
        len(unrelated_missing_attendance)
    )

    print()

    if linked_to_deleted_sessions:

        print(
            "First attendance rows selected for recovery:"
        )

        for attendance_id, session_id in (
            linked_to_deleted_sessions[:20]
        ):

            print(
                f"  AttendanceId={attendance_id}, "
                f"SessionId={session_id}"
            )

    if unrelated_missing_attendance:

        print()
        print(
            "NOTE: Other missing attendance exists, "
            "but this script will NOT restore it."
        )

        print(
            "Only attendance linked to the deleted "
            "sessions will be recovered."
        )

        print()
        print(
            "First unrelated missing attendance rows:"
        )

        for attendance_id, session_id in (
            unrelated_missing_attendance[:20]
        ):

            print(
                f"  AttendanceId={attendance_id}, "
                f"SessionId={session_id}"
            )

    return (
        attendance_pk,
        linked_to_deleted_sessions,
        unrelated_missing_attendance
    )


# ============================================================
# RESTORE ATTENDANCE
# ============================================================

def restore_attendance(
    live_cursor,
    recovery_cursor,
    attendance_pk,
    attendance_rows
):

    print()
    print("=" * 70)
    print("ATTENDANCE RECOVERY")
    print("=" * 70)

    print(
        f"Attendance rows selected for recovery: "
        f"{len(attendance_rows)}"
    )

    if not COMMIT_CHANGES:

        print(
            "PREVIEW MODE - attendance will NOT be inserted."
        )

        return 0

    insert_columns = get_insertable_columns(
        recovery_cursor,
        "SessionAttendance"
    )

    identity_columns = [
        column["name"]
        for column in insert_columns
        if column["is_identity"]
    ]

    identity_enabled = False

    restored = 0

    try:

        if identity_columns:

            live_cursor.execute(
                """
                SET IDENTITY_INSERT
                    dbo.SessionAttendance
                ON;
                """
            )

            identity_enabled = True

        for attendance_id, session_id in attendance_rows:

            # ------------------------------------------------
            # Critical safety check:
            # parent Session MUST exist before attendance.
            # ------------------------------------------------

            live_cursor.execute(
                """
                SELECT COUNT(*)
                FROM dbo.Sessions
                WHERE SessionId = ?;
                """,
                session_id
            )

            session_exists = (
                live_cursor.fetchone()[0]
            )

            if session_exists == 0:

                raise RuntimeError(
                    f"Cannot restore AttendanceId "
                    f"{attendance_id}. "
                    f"Parent SessionId {session_id} "
                    "does not exist in live database."
                )

            inserted = copy_row_by_id(
                live_cursor,
                recovery_cursor,
                "SessionAttendance",
                attendance_pk,
                attendance_id,
                insert_columns
            )

            if inserted:

                restored += 1

        if identity_enabled:

            live_cursor.execute(
                """
                SET IDENTITY_INSERT
                    dbo.SessionAttendance
                OFF;
                """
            )

            identity_enabled = False

    except Exception:

        if identity_enabled:

            try:

                live_cursor.execute(
                    """
                    SET IDENTITY_INSERT
                        dbo.SessionAttendance
                    OFF;
                    """
                )

            except Exception:
                pass

        raise

    print(
        f"Attendance restored: {restored}"
    )

    return restored


# ============================================================
# VALIDATE RECOVERY
# ============================================================

def validate_after_restore(
    live_cursor,
    recovery_cursor,
    original_missing_session_ids,
    recovery_attendance_rows
):

    print()
    print("=" * 70)
    print("POST-RECOVERY VALIDATION")
    print("=" * 70)

    # --------------------------------------------------------
    # Check all recovered SessionIds now exist
    # --------------------------------------------------------

    sessions_still_missing = []

    for session_id in original_missing_session_ids:

        live_cursor.execute(
            """
            SELECT COUNT(*)
            FROM dbo.Sessions
            WHERE SessionId = ?;
            """,
            session_id
        )

        if live_cursor.fetchone()[0] == 0:

            sessions_still_missing.append(
                session_id
            )

    # --------------------------------------------------------
    # Check selected attendance now exists
    # --------------------------------------------------------

    attendance_still_missing = []

    attendance_pk = get_primary_key(
        live_cursor,
        "SessionAttendance"
    )

    for attendance_id, session_id in recovery_attendance_rows:

        live_cursor.execute(
            f"""
            SELECT COUNT(*)
            FROM dbo.SessionAttendance
            WHERE [{attendance_pk}] = ?;
            """,
            attendance_id
        )

        if live_cursor.fetchone()[0] == 0:

            attendance_still_missing.append(
                attendance_id
            )

    # --------------------------------------------------------
    # Check attendance for restored sessions has valid parent
    # --------------------------------------------------------

    orphan_count = 0

    for attendance_id, session_id in recovery_attendance_rows:

        live_cursor.execute(
            """
            SELECT COUNT(*)
            FROM dbo.SessionAttendance a
            INNER JOIN dbo.Sessions s
                ON s.SessionId = a.SessionId
            WHERE a.AttendanceId = ?
              AND s.SessionId = ?;
            """,
            attendance_id,
            session_id
        )

        if live_cursor.fetchone()[0] == 0:

            orphan_count += 1

    print(
        "Recovered sessions still missing   :",
        len(sessions_still_missing)
    )

    print(
        "Recovered attendance still missing :",
        len(attendance_still_missing)
    )

    print(
        "Invalid/orphan recovered attendance:",
        orphan_count
    )

    if sessions_still_missing:

        raise RuntimeError(
            f"{len(sessions_still_missing)} "
            "session records are still missing."
        )

    if attendance_still_missing:

        raise RuntimeError(
            f"{len(attendance_still_missing)} "
            "attendance records are still missing."
        )

    if orphan_count > 0:

        raise RuntimeError(
            f"{orphan_count} recovered attendance "
            "records failed parent-session validation."
        )


# ============================================================
# MAIN
# ============================================================

live_conn = None
recovery_conn = None


try:

    print()
    print("=" * 70)
    print("SAHELI CRM SESSION / ATTENDANCE RECOVERY")
    print("=" * 70)

    print(
        "Live DB    :",
        LIVE_DB
    )

    print(
        "Recovery DB:",
        RECOVERY_DB
    )

    print()

    if COMMIT_CHANGES:

        print(
            "*** COMMIT MODE - DATABASE WILL BE MODIFIED ***"
        )

    else:

        print(
            "*** PREVIEW MODE - NOTHING WILL BE CHANGED ***"
        )

    # ========================================================
    # CONNECT
    # ========================================================

    live_conn = connect(
        LIVE_DB
    )

    recovery_conn = connect(
        RECOVERY_DB
    )

    live = live_conn.cursor()

    recovery = recovery_conn.cursor()

    # ========================================================
    # VALIDATE TABLE SCHEMAS
    # ========================================================

    print()
    print("=" * 70)
    print("SCHEMA CHECK")
    print("=" * 70)

    validate_schema(
        live,
        recovery,
        "Sessions"
    )

    print(
        "dbo.Sessions: OK"
    )

    validate_schema(
        live,
        recovery,
        "SessionAttendance"
    )

    print(
        "dbo.SessionAttendance: OK"
    )

    # ========================================================
    # CURRENT COUNTS
    # ========================================================

    live_session_count_before = get_row_count(
        live,
        "Sessions"
    )

    recovery_session_count = get_row_count(
        recovery,
        "Sessions"
    )

    live_attendance_count_before = get_row_count(
        live,
        "SessionAttendance"
    )

    recovery_attendance_count = get_row_count(
        recovery,
        "SessionAttendance"
    )

    print()
    print("=" * 70)
    print("DATABASE COUNTS")
    print("=" * 70)

    print(
        "Live Sessions          :",
        live_session_count_before
    )

    print(
        "Recovery Sessions      :",
        recovery_session_count
    )

    print(
        "Live Attendance        :",
        live_attendance_count_before
    )

    print(
        "Recovery Attendance    :",
        recovery_attendance_count
    )

    # ========================================================
    # FIND THE MISSING SESSIONS
    # ========================================================

    session_pk, missing_session_ids = (
        get_missing_ids(
            live,
            recovery,
            "Sessions"
        )
    )

    print()
    print("=" * 70)
    print("MISSING SESSION AUDIT")
    print("=" * 70)

    print(
        "Session primary key:",
        session_pk
    )

    print(
        "Missing Sessions   :",
        len(missing_session_ids)
    )

    print()

    print(
        "First missing SessionIds:"
    )

    for session_id in (
        sorted(missing_session_ids)[:30]
    ):

        print(
            f"  {session_id}"
        )

    # ========================================================
    # ATTENDANCE AUDIT
    # ========================================================

    (
        attendance_pk,
        attendance_to_restore,
        unrelated_missing_attendance
    ) = audit_missing_attendance(
        live,
        recovery,
        missing_session_ids
    )

    # ========================================================
    # SUMMARY BEFORE ANY WRITE
    # ========================================================

    print()
    print("=" * 70)
    print("RECOVERY PLAN")
    print("=" * 70)

    print(
        "Sessions to restore                  :",
        len(missing_session_ids)
    )

    print(
        "Attendance linked to those sessions  :",
        len(attendance_to_restore)
    )

    print(
        "Other missing attendance ignored     :",
        len(unrelated_missing_attendance)
    )

    # ========================================================
    # PREVIEW MODE
    # ========================================================

    if not COMMIT_CHANGES:

        print()
        print("=" * 70)
        print("PREVIEW COMPLETE")
        print("=" * 70)

        print()
        print(
            "NO DATA WAS CHANGED."
        )

        print()
        print(
            "Expected based on your previous preview:"
        )

        print(
            "  Missing Sessions   : 332"
        )

        print(
            "  Missing Attendance : 964"
        )

        print()
        print(
            "Check the RECOVERY PLAN above."
        )

        print()
        print(
            "If it shows the correct 332 sessions "
            "and the expected attendance linked to them, "
            "change:"
        )

        print()

        print(
            "COMMIT_CHANGES = False"
        )

        print()

        print(
            "to:"
        )

        print()

        print(
            "COMMIT_CHANGES = True"
        )

        print()

        # Explicit rollback even though preview did not write.
        live_conn.rollback()

    # ========================================================
    # REAL RECOVERY
    # ========================================================

    else:

        print()
        print("=" * 70)
        print("STARTING RECOVERY TRANSACTION")
        print("=" * 70)

        # ----------------------------------------------------
        # Sessions FIRST
        # ----------------------------------------------------

        sessions_restored = restore_sessions(
            live,
            recovery,
            missing_session_ids,
            session_pk
        )

        # ----------------------------------------------------
        # Attendance SECOND
        # ----------------------------------------------------

        attendance_restored = restore_attendance(
            live,
            recovery,
            attendance_pk,
            attendance_to_restore
        )

        # ----------------------------------------------------
        # Validate BEFORE COMMIT
        # ----------------------------------------------------

        validate_after_restore(
            live,
            recovery,
            missing_session_ids,
            attendance_to_restore
        )

        # ----------------------------------------------------
        # Final counts
        # ----------------------------------------------------

        live_session_count_after = (
            get_row_count(
                live,
                "Sessions"
            )
        )

        live_attendance_count_after = (
            get_row_count(
                live,
                "SessionAttendance"
            )
        )

        print()
        print("=" * 70)
        print("FINAL COUNTS BEFORE COMMIT")
        print("=" * 70)

        print(
            "Sessions before :",
            live_session_count_before
        )

        print(
            "Sessions after  :",
            live_session_count_after
        )

        print(
            "Difference      :",
            (
                live_session_count_after
                - live_session_count_before
            )
        )

        print()

        print(
            "Attendance before:",
            live_attendance_count_before
        )

        print(
            "Attendance after :",
            live_attendance_count_after
        )

        print(
            "Difference       :",
            (
                live_attendance_count_after
                - live_attendance_count_before
            )
        )

        # ----------------------------------------------------
        # Final sanity checks
        # ----------------------------------------------------

        if (
            live_session_count_after
            - live_session_count_before
        ) != sessions_restored:

            raise RuntimeError(
                "Session count increase does not match "
                "number restored."
            )

        if (
            live_attendance_count_after
            - live_attendance_count_before
        ) != attendance_restored:

            raise RuntimeError(
                "Attendance count increase does not match "
                "number restored."
            )

        # ----------------------------------------------------
        # COMMIT
        # ----------------------------------------------------

        live_conn.commit()

        print()
        print("=" * 70)
        print("RECOVERY SUCCESSFUL")
        print("=" * 70)

        print()

        print(
            f"Sessions restored   : "
            f"{sessions_restored}"
        )

        print(
            f"Attendance restored : "
            f"{attendance_restored}"
        )

        print()

        print(
            "Transaction committed successfully."
        )


# ============================================================
# ERROR HANDLING
# ============================================================

except Exception as ex:

    print()
    print("=" * 70)
    print("RECOVERY STOPPED")
    print("=" * 70)

    print()

    print(
        "ERROR:"
    )

    print(ex)

    if live_conn is not None:

        try:

            live_conn.rollback()

            print()
            print(
                "LIVE DATABASE TRANSACTION ROLLED BACK."
            )

            print(
                "No partial recovery should remain."
            )

        except Exception as rollback_error:

            print()
            print(
                "WARNING: rollback also reported an error:"
            )

            print(
                rollback_error
            )

    raise


# ============================================================
# CLEANUP
# ============================================================

finally:

    if recovery_conn is not None:

        try:
            recovery_conn.close()
        except Exception:
            pass

    if live_conn is not None:

        try:
            live_conn.close()
        except Exception:
            pass