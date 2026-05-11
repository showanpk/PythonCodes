import pyodbc
import os

SQL_USER = os.getenv('CRM_SQL_USER', 'sahelihubadmin')
SQL_PASSWORD = os.getenv('CRM_SQL_PASSWORD', 'W7WZ7ZaG1YbMZ71gh%2xSFuR')

SQL_CONNECTION_STRING = (
    'DRIVER={ODBC Driver 18 for SQL Server};'
    f'SERVER=tcp:sahelihub.database.windows.net:1433;'
    'DATABASE=SahelihubCRM;'
    f'UID={SQL_USER};'
    f'PWD={SQL_PASSWORD};'
    'Encrypt=yes;'
    'TrustServerCertificate=yes;'
    'Connection Timeout=30;'
)

conn = pyodbc.connect(SQL_CONNECTION_STRING)
cursor = conn.cursor()

print('Count of Tennis sessions:')
cursor.execute("SELECT COUNT(*) FROM dbo.Sessions WHERE ActivityName = 'Tennis'")
print(f'Total Tennis Sessions: {cursor.fetchone()[0]}')

print()
print('Recent Tennis Sessions:')
cursor.execute('''
SELECT TOP 5 SessionId, VenueName, ActivityName, SessionDate, StartTime, EndTime 
FROM dbo.Sessions 
WHERE ActivityName = 'Tennis'
ORDER BY SessionId DESC
''')
for row in cursor.fetchall():
    print(row)

print()
print('Count of Tennis Attendance:')
cursor.execute("SELECT COUNT(*) FROM dbo.SessionAttendance WHERE SessionId IN (SELECT SessionId FROM dbo.Sessions WHERE ActivityName = 'Tennis')")
print(f'Total Tennis Attendance records: {cursor.fetchone()[0]}')

print()
print('Recent Tennis Attendance:')
cursor.execute('''
SELECT TOP 5 AttendanceId, SessionId, MemberDisplayId, MemberName, AttendanceMemberKind
FROM dbo.SessionAttendance
WHERE SessionId IN (SELECT SessionId FROM dbo.Sessions WHERE ActivityName = 'Tennis')
ORDER BY AttendanceId DESC
''')
for row in cursor.fetchall():
    print(row)

cursor.close()
conn.close()
