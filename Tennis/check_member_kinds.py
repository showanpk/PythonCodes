import pyodbc
import os

SQL_USER = os.getenv('CRM_SQL_USER', 'sahelihubadmin')
SQL_PASSWORD = os.getenv('CRM_SQL_PASSWORD', 'W7WZ7ZaG1YbMZ71gh%2xSFuR')

SQL_CONNECTION_STRING = (
    'DRIVER={ODBC Driver 18 for SQL Server};'
    'SERVER=tcp:sahelihub.database.windows.net,1433;'
    'DATABASE=SahelihubCRM;'
    f'UID={SQL_USER};'
    f'PWD={SQL_PASSWORD};'
    'Encrypt=yes;'
    'TrustServerCertificate=yes;'
)

try:
    conn = pyodbc.connect(SQL_CONNECTION_STRING)
    cursor = conn.cursor()
    
    cursor.execute('''
    SELECT COUNT(*) as FULL_COUNT
    FROM dbo.SessionAttendance
    WHERE SessionId IN (SELECT SessionId FROM dbo.Sessions WHERE ActivityName = 'Tennis')
      AND AttendanceMemberKind = 'FULL'
    ''')
    
    full_count = cursor.fetchone()[0]
    
    cursor.execute('''
    SELECT COUNT(*) as LITE_COUNT
    FROM dbo.SessionAttendance
    WHERE SessionId IN (SELECT SessionId FROM dbo.Sessions WHERE ActivityName = 'Tennis')
      AND AttendanceMemberKind = 'LITE'
    ''')
    
    lite_count = cursor.fetchone()[0]
    
    print(f'FULL members in database: {full_count}')
    print(f'LITE members in database: {lite_count}')
    print(f'Total: {full_count + lite_count}')
    print()
    print(f'Expected from Excel: 519')
    print(f'Missing: {519 - (full_count + lite_count)}')
    
    cursor.close()
    conn.close()
except Exception as e:
    print(f'Error: {e}')
