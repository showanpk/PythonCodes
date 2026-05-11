import pandas as pd
import pyodbc
import os
import re
from datetime import datetime, date
from typing import Optional, Tuple

# Excel data extraction
excel_file = r'C:\Users\shonk\Downloads\Tennis Register 2025.xlsx'

def clean_text(value):
    if pd.isna(value):
        return None
    text = str(value).strip()
    if text.upper() in {"", "#N/A", "N/A", "NA", "NONE", "NULL", "NAN", "(BLANK)", "0"}:
        return None
    return text

def excel_date_to_date(value):
    value = clean_text(value) if isinstance(value, str) else value
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, (int, float)):
        try:
            return pd.Timestamp(value, unit='D', origin='1899-12-30').date()
        except:
            return None
    try:
        parsed = pd.to_datetime(value, dayfirst=True, errors='coerce')
        if pd.isna(parsed):
            return None
        return parsed.date()
    except:
        return None

# Collect all valid Excel rows
excel_rows = []

for sheet_name in pd.ExcelFile(excel_file).sheet_names:
    if sheet_name.strip() in ['Sheet1', 'Template', 'Full Register', '[1]Full Register']:
        continue
        
    df = pd.read_excel(excel_file, sheet_name=sheet_name, header=0, dtype=object)
    
    for idx, row in df.iterrows():
        activity = row.iloc[0] if len(row) > 0 else None
        date_val = row.iloc[2] if len(row) > 2 else None
        time = row.iloc[4] if len(row) > 4 else None
        card = row.iloc[5] if len(row) > 5 else None
        name = row.iloc[6] if len(row) > 6 else None
        
        # Is this a valid row?
        if (clean_text(activity) and clean_text(activity).lower() == 'tennis' and
            excel_date_to_date(date_val) and clean_text(time)):
            
            card_clean = clean_text(card)
            name_clean = clean_text(name)
            
            if card_clean or name_clean:
                excel_rows.append({
                    'sheet': sheet_name,
                    'row': idx + 2,
                    'date': excel_date_to_date(date_val),
                    'time': clean_text(time),
                    'card': card_clean,
                    'name': name_clean
                })

print(f'Total valid Excel rows: {len(excel_rows)}')

# Get database records
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
    SELECT SessionDate, SessionStartTime, SessionName, SaheliCardNumber, MemberName
    FROM dbo.SessionAttendance
    WHERE SessionId IN (SELECT SessionId FROM dbo.Sessions WHERE ActivityName = 'Tennis')
    ORDER BY SessionDate, SessionStartTime, MemberName
    ''')
    
    db_records = []
    for row in cursor.fetchall():
        db_records.append({
            'date': row.SessionDate if isinstance(row.SessionDate, date) else None,
            'time': str(row.SessionStartTime) if row.SessionStartTime else None,
            'card': str(row.SaheliCardNumber) if row.SaheliCardNumber else None,
            'name': str(row.MemberName) if row.MemberName else None
        })
    
    cursor.close()
    conn.close()
    
    print(f'Database records: {len(db_records)}')
    
    # Find missing rows
    missing = []
    for excel_row in excel_rows:
        found = False
        for db_row in db_records:
            # Match if same date, time, and (card or name)
            if (excel_row['date'] == db_row['date'] and
                str(excel_row['time']).split(':')[0] + ':' in (db_row['time'] or '')):  # Hour match
                
                if ((excel_row['card'] and excel_row['card'] == db_row['card']) or
                    (excel_row['name'] and excel_row['name'].lower() == (db_row['name'] or '').lower())):
                    found = True
                    break
        
        if not found:
            missing.append(excel_row)
    
    print(f'\nMissing records: {len(missing)}')
    print('\nFirst 10 missing rows:')
    for row in missing[:10]:
        print(f'  Sheet {row["sheet"]}, Row {row["row"]}: {row["date"]} {row["time"]} - Card: {row["card"]}, Name: {row["name"]}')

except Exception as e:
    print(f'Error: {e}')
    import traceback
    traceback.print_exc()
