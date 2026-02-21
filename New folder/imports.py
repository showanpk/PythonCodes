import pandas as pd
import pyodbc

CSV_FILE = r"C:\SQLImports\SaheliMigrations_rawjson.csv"

SQL_SERVER = "MIGHTYSUPERMAN"
DATABASE = "SahelihubCRM"
TABLE = "dbo.Staging_HealthAssessments_RawJson"

CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={SQL_SERVER};"
    f"DATABASE={DATABASE};"
    "Trusted_Connection=yes;"
)

df = pd.read_csv(CSV_FILE, encoding="utf-8")

conn = pyodbc.connect(CONN_STR)
cursor = conn.cursor()

# 🚨 CRITICAL FIX
cursor.setinputsizes([(pyodbc.SQL_WVARCHAR, 0)])

cursor.fast_executemany = True

insert_sql = f"""
INSERT INTO {TABLE} (RawJson)
VALUES (?)
"""

data = [(row,) for row in df["RawJson"]]

cursor.executemany(insert_sql, data)
conn.commit()

cursor.close()
conn.close()

print(f"✅ Inserted {len(data)} rows successfully")
