#!/usr/bin/env python3
"""
check_missing_birthdates.py
───────────────────────────
Report voters who still have NO BirthDate:
  • either no row in dbo.BirthDates
  • or a row exists but BirthDate IS NULL
"""

import os
import sys
import pyodbc
from dotenv import load_dotenv

# ───────── CONFIG – tweak if needed ────────────────────────
DB_DRIVER   = '{ODBC Driver 17 for SQL Server}'
DB_SERVER   = '.'                       # '.' = local default instance
DB_DATABASE = 'sa_database_enrichment'
DB_USER     = 'KC'
SHOW_ROWS   = 20                        # how many rows to display
# ───────────────────────────────────────────────────────────

load_dotenv()
PWD = os.getenv('DB_PASSWORD')
if not PWD:
    sys.exit("❌  DB_PASSWORD missing in environment / .env file")

CONN_STR = (
    f"DRIVER={DB_DRIVER};"
    f"SERVER={DB_SERVER};"
    f"DATABASE={DB_DATABASE};"
    f"UID={DB_USER};PWD={PWD};"
    f"TrustServerCertificate=yes;"
)

QUERY = """
SELECT mi.Id,
       mi.FullName,
       mi.IDNumber
FROM   dbo.MasterItems AS mi
LEFT   JOIN dbo.BirthDates AS bd ON bd.Id = mi.Id
WHERE  bd.Id IS NULL            -- no row
   OR  bd.BirthDate IS NULL     -- or row exists but NULL
"""

def main():
    with pyodbc.connect(CONN_STR) as cn:
        cur = cn.cursor()

        # 1️⃣  Total remaining
        cur.execute("SELECT COUNT(*) FROM (" + QUERY + ") x;")
        remaining = cur.fetchone()[0]
        print(f"Voters missing BirthDate : {remaining}")

        # 2️⃣  Sample list (first SHOW_ROWS)
        if remaining:
            cur.execute(QUERY + " ORDER BY mi.Id OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY;", SHOW_ROWS)
            rows = cur.fetchall()
            print(f"\nFirst {len(rows)} rows needing attention:")
            print("-" * 60)
            for r in rows:
                print(f"Id={r.Id:<8}  FullName={r.FullName or '—':<30}  IDNumber={r.IDNumber}")
            print("-" * 60)
            print("Tip: raise SHOW_ROWS or export to CSV if you need the full list.")

if __name__ == "__main__":
    main()