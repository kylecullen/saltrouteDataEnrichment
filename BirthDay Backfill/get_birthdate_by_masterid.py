#!/usr/bin/env python3
# ───────────────────────── CONFIG ──────────────────────────
TEST_MASTER_ID   = 1          # ← put the MasterItems.Id you want to write
ODBC_DRIVER      = '{ODBC Driver 17 for SQL Server}'
SQL_SERVER       = '.'             # '.' = local default instance
SQL_DATABASE     = 'sa_database_enrichment'
SQL_USERNAME     = 'KC'
# password comes from the environment variable DB_PASSWORD
# ───────────────────────────────────────────────────────────

import os, re, sys
from datetime import date
import pyodbc
from dotenv import load_dotenv

load_dotenv()                                   # reads DB_PASSWORD from .env
SQL_PASSWORD = os.getenv('DB_PASSWORD')

# ────── helpers ─────────────────────────────────────────────
def connect() -> pyodbc.Connection:
    if not SQL_PASSWORD:
        sys.exit("❌  DB_PASSWORD not set in environment / .env file.")
    conn_str = (
        f"DRIVER={ODBC_DRIVER};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        f"UID={SQL_USERNAME};"
        f"PWD={SQL_PASSWORD};"
        f"TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)

def first_six(idnum: str) -> str | None:
    m = re.search(r'\d{6}', idnum)
    return m.group(0) if m else None

def derive_dob(six: str) -> date | None:
    yy, mm, dd = int(six[:2]), int(six[2:4]), int(six[4:6])
    pivot = date.today().year % 100              # e.g. 25 for 2025
    century = 1900 if yy > pivot else 2000
    try:
        return date(century + yy, mm, dd)
    except ValueError:
        return None

def get_idnumber(cur: pyodbc.Cursor, master_id: int) -> str | None:
    cur.execute("SELECT IDNumber FROM dbo.MasterItems WHERE Id = ?", master_id)
    row = cur.fetchone()
    return row.IDNumber if row and row.IDNumber else None

def upsert_birthdate(conn: pyodbc.Connection,
                     cur:  pyodbc.Cursor,
                     master_id: int,
                     dob:       date) -> None:
    """
    • UPDATE if a row already exists for this MasterItemId
    • Otherwise INSERT a new row, toggling IDENTITY_INSERT because
      Id is an IDENTITY column but must equal MasterItems.Id (FK).
    """
    # 1. Try plain UPDATE first
    cur.execute("""
        UPDATE dbo.BirthDates
        SET    BirthDate = ?
        WHERE  Id = ?            -- Id is also the FK to MasterItems
    """, dob, master_id)

    if cur.rowcount:
        return  # row updated → done

    # 2. No row → need INSERT with explicit Id value
    print("ℹ️  No BirthDates row found; inserting new one…")

    # Toggle IDENTITY_INSERT ON for this table only
    cur.execute("SET IDENTITY_INSERT dbo.BirthDates ON;")
    cur.execute("""
        INSERT INTO dbo.BirthDates (Id, MasterItemId, BirthDate)
        VALUES (?, ?, ?)
    """, master_id, master_id, dob)
    cur.execute("SET IDENTITY_INSERT dbo.BirthDates OFF;")
    conn.commit()

# ────── main routine ───────────────────────────────────────
def main(master_id: int) -> None:
    with connect() as conn:
        with conn.cursor() as cur:
            idnum = get_idnumber(cur, master_id)
            if not idnum:
                print(f"⚠️  MasterId {master_id} has no IDNumber; aborting.")
                return

            six = first_six(idnum)
            if not six:
                print(f"⚠️  Could not find six digits in IDNumber '{idnum}'.")
                return

            dob = derive_dob(six)
            if not dob:
                print(f"⚠️  Six-digit block '{six}' is not a valid date.")
                return

            upsert_birthdate(conn, cur, master_id, dob)
            print(f"✅  {dob.isoformat()} written to dbo.BirthDates for Id {master_id}")

if __name__ == "__main__":
    main(TEST_MASTER_ID)