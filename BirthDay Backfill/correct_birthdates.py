#!/usr/bin/env python3
"""
correct_birthdates.py
───────────────────────
Corrects records in dbo.BirthDates by subtracting 100 years from the birth date
for any record where BirthDate > THRESHOLD_DATE.

*** WARNING: THIS SCRIPT MODIFIES DATABASE RECORDS. ***
It exports a CSV of all records that were changed.
"""

import os
import sys
import textwrap
import pyodbc
from dotenv import load_dotenv
import logging
from datetime import datetime
import csv

# ── 0.  CONFIGURATION & LOGGING SETUP ────────────────────────────────────
# --- User Configuration ---
# This is the threshold date. BirthDates AFTER this date will be targeted for correction.
CORRECTION_THRESHOLD_DATE = '2008-02-23'

# --- !! CRITICAL SAFETY SETTING !! ---
# Set to True to run the script without committing any changes to the database.
# A CSV of records that WOULD be changed will still be generated.
# Set to False to perform the actual database update.
DRY_RUN = False
# --- End User Configuration ---

# Log file setup
log_file_name = f"correct_birthdates_{'DRYRUN_' if DRY_RUN else ''}{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
log_file_full_path = os.path.join(os.getcwd(), log_file_name)

# CSV Output File for changed records
csv_file_name = f"corrected_records_{'DRYRUN_' if DRY_RUN else ''}{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
csv_file_full_path = os.path.join(os.getcwd(), csv_file_name)

try:
    print(f"--- Python script: Logging will be attempted to: {log_file_full_path} ---")
except Exception:
    pass

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_full_path, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

# ── 1.  CONNECTION DETAILS ──────────────────────────────────────────────
DB_DRIVER   = os.getenv('DB_DRIVER', '{ODBC Driver 17 for SQL Server}')
DB_SERVER   = os.getenv('DB_SERVER', '.')
DB_DATABASE = os.getenv('DB_DATABASE', 'sa_database_enrichment')
DB_USER     = os.getenv('DB_USER', 'KC')

load_dotenv()
DB_PASSWORD = os.getenv('DB_PASSWORD')

if not DB_PASSWORD:
    logging.error("DB_PASSWORD environment variable not found.")
    sys.exit("DB_PASSWORD missing. Exiting.")

CONN_STR = (
    f"DRIVER={DB_DRIVER};"
    f"SERVER={DB_SERVER};"
    f"DATABASE={DB_DATABASE};"
    f"UID={DB_USER};"
    f"PWD={DB_PASSWORD};"
    f"TrustServerCertificate=yes;"
)

# ── 2.  THE T-SQL FOR CORRECTING RECORDS ─────────────────────────────────
# This T-SQL updates the BirthDate by subtracting 100 years and uses the
# OUTPUT clause to return the old and new values of the changed rows.
TSQL_CORRECT_RECORDS = textwrap.dedent(f"""
    SET NOCOUNT ON;
    
    -- Declare variables for the execution
    DECLARE @ThresholdDate DATE = '{CORRECTION_THRESHOLD_DATE}';
    DECLARE @DryRun BIT = {1 if DRY_RUN else 0};
    DECLARE @UpdatedCount INT;

    -- Create a table variable to capture the changes
    DECLARE @ChangedRecords TABLE (
        Id INT,
        MasterItemId NVARCHAR(255),
        OldBirthDate DATE,
        NewBirthDate DATE
    );

    PRINT N'Starting BirthDate correction process.';
    PRINT CONCAT(N'Criteria: BirthDate > ''', CONVERT(NVARCHAR(10), @ThresholdDate, 23), N'''');

    -- Begin a transaction to ensure atomicity
    BEGIN TRANSACTION;

    -- Perform the update and capture the changes into the table variable
    UPDATE dbo.BirthDates
    SET 
        BirthDate = DATEADD(year, -100, BirthDate)
    OUTPUT 
        deleted.Id,
        deleted.MasterItemId,
        deleted.BirthDate, -- The old value
        inserted.BirthDate -- The new value
    INTO @ChangedRecords
    WHERE BirthDate > @ThresholdDate;

    SET @UpdatedCount = @@ROWCOUNT;
    PRINT CONCAT(N'Found and updated ', @UpdatedCount, N' records in the transaction.');

    -- Commit or Rollback the transaction based on the DryRun flag
    IF @DryRun = 1
    BEGIN
        PRINT N'DRY RUN MODE: Rolling back the transaction. No changes were saved.';
        ROLLBACK TRANSACTION;
    END
    ELSE
    BEGIN
        PRINT N'LIVE RUN: Committing the transaction to the database.';
        COMMIT TRANSACTION;
    END

    -- Select the captured changes to be returned to the Python script
    PRINT N'Returning the list of changed records for CSV export.';
    SELECT Id, MasterItemId, OldBirthDate, NewBirthDate FROM @ChangedRecords
    ORDER BY OldBirthDate ASC, Id ASC;
""")

# ── 3.  RUN THE UPDATE SCRIPT AND EXPORT RESULTS ─────────────────────────
if __name__ == "__main__":
    logging.info("Starting script to correct birth dates.")
    
    # --- SAFETY WARNINGS ---
    print("="*70)
    print("           *** D A T A B A S E   U P D A T E   S C R I P T ***")
    print("="*70)
    print(f"This script will target records in '{DB_SERVER}.{DB_DATABASE}'.")
    if DRY_RUN:
        print("\nMODE: DRY RUN. The script will NOT make any permanent changes.")
        print("      It will only show what would be changed and create a sample CSV.")
    else:
        print("\nMODE: LIVE RUN. This script WILL permanently change database records.")
        print("      A backup of your database is strongly recommended.")
        
        try:
            confirm = input("\n> To proceed with the LIVE RUN, type 'YES': ").strip().upper()
            if confirm != 'YES':
                logging.warning("User aborted the script. Exiting.")
                sys.exit("Script aborted by user.")
        except KeyboardInterrupt:
            logging.warning("User aborted the script via Ctrl+C. Exiting.")
            sys.exit("\nScript aborted.")
    print("-"*70)

    try:
        # Connect with autocommit OFF to allow the transaction in SQL to be managed
        logging.info(f"Connecting to DB: SERVER={DB_SERVER}, DATABASE={DB_DATABASE}")
        with pyodbc.connect(CONN_STR, autocommit=False) as cn:
            cur = cn.cursor()
            logging.info("Connected. Executing T-SQL to correct records...")
            
            cur.execute(TSQL_CORRECT_RECORDS)
            
            # <-- MODIFIED: Added loop to find the correct result set -->
            rows = []
            column_names = []
            while True:
                # Process any messages (like from PRINT statements)
                while cur.messages:
                    sql_message = cur.messages.pop(0)[1]
                    logging.info(f"SQL PRINT: {sql_message}")

                # Check if the current result is a query with columns
                if cur.description:
                    column_names = [column[0] for column in cur.description]
                    rows = cur.fetchall()
                    # After fetching, we might still have messages related to this result
                    while cur.messages:
                         sql_message = cur.messages.pop(0)[1]
                         logging.info(f"SQL PRINT: {sql_message}")
                
                # Move to the next result set; break if there are no more
                try:
                    if not cur.nextset():
                        break
                except pyodbc.ProgrammingError:
                    # This exception can be raised if there are no more sets.
                    break
            
            if rows:
                num_changed = len(rows)
                log_msg = f"{num_changed} records were {'processed in DRY RUN' if DRY_RUN else 'permanently corrected'}."
                logging.info(log_msg)
                print(f"\n{log_msg}")

                # --- EXPORT TO CSV ---
                try:
                    logging.info(f"Exporting details of {num_changed} records to CSV: {csv_file_full_path}")
                    with open(csv_file_full_path, 'w', newline='', encoding='utf-8') as csv_file:
                        writer = csv.writer(csv_file)
                        writer.writerow(column_names) # Header: Id, MasterItemId, OldBirthDate, NewBirthDate
                        writer.writerows(rows)
                    logging.info("Export successful.")
                    print(f">>> Details of all changed records exported to: {csv_file_full_path} <<<\n")
                except IOError as e:
                    logging.error(f"Failed to write to CSV file: {e}")
            else:
                logging.info("Query executed, but no records were found matching the criteria.")
                print("\nNo records found matching the criteria. No changes were made.")

    except pyodbc.Error as db_ex:
        logging.error(f"A database error occurred: {db_ex}")
        sys.exit("Database error. Check log for details.")
    except Exception as e:
        logging.exception(f"An unexpected Python error occurred: {e}")
        sys.exit("A Python error occurred. Check log for details.")
    finally:
        logging.info("Script finished.")