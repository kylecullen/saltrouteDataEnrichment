#!/usr/bin/env python3
"""
view_and_export_matching_birthdates.py
────────────────────────────────────────
Queries records from dbo.BirthDates where BirthDate > THRESHOLD_DATE,
displays a sample, and exports the full result set to a CSV file.

This script is READ-ONLY and does not make any changes to the database.
"""

import os
import sys
import textwrap
import pyodbc
from dotenv import load_dotenv
import logging
from datetime import datetime
import csv  # <-- ADDED: Import the csv module for file export

# ── 0.  CONFIGURATION & LOGGING SETUP ────────────────────────────────────
# --- User Configuration ---
# This is the threshold date. BirthDates AFTER this date will be queried.
CORRECTION_THRESHOLD_DATE = '2008-02-23' # From your SQL query
# --- End User Configuration ---

# Log file setup
log_file_name = f"export_matching_birthdates_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log" # <-- MODIFIED: Log file name updated
log_file_full_path = os.path.join(os.getcwd(), log_file_name)

# <-- ADDED: CSV Output File Configuration -->
csv_file_name = f"birthdate_records_gt_{CORRECTION_THRESHOLD_DATE.replace('-', '')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
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

# ── 2.  THE T-SQL FOR QUERYING RECORDS ───────────────────────────────────
# This T-SQL selects specified columns from dbo.BirthDates based on the threshold.
TSQL_EXPORT_RECORDS = textwrap.dedent(f"""
    USE [{DB_DATABASE}];
    SET NOCOUNT ON;

    DECLARE @ThresholdDate DATE = '{CORRECTION_THRESHOLD_DATE}';

    PRINT N'Selecting records from dbo.BirthDates for viewing and export.';
    PRINT CONCAT(N'Criteria: BirthDate > ''', CONVERT(NVARCHAR(10), @ThresholdDate, 23), N'''');

    SELECT
        bd.[Id],
        bd.[MasterItemId],
        bd.[BirthDate],
        bd.[Age]
        -- Add any other columns from dbo.BirthDates you wish to see here
    FROM dbo.BirthDates bd
    WHERE bd.BirthDate > @ThresholdDate
    ORDER BY bd.BirthDate ASC, bd.[Id] ASC;
""")

# ── 3.  RUN THE QUERY, EXPORT, AND DISPLAY RESULTS ─────────────────────────
if __name__ == "__main__":
    logging.info(f"Starting script to export records from dbo.BirthDates (BirthDate > '{CORRECTION_THRESHOLD_DATE}').")
    
    try:
        logging.info(f"Connecting to DB: SERVER={DB_SERVER}, DATABASE={DB_DATABASE}, USER={DB_USER}")
        with pyodbc.connect(CONN_STR, autocommit=True) as cn:
            logging.info("Connected. Executing T-SQL to fetch records...")
            cur = cn.cursor()
            
            cur.execute(TSQL_EXPORT_RECORDS)

            # Process any PRINT messages that come before the main result set
            while cur.messages:
                sql_message = cur.messages.pop(0)[1]
                logging.info(f"SQL PRINT: {sql_message}")
            
            # Find and process the main query result set
            rows = []
            column_names = []
            found_query_result_set = False
            
            while True:
                if cur.description:  # This indicates a result set with columns
                    column_names = [column[0] for column in cur.description]
                    logging.info(f"Fetching rows for columns: {', '.join(column_names)}")
                    rows = cur.fetchall()
                    found_query_result_set = True
                    break  # Found our data, exit the loop
                
                try:
                    if not cur.nextset():
                        break
                except pyodbc.ProgrammingError:
                    break

            if found_query_result_set:
                if rows:
                    logging.info(f"Successfully fetched {len(rows)} records matching the criteria.")
                    
                    # <-- ADDED: CSV EXPORT LOGIC -->
                    try:
                        logging.info(f"Attempting to export {len(rows)} records to CSV: {csv_file_full_path}")
                        with open(csv_file_full_path, 'w', newline='', encoding='utf-8') as csv_file:
                            writer = csv.writer(csv_file)
                            # Write the header row
                            writer.writerow(column_names)
                            # Write all the data rows
                            writer.writerows(rows)
                        logging.info(f"Successfully exported data to {csv_file_full_path}")
                        print(f"\n>>> Full results have been exported to: {csv_file_full_path} <<<\n")
                    except IOError as e:
                        logging.error(f"Failed to write to CSV file: {e}")
                        print(f"\n[ERROR] Could not write results to CSV file. Check permissions. See log for details.")
                    # <-- END OF ADDED LOGIC -->

                    # The console display now serves as a preview of the exported file
                    print(f"--- Displaying a sample of records (first {min(len(rows), 20)} of {len(rows)} total rows) ---")
                    if column_names:
                        print("\t|\t".join(column_names))
                        print("─" * (sum(len(name) for name in column_names) + (len(column_names) -1) * 3 + 4))
                    for i, row in enumerate(rows):
                        if i < 20:
                            print("\t|\t".join(str(col) if col is not None else "NULL" for col in row))
                        else:
                            break
                    if len(rows) > 20:
                        print(f"... and {len(rows) - 20} more rows not displayed here (see full CSV export).")
                    print("--- End of sample ---")

                else:
                    logging.info("Query executed successfully, but no records were found matching the criteria.")
                    print("\nNo records found matching the criteria. No CSV file was created.") # <-- MODIFIED: Clearer message
            else:
                logging.error("Failed to retrieve a valid result set from the T-SQL query. Check logs for details.")
                print("\n>>> Could not retrieve records. Please check log file. <<<\n")

        logging.info("T-SQL execution and export phase completed.")

    except pyodbc.Error as db_ex:
        error_message = str(db_ex)
        sql_state = getattr(db_ex, 'sqlstate', 'N/A')
        logging.error(f"Database/SQL Error (SQLSTATE: {sql_state}): {error_message}")
        if 'TSQL_EXPORT_RECORDS' in locals():
             logging.error(f"Failing T-SQL block (approx first 300 chars): {TSQL_EXPORT_RECORDS[:300]}")
        sys.exit(1)
    except Exception as e:
        logging.exception(f"An unexpected Python error occurred: {e}")
        sys.exit(1)
    finally:
        logging.info("Script to export matching birth dates finished.")