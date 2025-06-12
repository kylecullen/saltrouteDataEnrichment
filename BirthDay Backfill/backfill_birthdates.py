#!/usr/bin/env python3
"""
backfill_birthdates_diag_v5.py
──────────────────────
Populate dbo.BirthDates for every voter who is still missing a BirthDate.
(v5: T-SQL returns total counts via SELECT statement for reliability)
Runs completely inside SQL Server in batches.
"""

import os
import sys
import textwrap
import pyodbc
from dotenv import load_dotenv
import logging
from datetime import datetime

# ── 0.  LOGGING SETUP ───────────────────────────────────────────────────
log_file_name = f"backfill_birthdates_diag_v5_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
log_file_full_path = os.path.join(os.getcwd(), log_file_name)

print(f"--- Python script: Will attempt to create log file at: {log_file_full_path} ---")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_full_path, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

# ── 1.  CONNECTION DETAILS ──────────────────────────────────────────────
DB_DRIVER   = '{ODBC Driver 17 for SQL Server}'
DB_SERVER   = '.'
DB_DATABASE = 'sa_database_enrichment'
DB_USER     = 'KC'

load_dotenv()
DB_PASSWORD = os.getenv('DB_PASSWORD')
if not DB_PASSWORD:
    logging.error("DB_PASSWORD missing in environment / .env file")
    sys.exit("DB_PASSWORD missing in environment / .env file")

CONN_STR = (
    f"DRIVER={DB_DRIVER};"
    f"SERVER={DB_SERVER};"
    f"DATABASE={DB_DATABASE};"
    f"UID={DB_USER};"
    f"PWD={DB_PASSWORD};"
    f"TrustServerCertificate=yes;"
)

# ── 2.  THE SET-BASED, BATCHED T-SQL (Returns Counts via SELECT) ──
BATCH_SIZE = 100_000

TSQL = textwrap.dedent(f"""
    USE {DB_DATABASE};
    SET NOCOUNT ON;

    DECLARE @BatchSize int = {BATCH_SIZE};
    -- @pivotYY will be calculated inline

    PRINT 'T-SQL: Starting v5 DIAGNOSTIC (counts via SELECT)'; -- This simple PRINT might still come through

    ---------------------------------------------------------------
    -- Phase 1: INSERT rows that don’t exist
    ---------------------------------------------------------------
    -- PRINT 'T-SQL: Starting INSERT Phase'; -- Minimal internal prints
    DECLARE @i_v5 int = 1, @totalInserted_v5 int = 0;
    WHILE @i_v5 > 0
    BEGIN
        SET IDENTITY_INSERT dbo.BirthDates ON;
        INSERT INTO dbo.BirthDates (Id, MasterItemId, BirthDate)
        SELECT TOP (@BatchSize)
                mi.Id,
                mi.Id,
                TRY_CONVERT(date,
                    CASE WHEN LEFT(mi.IDNumber,2) > RIGHT(CONVERT(char(4), YEAR(GETDATE())), 2)
                         THEN '19'+LEFT(mi.IDNumber,6)
                         ELSE '20'+LEFT(mi.IDNumber,6) END) AS DOB
        FROM   dbo.MasterItems mi
        LEFT   JOIN dbo.BirthDates bd ON bd.Id = mi.Id
        WHERE  bd.Id IS NULL
          AND  mi.IDNumber LIKE '[0-9][0-9][0-9][0-9][0-9][0-9]%'
          AND  TRY_CONVERT(date,
                CASE WHEN LEFT(mi.IDNumber,2) > RIGHT(CONVERT(char(4), YEAR(GETDATE())), 2)
                     THEN '19'+LEFT(mi.IDNumber,6)
                     ELSE '20'+LEFT(mi.IDNumber,6) END) IS NOT NULL
        ORDER BY mi.Id;

        SET @i_v5 = @@ROWCOUNT;
        SET @totalInserted_v5 = @totalInserted_v5 + @i_v5;
        SET IDENTITY_INSERT dbo.BirthDates OFF;

        IF @i_v5 = 0 BEGIN BREAK; END
    END
    -- PRINT CONCAT('T-SQL: INSERT Phase finished. Total inserted in this run: ', @totalInserted_v5); -- Replaced by SELECT

    ---------------------------------------------------------------
    -- Phase 2: UPDATE rows whose BirthDate is still NULL
    ---------------------------------------------------------------
    -- PRINT 'T-SQL: Starting UPDATE Phase'; -- Minimal internal prints
    DECLARE @u_v5 int = 1, @totalUpdated_v5 int = 0;
    WHILE @u_v5 > 0
    BEGIN
        UPDATE bd
        SET bd.BirthDate = TRY_CONVERT(date,
                                CASE WHEN LEFT(mi.IDNumber,2) > RIGHT(CONVERT(char(4), YEAR(GETDATE())), 2)
                                     THEN '19'+LEFT(mi.IDNumber,6)
                                     ELSE '20'+LEFT(mi.IDNumber,6) END)
        FROM dbo.BirthDates bd
        JOIN (
            SELECT TOP (@BatchSize)
                    m.Id
            FROM dbo.MasterItems m
            JOIN dbo.BirthDates b ON b.Id = m.Id
            WHERE b.BirthDate IS NULL
              AND m.IDNumber LIKE '[0-9][0-9][0-9][0-9][0-9][0-9]%'
              AND TRY_CONVERT(date,
                    CASE WHEN LEFT(m.IDNumber,2) > RIGHT(CONVERT(char(4), YEAR(GETDATE())), 2)
                         THEN '19'+LEFT(m.IDNumber,6)
                         ELSE '20'+LEFT(m.IDNumber,6) END) IS NOT NULL
            ORDER BY m.Id
        ) AS todo ON todo.Id = bd.Id
        JOIN dbo.MasterItems mi ON mi.Id = bd.Id;

        SET @u_v5 = @@ROWCOUNT;
        SET @totalUpdated_v5 = @totalUpdated_v5 + @u_v5;

        IF @u_v5 = 0 BEGIN BREAK; END
    END
    -- PRINT CONCAT('T-SQL: UPDATE Phase finished. Total updated in this run: ', @totalUpdated_v5); -- Replaced by SELECT

    -- PRINT 'T-SQL: v5 DIAGNOSTIC finished. Returning counts.'; -- Replaced by SELECT
    SELECT @totalInserted_v5 AS TotalInsertedInRun, @totalUpdated_v5 AS TotalUpdatedInRun;
""")

# ── 3.  RUN IT AND FETCH RESULTS ─────────────────────────────────
if __name__ == "__main__":
    logging.info("Diagnostic script started (v5).")
    try:
        logging.info(f"Attempting to connect to DB: SERVER={DB_SERVER}, DATABASE={DB_DATABASE}, USER={DB_USER}")
        with pyodbc.connect(CONN_STR, autocommit=True) as cn:
            logging.info("Connected - running diagnostic back-fill (v5)...")
            cur = cn.cursor()
            
            # The entire TSQL block is one logical statement for pyodbc if not split by "GO"
            # Here, we expect it to be one block.
            logging.info(f"Executing T-SQL block...")
            try:
                cur.execute(TSQL) # TSQL now ends with a SELECT statement

                # Attempt to fetch the results of the SELECT statement
                results_fetched = False
                try:
                    row = cur.fetchone()
                    if row:
                        logging.info(f"T-SQL execution summary: TotalInsertedInRun = {row.TotalInsertedInRun}, TotalUpdatedInRun = {row.TotalUpdatedInRun}")
                        results_fetched = True
                    else:
                        logging.warning("T-SQL execution did not return a result row for counts.")
                except pyodbc.ProgrammingError as pe:
                    # This can happen if no results are returned (e.g. if TSQL had an error before SELECT)
                    logging.warning(f"Could not fetch T-SQL summary results: {pe}. This might be normal if TSQL had no SELECT output.")


                # Process any informational PRINT messages (though we don't expect many critical ones now)
                # It's important to process messages even if results are fetched,
                # or sometimes pyodbc can get into a strange state with multiple result sets / messages.
                # However, if fetchone() worked, nextset() might be needed if there were PRINTs *before* SELECT.
                # For simplicity here, we'll try to clear messages after attempting to fetch.
                # If there were PRINTs *before* the final SELECT, they might need `cur.nextset()` to clear before `fetchone()` for the SELECT.
                # Let's assume PRINTs are minimal and don't interfere with the final SELECT's resultset.
                
                # Simplified message processing:
                # After execute, if there are messages, process them.
                # The SELECT result is handled by fetchone().
                # If there were PRINTs *after* the SELECT, nextset() would be needed.
                # This ordering might need adjustment if PRINTs and SELECT results mix in complex ways.

                logging.info("Attempting to process any remaining SQL PRINT messages...")
                processed_messages = False
                while True:
                    try:
                        # Process messages from the current result set (if any from before SELECT)
                        while cur.messages:
                            sql_message = cur.messages.pop(0)[1]
                            logging.info(f"SQL PRINT (current_result_set): {sql_message}")
                            processed_messages = True
                        
                        # Move to the next result set (if any). This also clears PRINT messages
                        # that might be between result sets or after the last one.
                        if not cur.nextset():
                            break # No more result sets
                    except pyodbc.ProgrammingError:
                        # This can happen if there are no more results/messages to process.
                        logging.info("No more result sets or messages to process from pyodbc.")
                        break
                
                if not results_fetched and not processed_messages:
                    logging.info("No summary results fetched and no SQL PRINT messages processed after T-SQL execution.")


            except pyodbc.Error as stmt_ex:
                sqlstate = stmt_ex.args[0]
                error_message = stmt_ex.args[1]
                logging.error(f"SQL Error during T-SQL block execution ({sqlstate}): {error_message}")
                logging.error(f"Start of T-SQL block (approx first 200 chars): {TSQL[:200]}")
                raise 
        
        logging.info("T-SQL block execution attempted. Review logs for summary and any T-SQL PRINT output.")

    except pyodbc.Error as db_ex:
        sqlstate = db_ex.args[0]
        error_message = db_ex.args[1]
        logging.error(f"Database/SQL Error ({sqlstate}): {error_message}")
        sys.exit(1)
    except Exception as e:
        logging.exception(f"An unexpected Python error occurred: {e}")
        sys.exit(1)
    finally:
        logging.info("Diagnostic script finished (v5).")