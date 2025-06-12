import pyodbc
import os
from dotenv import load_dotenv
from openai import AsyncOpenAI # Use AsyncOpenAI for asyncio
import json
import asyncio
import time # For timing the process

load_dotenv()

# --- SQL Server Connection Configuration ---
DB_CONFIG = {
    'driver': '{ODBC Driver 17 for SQL Server}',
    'server': '.',
    'database': 'sa_database_enrichment',
    'uid': 'KC',
    'pwd': os.environ.get('DB_PASSWORD'),
    'TrustServerCertificate': 'yes'
}

# --- TEMPORARY DEBUG LINE ---
if DB_CONFIG['pwd']:
    print(f"DEBUG: Password (first 5 chars from .env): {DB_CONFIG['pwd'][:5]}*****")
else:
    print("DEBUG: DB_PASSWORD environment variable is NOT set or loaded correctly in script.")
# --- END TEMPORARY DEBUG LINE ---

# --- OpenAI API Client Initialization (Async) ---
try:
    async_client = AsyncOpenAI()
except Exception as e:
    print(f"Error initializing AsyncOpenAI client: {e}. Ensure OPENAI_API_KEY is set.")
    async_client = None

# --- Constants ---
DB_BATCH_SIZE = 10000      # Number of records to fetch and process in one DB batch

# V V V V V V V V V V V V V V V V V V V V V V V V V V V V V V V V V V V V V V V
# --- NEW VARIABLE: SET THE TOTAL NUMBER OF RECORDS TO PROCESS ---
# Set to a number (e.g., 50000) to limit total processing, or None to process all eligible records.
MAX_TOTAL_RECORDS_TO_PROCESS = None # Example: 50000 or None
# A A A A A A A A A A A A A A A A A A A A A A A A A A A A A A A A A A A A A A A

API_CALL_CONCURRENCY = 2000
RETRY_ATTEMPTS = 3
RETRY_DELAY_SECONDS = 5
INTER_BATCH_DELAY_SECONDS = 2

# --- Asynchronous Prediction Function (Optimized with Retries) ---
async def get_person_prediction_async(first_name: str, last_name: str, item_id: int):
    # ... (This function remains the same as in the previous version) ...
    if not async_client:
        return {"item_id": item_id, "error": "OpenAI client not initialized."}
    if not first_name or not last_name:
        return {"item_id": item_id, "error": "Missing first name or last name."}

    for attempt in range(RETRY_ATTEMPTS):
        raw_response_content = None
        try:
            response_object = await async_client.responses.create(
                model="gpt-4.1-nano",
                input=[
                    {
                        "role": "system",
                        "content": [{"type": "input_text", "text": (
                            "You are an expert linguist and an expert in name-based gender detection. "
                            "Given the first name and last name, determine the most likely first language, "
                            "gender, and a numerical confidence score for the person, assuming they are from South Africa. "
                            "Respond with a JSON object strictly adhering to the provided schema. The JSON object must include "
                            "'language' (chosen from the official South African languages specified in the schema's enum), "
                            "'gender' (as 'FEMALE' or 'MALE' as specified in the schema's enum), "
                            "and 'confidence' (a numerical score indicating the confidence level of the prediction, ideally between 0.0 and 1.0)."
                        )}]
                    },
                    {
                        "role": "user",
                        "content": [{"type": "input_text", "text": f"first_name: {first_name} last_name: {last_name}"}]
                    }
                ],
                text={"format": {"type": "json_schema", "name": "person_prediction", "strict": True,
                      "schema": {
                          "type": "object",
                          "properties": {
                              "language": {"type": "string", "description": "Most likely first language",
                                           "enum": ["Afrikaans", "English", "isiNdebele", "isiXhosa", "isiZulu", "Sepedi", "Sesotho", "Setswana", "siSwati", "Tshivenda", "Xitsonga"]},
                              "gender": {"type": "string", "description": "Most likely gender", "enum": ["FEMALE", "MALE"]},
                              "confidence": {"type": "number", "description": "Prediction confidence (e.g., 0.0 to 1.0)"}
                          }, "required": ["language", "gender", "confidence"], "additionalProperties": False}}},
                reasoning={}, tools=[], temperature=0.7, max_output_tokens=150, top_p=1, store=True
            )

            if response_object and response_object.output and response_object.output[0].content:
                raw_response_content = response_object.output[0].content[0].text
                prediction_data = json.loads(raw_response_content)
                return {"item_id": item_id, "prediction": prediction_data}
            else:
                error_msg = "Unexpected response structure from OpenAI API."
                if attempt < RETRY_ATTEMPTS - 1:
                    print(f"    Retrying ID {item_id}: {error_msg} (Attempt {attempt + 1}/{RETRY_ATTEMPTS})")
                else: 
                    return {"item_id": item_id, "error": error_msg}

        except json.JSONDecodeError as e:
            error_msg = f"JSONDecodeError: {e}. Raw: {raw_response_content}"
            if attempt < RETRY_ATTEMPTS - 1:
                print(f"    Retrying ID {item_id}: {error_msg} (Attempt {attempt + 1}/{RETRY_ATTEMPTS})")
            else:
                return {"item_id": item_id, "error": error_msg}
        except Exception as e: 
            error_msg = f"API Error: {type(e).__name__} - {e}"
            if attempt < RETRY_ATTEMPTS - 1:
                print(f"    Retrying ID {item_id}: {error_msg} (Attempt {attempt + 1}/{RETRY_ATTEMPTS})")
            else:
                return {"item_id": item_id, "error": error_msg}
        
        await asyncio.sleep(RETRY_DELAY_SECONDS * (attempt + 1)) 
    return {"item_id": item_id, "error": "All retry attempts failed."}


# --- Database Connection Function (remains synchronous) ---
def get_db_connection():
    # ... (This function remains the same) ...
    conn = None
    if DB_CONFIG['pwd'] is None:
        print("Error: 'DB_PASSWORD' environment variable is not set.")
        return None
    try:
        connection_string = (
            f"DRIVER={DB_CONFIG['driver']};"
            f"SERVER={DB_CONFIG['server']};"
            f"DATABASE={DB_CONFIG['database']};"
            f"UID={DB_CONFIG['uid']};"
            f"PWD={DB_CONFIG['pwd']};"
            f"TrustServerCertificate={DB_CONFIG['TrustServerCertificate']};"
        )
        conn = pyodbc.connect(connection_string)
        return conn
    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        print(f"Database connection error: {sqlstate}. Details: {ex}")
        return None

# --- Batch Database Update Functions (Optimized with MERGE) ---
def batch_upsert_genders(conn, gender_data_list):
    # ... (This function remains the same) ...
    if not conn or not gender_data_list:
        return 0 
    cursor = None
    try:
        cursor = conn.cursor()
        cursor.execute("""
            IF OBJECT_ID('tempdb..#TempGenders') IS NOT NULL DROP TABLE #TempGenders;
            CREATE TABLE #TempGenders (MasterItemId INT PRIMARY KEY, Description NVARCHAR(50));
        """)
        insert_sql = "INSERT INTO #TempGenders (MasterItemId, Description) VALUES (?, ?)"
        cursor.fast_executemany = True
        cursor.executemany(insert_sql, gender_data_list)
        
        merge_sql = """
            MERGE dbo.Genders AS target
            USING #TempGenders AS source ON target.MasterItemId = source.MasterItemId
            WHEN MATCHED AND target.Description <> source.Description THEN 
                UPDATE SET target.Description = source.Description
            WHEN NOT MATCHED BY TARGET THEN 
                INSERT (MasterItemId, Description) VALUES (source.MasterItemId, source.Description);
        """
        rows_affected = cursor.execute(merge_sql).rowcount
        conn.commit()
        print(f"    Successfully upserted/merged {len(gender_data_list)} gender records. Rows affected in target: {rows_affected if rows_affected != -1 else 'Unknown (check DB)'}.")
        return rows_affected if rows_affected != -1 else len(gender_data_list) 
    except pyodbc.Error as ex:
        print(f"    Error batch upserting genders: {ex}")
        if conn.autocommit == False: conn.rollback()
        return 0
    finally:
        if cursor: cursor.close()

def batch_upsert_languages(conn, language_data_list):
    # ... (This function remains the same) ...
    if not conn or not language_data_list:
        return 0
    cursor = None
    try:
        cursor = conn.cursor()
        cursor.execute("""
            IF OBJECT_ID('tempdb..#TempLanguages') IS NOT NULL DROP TABLE #TempLanguages;
            CREATE TABLE #TempLanguages (MasterItemId INT PRIMARY KEY, Description NVARCHAR(50));
        """)
        insert_sql = "INSERT INTO #TempLanguages (MasterItemId, Description) VALUES (?, ?)"
        cursor.fast_executemany = True
        cursor.executemany(insert_sql, language_data_list)
        
        merge_sql = """
            MERGE dbo.Languages AS target
            USING #TempLanguages AS source ON target.MasterItemId = source.MasterItemId
            WHEN MATCHED AND target.Description <> source.Description THEN 
                UPDATE SET target.Description = source.Description
            WHEN NOT MATCHED BY TARGET THEN 
                INSERT (MasterItemId, Description, DateCreated) VALUES (source.MasterItemId, source.Description, GETDATE());
        """
        rows_affected = cursor.execute(merge_sql).rowcount
        conn.commit()
        print(f"    Successfully upserted/merged {len(language_data_list)} language records. Rows affected in target: {rows_affected if rows_affected != -1 else 'Unknown (check DB)'}.")
        return rows_affected if rows_affected != -1 else len(language_data_list)
    except pyodbc.Error as ex:
        print(f"    Error batch upserting languages: {ex}")
        if conn.autocommit == False: conn.rollback()
        return 0
    finally:
        if cursor: cursor.close()

# --- Main Asynchronous Processing Function for Batches ---
async def process_master_items_in_batches_async():
    if not async_client:
        print("Cannot proceed: AsyncOpenAI client failed to initialize.")
        return

    last_processed_id = 0 
    # RENAMED and USED for MAX_TOTAL_RECORDS_TO_PROCESS logic
    cumulative_records_fetched_for_processing = 0 
    total_api_predictions_successful = 0
    total_api_errors = 0
    total_genders_upserted = 0
    total_languages_upserted = 0
    batch_number = 0

    if MAX_TOTAL_RECORDS_TO_PROCESS is not None and MAX_TOTAL_RECORDS_TO_PROCESS <= 0:
        print(f"MAX_TOTAL_RECORDS_TO_PROCESS is set to {MAX_TOTAL_RECORDS_TO_PROCESS}. No records will be processed.")
        return

    print(f"Starting data enrichment process. DB Batch Size: {DB_BATCH_SIZE}, API Concurrency: {API_CALL_CONCURRENCY}")
    if MAX_TOTAL_RECORDS_TO_PROCESS is not None:
        print(f"Maximum total records to process in this run: {MAX_TOTAL_RECORDS_TO_PROCESS}")
    else:
        print("Processing all eligible records (no maximum total limit set).")


    while True:
        # --- Check against MAX_TOTAL_RECORDS_TO_PROCESS before fetching a new batch ---
        if MAX_TOTAL_RECORDS_TO_PROCESS is not None and \
           cumulative_records_fetched_for_processing >= MAX_TOTAL_RECORDS_TO_PROCESS:
            print(f"Reached MAX_TOTAL_RECORDS_TO_PROCESS limit of {MAX_TOTAL_RECORDS_TO_PROCESS}. Stopping.")
            break

        # --- Calculate how many records to fetch in the current batch ---
        records_to_fetch_this_batch = DB_BATCH_SIZE
        if MAX_TOTAL_RECORDS_TO_PROCESS is not None:
            remaining_to_hit_max = MAX_TOTAL_RECORDS_TO_PROCESS - cumulative_records_fetched_for_processing
            records_to_fetch_this_batch = min(DB_BATCH_SIZE, remaining_to_hit_max)
        
        if records_to_fetch_this_batch <= 0: # Should be caught by the check above, but as a safeguard
            print("No more records to fetch based on MAX_TOTAL_RECORDS_TO_PROCESS. Stopping.")
            break
        # --- End calculation for records to fetch ---

        batch_number += 1
        print(f"\n--- Starting Batch {batch_number} (processing records after ID: {last_processed_id}) ---")
        print(f"  Attempting to fetch up to {records_to_fetch_this_batch} records for this batch.")
        start_time_batch_overall = time.time()
        
        conn = get_db_connection()
        if not conn:
            print(f"Failed to get DB connection for Batch {batch_number}. Retrying in 60s...")
            await asyncio.sleep(60)
            continue
        
        print(f"  Successfully connected to database for Batch {batch_number}.")
        fetch_cursor = None
        rows_in_current_batch = [] # To store actual fetched rows
        
        try:
            fetch_cursor = conn.cursor()
            query = f"""
                SELECT TOP (?)
                    mi.Id,
                    mi.FullName,
                    mi.Surname
                FROM
                    dbo.MasterItems AS mi
                LEFT JOIN
                    dbo.Genders AS g ON mi.Id = g.MasterItemId
                LEFT JOIN
                    dbo.Languages AS l ON mi.Id = l.MasterItemId
                WHERE
                    (g.MasterItemId IS NULL OR l.MasterItemId IS NULL) 
                    AND mi.Id > ? 
                    AND mi.FullName IS NOT NULL
                    AND LTRIM(RTRIM(mi.FullName)) <> ''
                    AND mi.Surname IS NOT NULL
                    AND LTRIM(RTRIM(mi.Surname)) <> ''
                ORDER BY
                    mi.Id ASC;
            """
            fetch_cursor.execute(query, records_to_fetch_this_batch, last_processed_id)
            rows_in_current_batch = fetch_cursor.fetchall()
            
            if not rows_in_current_batch:
                print("No more records to process that meet the criteria from the database.")
                break 

            print(f"  Fetched {len(rows_in_current_batch)} records in Batch {batch_number} for API processing.")
            # This counter tracks records *selected* from DB for processing in this run
            cumulative_records_fetched_for_processing += len(rows_in_current_batch) 
            
            tasks = []
            for db_row_data in rows_in_current_batch:
                item_id, full_name, surname = db_row_data
                processed_full_name = full_name.strip() if isinstance(full_name, str) else ""
                processed_surname = surname.strip() if isinstance(surname, str) else ""

                if not processed_full_name or not processed_surname:
                    print(f"    Skipping ID: {item_id} due to missing name components.")
                    continue
                tasks.append(get_person_prediction_async(processed_full_name, processed_surname, item_id))

            if not tasks:
                print(f"  No valid API tasks created for Batch {batch_number}. Moving to next potential batch.")
                if rows_in_current_batch: last_processed_id = rows_in_current_batch[-1].Id 
                if fetch_cursor: fetch_cursor.close()
                if conn: conn.close()
                print(f"  Updated last_processed_id to: {last_processed_id}")
                if INTER_BATCH_DELAY_SECONDS > 0: await asyncio.sleep(INTER_BATCH_DELAY_SECONDS)
                continue
            
            print(f"  Processing {len(tasks)} API calls concurrently for Batch {batch_number}...")
            start_time_api_calls = time.time()
            api_results = []
            for i in range(0, len(tasks), API_CALL_CONCURRENCY):
                chunk = tasks[i:i+API_CALL_CONCURRENCY]
                api_results.extend(await asyncio.gather(*chunk))
                if len(tasks) > API_CALL_CONCURRENCY and i + API_CALL_CONCURRENCY < len(tasks):
                    print(f"    Processed {len(api_results)}/{len(tasks)} API calls for current DB batch, brief pause...")
                    await asyncio.sleep(1) 
            api_calls_duration = time.time() - start_time_api_calls
            print(f"  API calls for Batch {batch_number} completed in {api_calls_duration:.2f}s.")

            genders_to_upsert = []
            languages_to_upsert = []
            current_batch_api_success = 0
            current_batch_api_errors = 0

            for result in api_results:
                item_id = result["item_id"]
                if "prediction" in result:
                    current_batch_api_success +=1
                    prediction = result["prediction"]
                    predicted_gender = prediction.get("gender")
                    predicted_language = prediction.get("language")
                    
                    if predicted_gender:
                        genders_to_upsert.append((item_id, predicted_gender))
                    if predicted_language:
                        languages_to_upsert.append((item_id, predicted_language))
                else:
                    current_batch_api_errors += 1
                    print(f"    API Error for ID {item_id} in Batch {batch_number}: {result.get('error', 'Unknown error')}")
            
            total_api_predictions_successful += current_batch_api_success
            total_api_errors += current_batch_api_errors

            if genders_to_upsert:
                affected_g = batch_upsert_genders(conn, genders_to_upsert)
                total_genders_upserted += affected_g if isinstance(affected_g, int) else len(genders_to_upsert) 
            if languages_to_upsert:
                affected_l = batch_upsert_languages(conn, languages_to_upsert)
                total_languages_upserted += affected_l if isinstance(affected_l, int) else len(languages_to_upsert)

            if rows_in_current_batch: last_processed_id = rows_in_current_batch[-1].Id 
            print(f"  Updated last_processed_id to: {last_processed_id}")
            # TODO: For true resumability across script restarts, persistently save last_processed_id here.

            batch_overall_duration = time.time() - start_time_batch_overall
            print(f"--- Batch {batch_number} completed in {batch_overall_duration:.2f}s. ---")
            print(f"  Summary for Batch {batch_number}: API Successes: {current_batch_api_success}, API Errors: {current_batch_api_errors}")
            print(f"  Cumulative records selected for processing so far: {cumulative_records_fetched_for_processing}")


        except pyodbc.Error as db_ex:
            print(f"Database error during Batch {batch_number} processing: {db_ex}. Check connection and query.")
            await asyncio.sleep(30) 
        except Exception as e:
            print(f"Unexpected error in Batch {batch_number} processing loop: {e}")
            await asyncio.sleep(10)
        finally:
            if fetch_cursor:
                fetch_cursor.close()
            if conn:
                conn.close()
                print(f"  Database connection for Batch {batch_number} closed.")
        
        if INTER_BATCH_DELAY_SECONDS > 0 and rows_in_current_batch: 
            print(f"Pausing for {INTER_BATCH_DELAY_SECONDS}s before next batch...")
            await asyncio.sleep(INTER_BATCH_DELAY_SECONDS)

    print("\n--- Overall Processing Summary ---")
    final_batch_count = batch_number -1 if not rows_in_current_batch and batch_number > 0 else batch_number 
    print(f"Total DB Batches attempted: {final_batch_count}")
    print(f"Total records selected from DB for processing in this run: {cumulative_records_fetched_for_processing}")
    print(f"Total successful API predictions: {total_api_predictions_successful}")
    print(f"Total API errors encountered: {total_api_errors}")
    print(f"Total gender records effectively upserted/merged: {total_genders_upserted}")
    print(f"Total language records effectively upserted/merged: {total_languages_upserted}")
    print("Processing finished.")


if __name__ == "__main__":
    if not async_client:
        print("Exiting: AsyncOpenAI client failed to initialize.")
    else:
        try:
            asyncio.run(process_master_items_in_batches_async())
        except KeyboardInterrupt:
            print("\nProcess interrupted by user. Exiting.")
        except Exception as e:
            print(f"Unhandled exception in main execution: {e}")