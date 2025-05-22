import pyodbc
import os
from dotenv import load_dotenv

load_dotenv() # Load .env variables

# --- SQL Server Connection Configuration (same as your other scripts) ---
DB_CONFIG = {
    'driver': '{ODBC Driver 17 for SQL Server}', # VERIFY THIS DRIVER NAME ON YOUR VM
    'server': '.',                              # Use your actual server name if not localhost default instance
    'database': 'sa_database_enrichment',       # Your database name
    'uid': 'KC',                                # Your SQL login username
    'pwd': os.environ.get('DB_PASSWORD'),       # Fetched from environment variable
    'TrustServerCertificate': 'yes'             # If your server uses a self-signed certificate
}

def get_db_connection():
    """
    Establishes and returns a pyodbc connection to the SQL Server database.
    Handles potential connection errors.
    """
    conn = None
    if DB_CONFIG['pwd'] is None:
        print("Error: 'DB_PASSWORD' environment variable is not set. Please set it before running the script.")
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
        print("Successfully connected to the database.")
        return conn
    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        print(f"Database connection error: {sqlstate}. Details: {ex}")
        return None

def count_records_missing_both_gender_and_language():
    """
    Connects to the database and counts records in dbo.MasterItems
    that have neither a gender nor a language entry.
    """
    conn = get_db_connection()
    if not conn:
        # get_db_connection already prints an error
        return

    cursor = None
    count = 0
    try:
        cursor = conn.cursor()
        # SQL query to count records missing entries in BOTH Genders and Languages tables
        query = """
            SELECT COUNT(mi.Id) AS MissingBothCount
            FROM
                dbo.MasterItems AS mi
            LEFT JOIN
                dbo.Genders AS g ON mi.Id = g.MasterItemId
            LEFT JOIN
                dbo.Languages AS l ON mi.Id = l.MasterItemId
            WHERE
                g.MasterItemId IS NULL AND l.MasterItemId IS NULL;
        """
        
        print("\nExecuting query to count records missing both gender and language...")
        cursor.execute(query)
        result = cursor.fetchone() # COUNT(*) will return one row with one column
        
        if result:
            count = result[0]
            print(f"\n>>> Number of records in 'dbo.MasterItems' missing BOTH a gender AND a language entry: {count}\n")
        else:
            # This case should ideally not happen with a COUNT(*) query unless there's a severe issue
            print("Query did not return a result for the count.")

    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        print(f"Database query error: {sqlstate}. Details: {ex}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    # Optional: Check if DB_PASSWORD is loaded, useful for debugging .env setup
    if not DB_CONFIG['pwd']:
        print("Warning: DB_PASSWORD environment variable might not be loaded correctly.")
        # The get_db_connection function will handle the error if it's truly missing.
    
    count_records_missing_both_gender_and_language()