import pyodbc
import os
from dotenv import load_dotenv

load_dotenv() # Load .env variables at the very beginning of the script

# --- SQL Server Connection Configuration ---
# IMPORTANT: Ensure your .env file has DB_PASSWORD set.
DB_CONFIG = {
    'driver': '{ODBC Driver 17 for SQL Server}', # <<< VERIFY THIS DRIVER NAME ON YOUR SYSTEM
    'server': '.',                               # Or your specific server name/address
    'database': 'sa_database_enrichment',        # Your database name
    'uid': 'KC',                                 # Your SQL Server login username
    'pwd': os.environ.get('DB_PASSWORD'),        # Fetched from DB_PASSWORD environment variable
    'TrustServerCertificate': 'yes'
}

# --- TEMPORARY DEBUG LINE ---
# REMOVE THIS LINE AFTER DEBUGGING FOR SECURITY!
if DB_CONFIG['pwd']:
    print(f"DEBUG: Password (first 5 chars from .env): {DB_CONFIG['pwd'][:5]}*****")
else:
    print("DEBUG: DB_PASSWORD environment variable is NOT set or loaded correctly in script.")
# --- END TEMPORARY DEBUG LINE ---

def get_db_connection():
    """
    Establishes and returns a pyodbc connection to the SQL Server database.
    Handles potential connection errors.
    """
    conn = None
    try:
        if DB_CONFIG['pwd'] is None:
            print("Error: 'DB_PASSWORD' environment variable is not set. Please set it in your .env file.")
            return None

        connection_string = (
            f"DRIVER={DB_CONFIG['driver']};"
            f"SERVER={DB_CONFIG['server']};"
            f"DATABASE={DB_CONFIG['database']};"
            f"UID={DB_CONFIG['uid']};"
            f"PWD={DB_CONFIG['pwd']};"
            f"TrustServerCertificate={DB_CONFIG['TrustServerCertificate']};"
        )
        conn = pyodbc.connect(connection_string)
        print("Successfully connected to the database!")
        return conn
    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        print(f"Database connection error: {sqlstate}. Details: {ex}")
        print("Please ensure your SQL Server is running, the details in DB_CONFIG are correct,")
        print("and the ODBC driver is installed and correctly named.")
        return None

def count_records_with_missing_name_parts(conn):
    """
    Counts and prints the number of records in dbo.MasterItems
    where FullName OR Surname is NULL, an empty string, or consists only of whitespace.
    Returns the count, or None if an error occurs.
    """
    if not conn:
        print("No active database connection to execute the count.")
        return None

    # Query to count records where either FullName or Surname is missing
    query = """
    SELECT COUNT(*) AS MissingNamePartCount
    FROM dbo.MasterItems
    WHERE (FullName IS NULL OR LTRIM(RTRIM(FullName)) = '')
       OR (Surname IS NULL OR LTRIM(RTRIM(Surname)) = '');
    """
    
    cursor = None
    try:
        cursor = conn.cursor()
        print(f"\nExecuting query: {query.strip()}")
        cursor.execute(query)
        result = cursor.fetchone() # COUNT(*) will return one row with one column

        if result:
            count = result[0]
            print(f"-----------------------------------------------------------------")
            print(f"Number of records with missing FullName OR Surname: {count}")
            print(f"-----------------------------------------------------------------")
            return count
        else:
            print("Query executed but no result was returned for the count.")
            return None
            
    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        print(f"Error executing count query: {sqlstate}. Details: {ex}")
        return None
    finally:
        if cursor:
            cursor.close()

if __name__ == "__main__":
    print("Attempting to count records with missing FullName or Surname...")
    
    # 1. Get a database connection
    connection = get_db_connection()

    if connection:
        # 2. Execute the count query
        missing_count = count_records_with_missing_name_parts(connection)
        
        # You could do something with missing_count here if needed,
        # for example, log it or use it in further checks.

        # 3. Close the database connection when done
        connection.close()
        print("\nDatabase connection closed.")
    else:
        print("Could not establish a database connection. Count not performed.")