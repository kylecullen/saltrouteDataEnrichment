import pyodbc
import os # Import the os module to access environment variables
from dotenv import load_dotenv # Make sure this is imported

load_dotenv() # Load .env variables at the very beginning of the script

# --- SQL Server Connection Configuration ---
# IMPORTANT: Update these values with your actual SQL Server details.
# Ensure your .env file has DB_PASSWORD set.
DB_CONFIG = {
    'driver': '{ODBC Driver 17 for SQL Server}', # <<< VERIFY THIS DRIVER NAME ON YOUR SYSTEM
    'server': '.',                               # Matches the '.' (localhost/default instance) or your server name
    'database': 'sa_database_enrichment',        # Your database name
    'uid': 'KC',                                 # Your SQL Server login username
    'pwd': os.environ.get('DB_PASSWORD'),        # Fetched from DB_PASSWORD environment variable
    'TrustServerCertificate': 'yes'              # Added based on 'Trust server certificate' being checked
}

# --- TEMPORARY DEBUG LINE ---
# This will print the first few characters of the password to confirm it's being read.
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
        # Check if the password environment variable is set
        if DB_CONFIG['pwd'] is None:
            print("Error: 'DB_PASSWORD' environment variable is not set. Please set it in your .env file.")
            return None

        # Construct the connection string from the DB_CONFIG dictionary
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

def fetch_random_20_enriched_people_sample(conn): # Renamed for clarity
    """
    Retrieves a random set of 20 people who have a non-empty FullName,
    a Gender, and a Language allocated.
    For spot-checking enriched data.
    """
    if not conn:
        print("No active database connection to fetch data.")
        return []

    cursor = None # Initialize cursor to None
    people_data = []
    try:
        cursor = conn.cursor() # Create cursor inside try
        # SQL query to select a random TOP 20 set of records
        # that have FullName, Gender, and Language allocated.
        query = """
        SELECT TOP 20
            mi.Id,
            mi.FullName,
            mi.Surname,
            g.Description AS Gender,
            l.Description AS Language
        FROM
            dbo.MasterItems AS mi
        INNER JOIN  -- Ensures a matching record exists in Genders
            dbo.Genders AS g ON mi.Id = g.MasterItemId
        INNER JOIN  -- Ensures a matching record exists in Languages
            dbo.Languages AS l ON mi.Id = l.MasterItemId
        WHERE
            mi.FullName IS NOT NULL AND LTRIM(RTRIM(mi.FullName)) <> '' -- Ensures FullName is not NULL and not effectively empty
            -- Optional: If you also want to ensure that the Description fields themselves in Genders/Languages are not empty/null:
            -- AND g.Description IS NOT NULL AND LTRIM(RTRIM(g.Description)) <> ''
            -- AND l.Description IS NOT NULL AND LTRIM(RTRIM(l.Description)) <> ''
        ORDER BY
            NEWID()  -- This will order the rows randomly before picking the TOP 20
        """
        cursor.execute(query)

        rows = cursor.fetchall()

        if rows:
            print("\nFetched Random Sample of Enriched Data (20 People):")
            for row in rows:
                person_id, full_name, surname, gender, language = row
                people_data.append({
                    'id': person_id,
                    'full_name': full_name,
                    'surname': surname,
                    'gender': gender,
                    'language': language
                })
                # With INNER JOINs, gender and language should ideally not be None from the query itself,
                # but keeping the display handling is safe.
                gender_display = gender if gender is not None else "Error:Gender_NULL_Unexpected"
                language_display = language if language is not None else "Error:Language_NULL_Unexpected"
                print(f"ID: {person_id}, Full Name: {full_name}, Surname: {surname}, Gender: {gender_display}, Language: {language_display}")
        else:
            print("No data found matching the criteria (FullName, Gender, and Language allocated).")
            print("This could mean no records are fully enriched yet, or none also meet the FullName criteria.")

    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        print(f"Error fetching data: {sqlstate}. Details: {ex}")
    finally:
        if cursor: # Ensure cursor exists before trying to close
            cursor.close() # Always close the cursor

    return people_data

if __name__ == "__main__":
    # 1. Get a database connection
    connection = get_db_connection()

    if connection:
        # 2. Fetch a random sample of 20 fully enriched people
        fetched_people = fetch_random_20_enriched_people_sample(connection) # Updated function name

        if fetched_people:
            print(f"\nTotal random enriched records fetched for spot check: {len(fetched_people)}")
        else:
            print("\nNo fully enriched records were fetched for the spot check.")
            
        # 3. Close the database connection when done
        connection.close()
        print("\nDatabase connection closed.")
    else:
        print("Could not establish a database connection. Please check DB_CONFIG, .env file, and ODBC driver.")