import pyodbc
import os # Import the os module to access environment variables
from dotenv import load_dotenv # Make sure this is imported

load_dotenv() # Load .env variables at the very beginning of the script

# --- SQL Server Connection Configuration ---
# IMPORTANT: Update these values with your actual SQL Server details.
# Values below are pre-filled based on your provided SSMS login screenshot and database structure.
DB_CONFIG = {
    'driver': '{ODBC Driver 17 for SQL Server}', # <<< VERIFY THIS DRIVER NAME ON YOUR VM
    'server': '.',                              # Matches the '.' in your SSMS screenshot (localhost/default instance)
    'database': 'sa_database_enrichment',       # UPDATED based on your database structure image
    'uid': 'KC',                                # Matches the 'Login' from your SSMS screenshot
    'pwd': os.environ.get('DB_PASSWORD'),       # <<< NOW FETCHED FROM ENVIRONMENT VARIABLE
    'TrustServerCertificate': 'yes'             # Added based on 'Trust server certificate' being checked
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
            print("Error: 'DB_PASSWORD' environment variable is not set. Please set it before running the script.")
            return None

        # Construct the connection string from the DB_CONFIG dictionary
        # Added TrustServerCertificate to the connection string
        connection_string = (
            f"DRIVER={DB_CONFIG['driver']};"
            f"SERVER={DB_CONFIG['server']};"
            f"DATABASE={DB_CONFIG['database']};"
            f"UID={DB_CONFIG['uid']};"
            f"PWD={DB_CONFIG['pwd']};"
            f"TrustServerCertificate={DB_CONFIG['TrustServerCertificate']};" # Include this for trusted connections
        )
        conn = pyodbc.connect(connection_string)
        print("Successfully connected to the database!")
        return conn
    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        print(f"Database connection error: {sqlstate}. Details: {ex}")
        return None

def fetch_first_20_people(conn):
    """
    Retrieves the first 20 people's ID, full name, surname, gender, and language
    from the 'dbo.MasterItems' table by joining with dbo.Genders and dbo.Languages.
    """
    if not conn:
        print("No active database connection to fetch data.")
        return []

    cursor = conn.cursor()
    people_data = []
    try:
        # SQL query to select Id, FullName, Surname, Gender (from dbo.Genders), and Language (from dbo.Languages)
        # using LEFT JOINs to include gender and language descriptions.
        query = """
        SELECT TOP 1000
            mi.Id,
            mi.FullName,
            mi.Surname,
            g.Description AS Gender,
            l.Description AS Language
        FROM
            dbo.MasterItems AS mi
        LEFT JOIN
            dbo.Genders AS g ON mi.Id = g.MasterItemId
        LEFT JOIN
            dbo.Languages AS l ON mi.Id = l.MasterItemId
        ORDER BY
            mi.Id ASC
        """
        cursor.execute(query)

        # Fetch all the results
        rows = cursor.fetchall()

        if rows:
            print("\nFetched Data (First 20 People):")
            for row in rows:
                # Access columns by index (0 for Id, 1 for FullName, 2 for Surname, 3 for Gender, 4 for Language)
                person_id, full_name, surname, gender, language = row
                people_data.append({
                    'id': person_id,
                    'full_name': full_name,
                    'surname': surname,
                    'gender': gender,
                    'language': language
                })
                print(f"ID: {person_id}, Full Name: {full_name}, Surname: {surname}, Gender: {gender}, Language: {language}")
        else:
            print("No data found in the 'dbo.MasterItems' table.")

    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        print(f"Error fetching data: {sqlstate}. Details: {ex}")
    finally:
        cursor.close() # Always close the cursor

    return people_data

if __name__ == "__main__":
    # 1. Get a database connection
    connection = get_db_connection()

    if connection:
        # 2. Fetch the first 20 people
        fetched_people = fetch_first_20_people(connection)

        # You can now work with 'fetched_people' list, for example:
        # print("\nTotal records fetched:", len(fetched_people))

        # 3. Close the database connection when done
        connection.close()
        print("\nDatabase connection closed.")
    else:
        print("Could not establish a database connection. Please check DB_CONFIG and ODBC driver.")
