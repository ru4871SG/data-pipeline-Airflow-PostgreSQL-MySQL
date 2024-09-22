"""
Check if the transaction_data table exists in MySQL, and if it does, print the first 5 rows.
If the table doesn't exist, raise an AirflowException.
"""

# Libraries
from dotenv import load_dotenv
import mysql.connector
import os
from airflow.exceptions import AirflowException

# Function to check if the table exists and get the first 5 rows from the transaction_data table in MySQL
def check_table_and_get_first_five_rows(mysql_connection):
    try:
        cursor = mysql_connection.cursor(dictionary=True)
        
        # Check if the table exists
        cursor.execute("SHOW TABLES LIKE 'transaction_data'")
        result = cursor.fetchone()
        if not result:
            raise AirflowException("Table 'transaction_data' does not exist in the database.")
        
        # If the table exists, proceed to fetch the first 5 rows
        query = "SELECT * FROM transaction_data LIMIT 5"
        cursor.execute(query)

        records = cursor.fetchall()
        if records:
            print(f"Found {len(records)} records.")
            for record in records:
                print(record)
        else:
            print("No records found in the transaction_data table.")
            raise AirflowException("Table 'transaction_data' does not have any records.")

        cursor.close()

    except mysql.connector.Error as error:
        raise AirflowException(f"Error while fetching data from MySQL: {error}")

# Main function
def main():
    load_dotenv()

    try:
        mysql_connection = mysql.connector.connect(
            database=os.getenv('mysql_db_name'),
            user=os.getenv('mysql_db_user'),
            password=os.getenv('mysql_db_password'),
            host=os.getenv('mysql_db_host'),
            port=os.getenv('mysql_db_port')
        )
        print("Connected to MySQL successfully.")

        check_table_and_get_first_five_rows(mysql_connection)

    except mysql.connector.Error as error:
        raise AirflowException(f"Error while connecting to MySQL: {error}")
    
    finally:
        if 'mysql_connection' in locals() and mysql_connection.is_connected():
            mysql_connection.close()
            print("MySQL connection closed.")

if __name__ == "__main__":
    main()
