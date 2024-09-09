"""
Print the first 5 rows from the transaction_data table in MySQL.
"""

# Libraries
from dotenv import load_dotenv
import mysql.connector
import os


# Function to get the first 5 rows from the transaction_data table in MySQL
def get_first_five_rows(mysql_connection):
    try:
        cursor = mysql_connection.cursor(dictionary=True)
        query = "SELECT * FROM transaction_data LIMIT 5"
        cursor.execute(query)

        records = cursor.fetchall()
        if records:
            print(f"Found {len(records)} records.")
            for record in records:
                print(record)
        else:
            print("No records found.")

        cursor.close()

    except mysql.connector.Error as error:
        print("Error while fetching data from MySQL", error)


# Main function
def main():
    load_dotenv()

    mysql_connection = mysql.connector.connect(
        database=os.getenv('mysql_db_name'),
        user=os.getenv('mysql_db_user'),
        password=os.getenv('mysql_db_password'),
        host=os.getenv('mysql_db_host'),
        port=os.getenv('mysql_db_port')
    )
    print("Connected to MySQL successfully.")

    get_first_five_rows(mysql_connection)

    mysql_connection.close()

if __name__ == "__main__":
    main()
