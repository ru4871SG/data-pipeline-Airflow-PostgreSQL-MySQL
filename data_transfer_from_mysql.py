"""
Transfer data from MySQL to staging area in PostgreSQL.
"""

# Libraries
from dotenv import load_dotenv
import mysql.connector
import os
import psycopg2


# Function to get the last transaction_id from the staging area in PostgreSQL
def get_last_transaction_id(staging_db_name, staging_db_user, staging_db_password, staging_db_host, staging_db_port):
    try:
        connection = psycopg2.connect(
            database=staging_db_name,
            user=staging_db_user,
            password=staging_db_password,
            host=staging_db_host,
            port=staging_db_port
        )
        print("Connected to PostgreSQL successfully.")

        cursor = connection.cursor()
        cursor.execute("SELECT max(transaction_id) FROM staging")

        last_transaction_id = cursor.fetchone()[0]
        if last_transaction_id is not None:
            print(f"Last transaction_id in staging from PostgreSQL: {last_transaction_id}")
        else:
            print("No rows found in staging.")

        cursor.close()
        connection.close()

        return last_transaction_id

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while fetching data from PostgreSQL", error)

# Function to get the latest records from MySQL
def get_latest_records(mysql_connection, last_transaction_id):
    try:
        cursor = mysql_connection.cursor(dictionary=True)
        query = "SELECT * FROM transaction_data WHERE transaction_id > %s LIMIT 100"
        cursor.execute(query, (last_transaction_id,))

        records = cursor.fetchall()
        if records:
            print(f"Found {len(records)} new records.")
        else:
            print("No new records found.")

        cursor.close()

        return records

    except mysql.connector.Error as error:
        print("Error while fetching data from MySQL", error)


# Function to insert the new records into PostgreSQL
def insert_records(records, staging_db_name, staging_db_user, staging_db_password, staging_db_host, staging_db_port):
    try:
        connection = psycopg2.connect(
            database=staging_db_name,
            user=staging_db_user,
            password=staging_db_password,
            host=staging_db_host,
            port=staging_db_port
        )
        print("Connected to PostgreSQL successfully for insertion.")

        cursor = connection.cursor()

        insert_query = """
        INSERT INTO staging (
            transaction_id, 
            transaction_date, 
            sales_outlet_id, 
            staff_id, 
            customer_id, 
            sales_detail_id, 
            product_id, 
            quantity, 
            price, 
            staff_first_name, 
            staff_last_name, 
            staff_position, 
            staff_location, 
            sales_outlet_type, 
            outlet_address, 
            outlet_city, 
            outlet_telephone, 
            outlet_postal_code, 
            outlet_manager, 
            customer_name, 
            customer_email, 
            card_number, 
            gender_desc, 
            product_name, 
            description, 
            product_price, 
            product_type_id, 
            product_type, 
            product_category, 
            customer_gender_id, 
            city_id, 
            month_id, 
            year, 
            day_of_week_id, 
            day_of_week, 
            month_name, 
            year_id, 
            date_id
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (transaction_id) DO NOTHING
        """
        for record in records:
            cursor.execute(insert_query, (
                record['transaction_id'],
                record['transaction_date'],
                record['sales_outlet_id'],
                record['staff_id'],
                record['customer_id'],
                record['sales_detail_id'],
                record['product_id'],
                record['quantity'],
                record['price'],
                record['staff_first_name'],
                record['staff_last_name'],
                record['staff_position'],
                record['staff_location'],
                record['sales_outlet_type'],
                record['outlet_address'],
                record['outlet_city'],
                record['outlet_telephone'],
                record['outlet_postal_code'],
                record['outlet_manager'],
                record['customer_name'],
                record['customer_email'],
                record['card_number'],
                record['gender_desc'],
                record['product_name'],
                record['description'],
                record['product_price'],
                record['product_type_id'],
                record['product_type'],
                record['product_category'],
                record['customer_gender_id'],
                record['city_id'],
                record['month_id'],
                record['year'],
                record['day_of_week_id'],
                record['day_of_week'],
                record['month_name'],
                record['year_id'],
                record['date_id']
            ))

        connection.commit()
        print(f"Inserted {len(records)} records into PostgreSQL.")

        cursor.close()
        connection.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while inserting data into PostgreSQL", error)


# Main function
def main():
    load_dotenv()

    # Connect to MySQL
    mysql_connection = mysql.connector.connect(
        database=os.getenv('mysql_db_name'),
        user=os.getenv('mysql_db_user'),
        password=os.getenv('mysql_db_password'),
        host=os.getenv('mysql_db_host'),
        port=os.getenv('mysql_db_port')
    )
    print("Connected to MySQL successfully.")

    # PostgreSQL (Staging Area) connection details
    staging_db_name = os.getenv('staging_db_name')
    staging_db_user = os.getenv('staging_db_user')
    staging_db_password = os.getenv('staging_db_password')
    staging_db_host = os.getenv('staging_db_host')
    staging_db_port = os.getenv('staging_db_port')

    # Get the last transaction_id from PostgreSQL
    last_transaction_id = get_last_transaction_id(staging_db_name, staging_db_user, staging_db_password, staging_db_host, staging_db_port)

    # Get the latest records from MySQL
    new_records = get_latest_records(mysql_connection, last_transaction_id)
    print("Latest records:", new_records)

    # Insert the new records into PostgreSQL
    insert_records(new_records, staging_db_name, staging_db_user, staging_db_password, staging_db_host, staging_db_port)
    print("New rows inserted into PostgreSQL = ", len(new_records))

    # Close MySQL connection
    mysql_connection.close()

if __name__ == "__main__":
    main()
