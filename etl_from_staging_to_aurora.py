"""
ETL Process from Staging Area to Target Database in PostgreSQL (Amazon Aurora)
"""

# Libraries
import psycopg2
import os

from dotenv import load_dotenv
from psycopg2 import extras

# Function to get the latest records from the table in staging area db
def get_latest_records(staging_db_name, staging_db_user, staging_db_password, staging_db_host, staging_db_port,
                       target_db_name, target_db_user, target_db_password, target_db_host, target_db_port):
    try:
        print(f"Attempting to connect to staging database at {staging_db_host}:{staging_db_port}")
        staging_connection = psycopg2.connect(
            database=staging_db_name,
            user=staging_db_user,
            password=staging_db_password,
            host=staging_db_host,
            port=staging_db_port
        )
        print("Successfully connected to staging database")

        print(f"Attempting to connect to target database at {target_db_host}:{target_db_port}")
        target_connection = psycopg2.connect(
            database=target_db_name,
            user=target_db_user,
            password=target_db_password,
            host=target_db_host,
            port=target_db_port,
            sslmode='require'
        )
        print("Successfully connected to target database")

        staging_cursor = staging_connection.cursor()
        target_cursor = target_connection.cursor()

        # Fetch existing transaction_id values from the fact table in target db
        target_cursor.execute("SELECT transaction_id FROM fact_sales")
        existing_transaction_id = {row[0] for row in target_cursor.fetchall()}

        # Fetch 100 latest records from staging area db, by excluding existing transaction_id values in target db
        if existing_transaction_id:
            query = "SELECT * FROM staging WHERE transaction_id NOT IN %s LIMIT 20000"
            staging_cursor.execute(query, (tuple(existing_transaction_id),))
        else:
            query = "SELECT * FROM staging LIMIT 20000"
            staging_cursor.execute(query)

        records = staging_cursor.fetchall()
        column_names = [desc[0] for desc in staging_cursor.description]

        staging_cursor.close()
        staging_connection.close()
        target_cursor.close()
        target_connection.close()

        return records, column_names

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while fetching data from staging area", error)
        return [], []


# Transform function to mask PII data in the records
def pii_masking(records, columns):
    email_index = columns.index('customer_email')
    masked_records = []
    
    for record in records:
        record_list = list(record)
        email = record_list[email_index]
        
        if email is None or '@' not in email:
            masked_email = '***@****.***'  # Default mask for None or invalid emails
        else:
            try:
                username, domain = email.split('@')
                domain_parts = domain.split('.')
                
                masked_username = '***'
                masked_domain = '****'
                masked_extension = '***' if len(domain_parts) > 1 else ''
                
                if masked_extension:
                    masked_email = f"{masked_username}@{masked_domain}.{masked_extension}"
                else:
                    masked_email = f"{masked_username}@{masked_domain}"
            except Exception as e:
                print(f"Error masking email: {email}. Error: {str(e)}")
                masked_email = '***@****.***'  # Fallback mask for any errors
        
        record_list[email_index] = masked_email
        masked_records.append(tuple(record_list))
    
    return masked_records


# Function to insert the latest records into the target db in Amazon Aurora with chunk size of 1000
def insert_records(records, columns, target_db_name, target_db_user, target_db_password, target_db_host, target_db_port, chunk_size=1000):
    total_inserted = 0
    try:
        connection = psycopg2.connect(
            database=target_db_name,
            user=target_db_user,
            password=target_db_password,
            host=target_db_host,
            port=target_db_port
        )
        cursor = connection.cursor()

        # Insert queries for sub-dimension tables
        sub_dim_insert_queries = {
            "dim_customer_gender": """
            INSERT INTO dim_customer_gender (customer_gender_id, gender_desc)
            VALUES (%s, %s)
            ON CONFLICT (customer_gender_id) DO NOTHING
            RETURNING customer_gender_id
            """,
            "dim_product_type": """
            INSERT INTO dim_product_type (product_type_id, product_type, product_category)
            VALUES (%s, %s, %s)
            ON CONFLICT (product_type_id) DO NOTHING
            RETURNING product_type_id
            """,
            "dim_sales_outlet_city": """
            INSERT INTO dim_sales_outlet_city (city_id, outlet_city)
            VALUES (%s, %s)
            ON CONFLICT (city_id) DO NOTHING
            RETURNING city_id
            """,
            "dim_day_of_week": """
            INSERT INTO dim_day_of_week (day_of_week_id, day_of_week)
            VALUES (%s, %s)
            ON CONFLICT (day_of_week_id) DO NOTHING
            RETURNING day_of_week_id
            """,
            "dim_month": """
            INSERT INTO dim_month (month_id, month_name)
            VALUES (%s, %s)
            ON CONFLICT (month_id) DO NOTHING
            RETURNING month_id
            """,
            "dim_year": """
            INSERT INTO dim_year (year_id, year)
            VALUES (%s, %s)
            ON CONFLICT (year_id) DO NOTHING
            RETURNING year_id
            """
        }

        # Insert queries for dimension tables and fact table
        dim_insert_queries = {
            "dim_customer": """
            INSERT INTO dim_customer (customer_id, customer_name, customer_email, card_number, customer_gender_id)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (customer_id) DO NOTHING
            RETURNING customer_id
            """,
            "dim_product": """
            INSERT INTO dim_product (product_id, product_name, description, product_price, product_type_id)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (product_id) DO NOTHING
            RETURNING product_id
            """,
            "dim_sales_outlet": """
            INSERT INTO dim_sales_outlet (sales_outlet_id, sales_outlet_type, outlet_address, city_id, outlet_telephone, outlet_postal_code, outlet_manager)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (sales_outlet_id) DO NOTHING
            RETURNING sales_outlet_id
            """,
            "dim_staff": """
            INSERT INTO dim_staff (staff_id, staff_first_name, staff_last_name, staff_position, staff_location)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (staff_id) DO NOTHING
            RETURNING staff_id
            """,
            "dim_date": """
            INSERT INTO dim_date (date_id, transaction_date, day_of_week_id, month_id, year_id)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (date_id) DO NOTHING
            RETURNING date_id
            """,
            "fact_sales": """
            INSERT INTO fact_sales (transaction_id, date_id, sales_outlet_id, staff_id, product_id, customer_id, quantity, price)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (transaction_id) DO NOTHING
            RETURNING transaction_id
            """
        }

        # Function to process chunks
        def process_chunks(data, query):
            nonlocal total_inserted
            for i in range(0, len(data), chunk_size):
                chunk = data[i:i + chunk_size]
                extras.execute_batch(cursor, query, chunk)
                total_inserted += len(chunk)
                connection.commit()

        # Prepare data for sub-dimension tables
        sub_dim_data = {
            "dim_customer_gender": [(r[columns.index('customer_gender_id')], r[columns.index('gender_desc')]) for r in records],
            "dim_product_type": [(r[columns.index('product_type_id')], r[columns.index('product_type')], r[columns.index('product_category')]) for r in records],
            "dim_sales_outlet_city": [(r[columns.index('city_id')], r[columns.index('outlet_city')]) for r in records],
            "dim_day_of_week": [(r[columns.index('day_of_week_id')], r[columns.index('day_of_week')]) for r in records],
            "dim_month": [(r[columns.index('month_id')], r[columns.index('month_name')]) for r in records],
            "dim_year": [(r[columns.index('year_id')], r[columns.index('year')]) for r in records]
        }

        # Insert records into sub-dimension tables
        for table, data in sub_dim_data.items():
            process_chunks(data, sub_dim_insert_queries[table])

        # Prepare data for dimension tables and fact table
        dim_data = {
            "dim_customer": [(r[columns.index('customer_id')], r[columns.index('customer_name')], r[columns.index('customer_email')], r[columns.index('card_number')], r[columns.index('customer_gender_id')]) for r in records],
            "dim_product": [(r[columns.index('product_id')], r[columns.index('product_name')], r[columns.index('description')], r[columns.index('product_price')], r[columns.index('product_type_id')]) for r in records],
            "dim_sales_outlet": [(r[columns.index('sales_outlet_id')], r[columns.index('sales_outlet_type')], r[columns.index('outlet_address')], r[columns.index('city_id')], r[columns.index('outlet_telephone')], r[columns.index('outlet_postal_code')], r[columns.index('outlet_manager')]) for r in records],
            "dim_staff": [(r[columns.index('staff_id')], r[columns.index('staff_first_name')], r[columns.index('staff_last_name')], r[columns.index('staff_position')], r[columns.index('staff_location')]) for r in records],
            "dim_date": [(r[columns.index('date_id')], r[columns.index('transaction_date')], r[columns.index('day_of_week_id')], r[columns.index('month_id')], r[columns.index('year_id')]) for r in records],
            "fact_sales": [(r[columns.index('transaction_id')], r[columns.index('date_id')], r[columns.index('sales_outlet_id')], r[columns.index('staff_id')], r[columns.index('product_id')], r[columns.index('customer_id')], r[columns.index('quantity')], r[columns.index('price')]) for r in records]
        }

        # Insert records into dimension tables and fact table
        for table, data in dim_data.items():
            process_chunks(data, dim_insert_queries[table])

        print(f"Inserted {total_inserted} new records.")

        cursor.close()
        connection.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while inserting data", error)


# Main function
def main():
    load_dotenv()

    # Connect to staging area db
    staging_db_name = os.getenv('staging_db_name')
    staging_db_user = os.getenv('staging_db_user')
    staging_db_password = os.getenv('staging_db_password')
    staging_db_host = os.getenv('staging_db_host')
    staging_db_port = os.getenv('staging_db_port')

    # Connect to target db in Amazon Aurora
    target_db_name = os.getenv('target_db_name_aurora')
    target_db_user = os.getenv('target_db_user_aurora')
    target_db_password = os.getenv('target_db_password_aurora')
    target_db_host = os.getenv('target_db_host_aurora')
    target_db_port = os.getenv('target_db_port_aurora')

    # Use the above functions to fetch and insert the latest records from staging area db to target db
    new_records, column_names = get_latest_records(
        staging_db_name, staging_db_user, staging_db_password, staging_db_host, staging_db_port,
        target_db_name, target_db_user, target_db_password, target_db_host, target_db_port
    )
    print("Latest records:", new_records)

    # Apply PII masking
    masked_records = pii_masking(new_records, column_names)

    # Insert the transformed records into the target db
    insert_records(masked_records, column_names, target_db_name, target_db_user, target_db_password, target_db_host, target_db_port)

if __name__ == "__main__":
    main()
