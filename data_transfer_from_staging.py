"""
Transfer data from staging area to production in PostgreSQL.
"""

# Libraries
from dotenv import load_dotenv
import psycopg2
import os

# Main function
def main():
    load_dotenv()

    # Connect to staging area db
    staging_db_name = os.getenv('staging_db_name')
    staging_db_user = os.getenv('staging_db_user')
    staging_db_password = os.getenv('staging_db_password')
    staging_db_host = os.getenv('staging_db_host')
    staging_db_port = os.getenv('staging_db_port')

    # Connect to target db (production db)
    target_db_name = os.getenv('target_db_name')
    target_db_user = os.getenv('target_db_user')
    target_db_password = os.getenv('target_db_password')
    target_db_host = os.getenv('target_db_host')
    target_db_port = os.getenv('target_db_port')

    # Function to get the latest records from the table in staging area db
    def get_latest_records():
        try:
            staging_connection = psycopg2.connect(
                database=staging_db_name,
                user=staging_db_user,
                password=staging_db_password,
                host=staging_db_host,
                port=staging_db_port
            )
            target_connection = psycopg2.connect(
                database=target_db_name,
                user=target_db_user,
                password=target_db_password,
                host=target_db_host,
                port=target_db_port
            )

            staging_cursor = staging_connection.cursor()
            target_cursor = target_connection.cursor()

            # Fetch existing transaction_id values from the fact table in target db
            target_cursor.execute("SELECT transaction_id FROM fact_sales")
            existing_transaction_id = {row[0] for row in target_cursor.fetchall()}

            # Fetch 100 latest records from staging area db, by excluding existing transaction_id values in target db
            if existing_transaction_id:
                query = "SELECT * FROM staging WHERE transaction_id NOT IN %s LIMIT 100"
                staging_cursor.execute(query, (tuple(existing_transaction_id),))
            else:
                query = "SELECT * FROM staging LIMIT 100"
                staging_cursor.execute(query)

            staging_cursor.execute(query, (tuple(existing_transaction_id),))
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

    # Function to insert the latest records into target db
    def insert_records(records, columns):
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
                """,
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

            # Insert records into sub-dimension tables
            for record in records:
                cursor.execute(sub_dim_insert_queries["dim_customer_gender"], (
                    record[columns.index('customer_gender_id')], record[columns.index('gender_desc')]
                ))
                total_inserted += cursor.rowcount
                cursor.execute(sub_dim_insert_queries["dim_product_type"], (
                    record[columns.index('product_type_id')], record[columns.index('product_type')], record[columns.index('product_category')]
                ))
                total_inserted += cursor.rowcount
                cursor.execute(sub_dim_insert_queries["dim_sales_outlet_city"], (
                    record[columns.index('city_id')], record[columns.index('outlet_city')]
                ))
                total_inserted += cursor.rowcount
                cursor.execute(sub_dim_insert_queries["dim_day_of_week"], (
                    record[columns.index('day_of_week_id')], record[columns.index('day_of_week')]
                ))
                total_inserted += cursor.rowcount
                cursor.execute(sub_dim_insert_queries["dim_month"], (
                    record[columns.index('month_id')], record[columns.index('month_name')]
                ))
                total_inserted += cursor.rowcount
                cursor.execute(sub_dim_insert_queries["dim_year"], (
                    record[columns.index('year_id')], record[columns.index('year')]
                ))
                total_inserted += cursor.rowcount

            # Insert records into dimension tables
            for record in records:
                cursor.execute(dim_insert_queries["dim_customer"], (
                    record[columns.index('customer_id')], record[columns.index('customer_name')], record[columns.index('customer_email')], record[columns.index('card_number')], record[columns.index('customer_gender_id')]
                ))
                total_inserted += cursor.rowcount
                cursor.execute(dim_insert_queries["dim_product"], (
                    record[columns.index('product_id')], record[columns.index('product_name')], record[columns.index('description')], record[columns.index('product_price')], record[columns.index('product_type_id')]
                ))
                total_inserted += cursor.rowcount
                cursor.execute(dim_insert_queries["dim_sales_outlet"], (
                    record[columns.index('sales_outlet_id')], record[columns.index('sales_outlet_type')], record[columns.index('outlet_address')], record[columns.index('city_id')], record[columns.index('outlet_telephone')], record[columns.index('outlet_postal_code')], record[columns.index('outlet_manager')]
                ))
                total_inserted += cursor.rowcount
                cursor.execute(dim_insert_queries["dim_staff"], (
                    record[columns.index('staff_id')], record[columns.index('staff_first_name')], record[columns.index('staff_last_name')], record[columns.index('staff_position')], record[columns.index('staff_location')]
                ))
                total_inserted += cursor.rowcount
                cursor.execute(dim_insert_queries["dim_date"], (
                    record[columns.index('date_id')], record[columns.index('transaction_date')], record[columns.index('day_of_week_id')], record[columns.index('month_id')], record[columns.index('year_id')]
                ))
                total_inserted += cursor.rowcount

            # Insert records into the fact table
            for record in records:
                cursor.execute(dim_insert_queries["fact_sales"], (
                    record[columns.index('transaction_id')], record[columns.index('date_id')], record[columns.index('sales_outlet_id')], record[columns.index('staff_id')], record[columns.index('product_id')], record[columns.index('customer_id')], record[columns.index('quantity')], record[columns.index('price')]
                ))
                total_inserted += cursor.rowcount

            connection.commit()
            print(f"Inserted {total_inserted} new records.")

            cursor.close()
            connection.close()

        except (Exception, psycopg2.DatabaseError) as error:
            print("Error while inserting data", error)

    # Use the above functions to fetch and insert the latest records from staging area db to target db
    new_records, column_names = get_latest_records()
    print("Latest records:", new_records)
    insert_records(new_records, column_names)

if __name__ == "__main__":
    main()
