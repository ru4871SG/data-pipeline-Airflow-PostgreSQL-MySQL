# Automated Data Pipeline Using Airflow

This repository contains multiple scripts that you can use to automate data transfer from MySQL to your data warehouse in PostgreSQL. I include the Airflow DAG script here `data_transfer_airflow.py`, which you can use in Airflow to automate the other two scripts: `data_transfer_from_mysql.py` (to transfer data from MySQL to Staging Area database in PostgreSQL), and `data_transfer_from_staging.py` (to transfer data from the Staging Area to the Production data warehouse in PostgreSQL).

The repository also include the SQL scripts that you should execute before running the Airflow DAG script. The original data is also included in a CSV format inside the `transaction_data` folder.

## Project Structure

The project is structured as follows:

- `01_create_table_mysql.sql`: Execute this SQL script to create the transaction_data table in MySQL
- `02_update_empty_string_mysql.sql`: SQL script to update empty strings in MySQL, so they will become NULL. You should only execute this script after uploading the initial data into your MySQL database.
- `03_create_table_staging_area_postgre.sql`: SQL script to create the table in the staging area in PostgreSQL.
- `04_insert_initial_records_postgre.sql`: SQL script to insert initial records into the staging area in PostgreSQL.
- `05_create_snowflake_schema_postgre.sql`: SQL script to create the tables in a Snowflake schema for the production data warehouse in PostgreSQL.
- `data_transfer_from_mysql.py`: Python script to transfer data from MySQL to staging area in PostgreSQL.
- `data_transfer_from_staging.py`: Python script to transfer data from the staging area into the production data warehouse in PostgreSQL.
- `data_transfer_airflow.py`: Airflow DAG script to automate the entire data pipeline using Airflow.

The `transaction_data` folder contains the CSV file which should be used to fill the table in your MySQL database.

## How to Use

1. Ensure you have MySQL, PostgreSQL, and Airflow installed and running on your machine.

2. In MySQL, create a new database, and execute `01_create_table_mysql.sql` inside it. Make sure to include the database name that you choose in your .env file (check `.env.example` for the structure)

3. Use PHPMyAdmin to import the included CSV file `transaction_data.csv` (inside the `transaction_data` folder) into your transaction_data table that's created by the firt SQL script above.

4. Once your transaction_data table in MySQL is filled with the data from the CSV file, you can proceed to execute `02_update_empty_string_mysql.sql`, which should update all the empty string values to become actually NULL values.

5. Go to PostgreSQL, and create a database for the staging area. Inside that database, execute `03_create_table_staging_area_postgre.sql` and `04_insert_initial_records_postgre.sql`. The staging area database name should be included in your .env file.

6. Next, create the production data warehouse, and execute `05_create_snowflake_schema_postgre.sql`, which will create your tables in a snowflake schema. And just like step 2 and 5 above, you should also include the production database name in your .env file (under target_db_name).

7. Once you are done with all the 6 steps above, you can then place the Airflow DAG script `data_transfer_airflow.py` into the "dags" subfolder of your Airflow folder. Make sure to change the sys.path to the folder where you store the other Python scripts locally (`data_transfer_from_mysql.py` and `data_transfer_from_staging.py`). Also, if the DAG script file has permission issues (after you move it), you have to change its permission with `sudo chmod 777 data_transfer_airflow.py`

8. If you see no issues with the DAG script file in Airflow, you can simply unpause it with the command `airflow dags unpause data_transfer_airflow`. You can also unpause the DAG directly using the Airflow UI. Make sure both the Airflow server and scheduler are running properly. That's it! Every 3 minutes you will see 100 new records being transferred from MySQL to the staging area database in PostgreSQL, and then 100 records will also be transferred from the staging area into the production warehouse inside PostgreSQL itself. 

Everything is automated once the DAG is up and running! If you want to speed up the data transfer process, feel free to edit the schedule interval in the airflow DAG script. You can also increase the transfer limit (from 100 records) inside `data_transfer_from_mysql.py` and `data_transfer_from_staging.py`