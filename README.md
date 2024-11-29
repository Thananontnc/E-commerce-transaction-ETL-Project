# E-commerce-transaction-ETL-Project
## Architecture of this project 

![E-Commerce-Architecture](https://github.com/user-attachments/assets/860c1b86-c3e0-4175-bf0e-de56cfcfeeed)

1. Get Data from MySQL: The MySQL hook retrieves data and stores it in GCS.
2. Get Currency Data via API: The API data is fetched and stored in GCS.
3. Merge Data: All data sources (MySQL and API data) are merged in GCS.
4. Data Transformation: The merged data is transformed and saved to GCS.
5. Load into BigQuery: The final CSV file is loaded into BigQuery.

## Pipeline:
![image](https://github.com/user-attachments/assets/19def3c1-e57d-41fc-bbb6-95e4a2457b7f)

Step-by-step breakdown of the architecture:
MySQL Database:
Get Data from Database (t2): This task fetches data from a MySQL database using MySqlHook and stores the raw data in Google Cloud Storage (GCS) as CSV files (customer.csv, transaction.csv, product.csv).

API Data:
Get Currency Data (t1): This task fetches currency data from an external API, processes it into a CSV file (currency_baht.csv), and stores it in GCS.

Airflow DAG:
Airflow DAG (ETL Pipeline): The DAG orchestrates the workflow by defining the sequence of tasks (t1, t2, t3, t4, t5).

Merge Data:
Merge Data (t3): This task merges the customer, product, transaction, and API data into a single dataset and saves it as a CSV file (customer_cleaned.csv) in GCS.

Transform Data:
Transform Data (t4): The data from customer_cleaned.csv is transformed by calculating total_amount and thb_amount, and it is saved as a final dataset (final_output.csv) in GCS.

Google Cloud Storage:
GCS is used to temporarily store the raw data, cleaned data, and final data files during the ETL process.

BigQuery:
Load Data to BigQuery (t5): The final transformed data (final_output.csv) is loaded into a BigQuery dataset (Retail_Dataset.retail_transaction_customer_table).
