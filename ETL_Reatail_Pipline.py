from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
import pandas as pd
import requests
from datetime import datetime,timedelta

MYSQL_CONNECTION = "mysql_default"
API_raw_out_path = '/home/airflow/gcs/data/raw/currency_baht.csv'
customer_raw_output_path = "/home/airflow/gcs/data/raw/customer.csv"
product_raw_output_path = "/home/airflow/gcs/data/raw/product.csv"
transaction_raw_output_path = "/home/airflow/gcs/data/raw/transaction.csv"
customer_cleaned_output_path = "/home/airflow/gcs/data/cleaned/customer.csv"
final_output_path = "/home/airflow/gcs/data/cleaned/final_output.csv"

def convert_rate(price, rate):
  return price * rate

def get_api_data(out_put_path):
    try:
        r = requests.get('API_LINK', timeout=10)
        r.raise_for_status()  
        data = r.json()
        conversion_rate = pd.DataFrame(data)
        conversion_rate = conversion_rate.drop(columns=['id'])
        conversion_rate['date'] = pd.to_datetime(conversion_rate['date'])
        conversion_rate.to_csv(out_put_path, index=False)
    except requests.exceptions.RequestException as e:
        raise ValueError(f"API request failed: {e}")

def get_data_from_db(customer_raw_path,transaction_raw_path,product_raw_path):
    mysqlserver = MySqlHook(MYSQL_CONNECTION)
    customer_raw = mysqlserver.get_pandas_df(sql = "SELECT * FROM DB.customer")
    transaction_raw = mysqlserver.get_pandas_df(sql = 'SELECT * FROM DB.transaction')
    product_raw = mysqlserver.get_pandas_df(sql = "SELECT * FROM DB.product")
    customer_raw.to_csv(customer_raw_path,index = False)
    transaction_raw.to_csv(transaction_raw_path,index = False)
    product_raw.to_csv(product_raw_path,index=False)

def merge_data(customer_raw_path, transaction_raw_path, product_raw_path, api_data_path, output_file_path):
    customer = pd.read_csv(customer_raw_path)
    transaction = pd.read_csv(transaction_raw_path)
    product = pd.read_csv(product_raw_path)
    api_data = pd.read_csv(api_data_path)

    merged_transaction = transaction.merge(product, how="left", on="ProductNo") \
                                    .merge(customer, how="left", on="CustomerNo")
    final_df = merged_transaction.merge(api_data, how="left", left_on="Date", right_on="date")
    final_df.to_csv(output_file_path, index=False)


def transform_data(final_df_path, final_output_path):
    final_df = pd.read_csv(final_df_path)
    final_df["total_amount"] = final_df["Price"] * final_df["Quantity"]
    final_df["thb_amount"] = final_df["total_amount"] * final_df["gbp_thb"]

    final_df = final_df.drop(["date", "gbp_thb"], axis=1)
    final_df.columns = ['transaction_id', 'date', 'product_id', 'price', 'quantity', 'customer_id',
        'product_name', 'customer_country', 'customer_name', 'total_amount', 'thb_amount']
    final_df.to_csv(final_output_path, index=False)


default_args = {
    'owner': 'Pie',
    'start_date': datetime(2024, 11, 29),
    'depends_on_past': False,
    'email': ['EMAIL'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="Retail_Transaction_Product_Customer_ETL_Pipeline",
    default_args=default_args,
    description='ETL Pipeline for Retail data',
    schedule_interval='@hourly',
    catchup=False,
    concurrency=10,
    max_active_runs=2,
) as dag:
    
    t1 = PythonOperator(
        task_id = "get_currency_data",
        python_callable = get_api_data,
        op_kwargs = {
            'out_put_path': API_raw_out_path
        }
    )
    
    t2 = PythonOperator(
        task_id = "Get_data_from_database",
        python_callable = get_data_from_db,
        op_kwargs = {
            'customer_raw_path':customer_raw_output_path,
            'transaction_raw_path':transaction_raw_output_path,
            'product_raw_path':product_raw_output_path
        }
    )
    
    t3 = PythonOperator(
        task_id="Merge_Data_database_with_API_data",
        python_callable=merge_data,
        op_kwargs={
            'customer_raw_path': customer_raw_output_path,
            'transaction_raw_path': transaction_raw_output_path,
            'product_raw_path': product_raw_output_path,
            'api_data_path': API_raw_out_path,
            'output_file_path': customer_cleaned_output_path,
        }
    )

    t4 = PythonOperator(
        task_id="transform_data_then_load",
        python_callable=transform_data,
        op_kwargs={
            'final_df_path': customer_cleaned_output_path,
            'final_output_path': final_output_path,
        },
    )

    t5 = BashOperator(
        task_id="load_to_bigquery",
        bash_command=(
            "bq load --source_format=CSV --autodetect "
            "Retail_Dataset.retail_transaction_customer_table "
            "gs://COMPOSER_BUCKET/data/cleaned/final_output.csv"
        ),
    )

t1 >> t2 >> t3 >> t4 >> t5




