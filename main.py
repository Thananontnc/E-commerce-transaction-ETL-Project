
import pandas as pd 
import sqlalchemy
import requests
from google.colab import userdata


class Config:
  MYSQL_HOST = userdata.get("MYSQL_HOST")
  MYSQL_PORT = userdata.get("MYSQL_PORT") 
  MYSQL_USER = userdata.get("MYSQL_USER")
  MYSQL_PASSWORD = userdata.get("MYSQL_PASSWORD")
  MYSQL_DB = 'r2de3'
  MYSQL_CHARSET = 'utf8mb4'
  
engine = sqlalchemy.create_engine(
    "mysql+pymysql://{user}:{password}@{host}:{port}/{db}".format(
        user=Config.MYSQL_USER,
        password=Config.MYSQL_PASSWORD,
        host=Config.MYSQL_HOST,
        port=Config.MYSQL_PORT,
        db=Config.MYSQL_DB,
    )
)
with engine.connect() as connection:
    result = connection.execute(sqlalchemy.text(f"Show tables")).fetchall()


def convert_rate(price, rate):
  return price * rate
    
# Pull API (Thai Currency) 
def get_api_data(url):
    r = requests.get(url)
    r = r.json()
    conversion_rate = pd.DataFrame(r)
    conversion_rate = conversion_rate.drop(columns=['id'])
    conversion_rate['date'] = pd.to_datetime(conversion_rate['date'])
    return conversion_rate

def get_data_from_db(db_name,customer_data,transaction_data,product_data,engine,API_data):
    customer = pd.read_sql(f"SELECT * FROM {db_name}.{customer_data}",engine)
    transaction = pd.read_sql(f"SELECT * FROM {db_name}.{transaction_data}",engine)
    product = pd.read_sql(f"SELECT * FROM {db_name}.{product_data}",engine)
    merged_transaction = transaction.merge(product, how="left", left_on="ProductNo", right_on="ProductNo").merge(customer, how="left", left_on="CustomerNo", right_on="CustomerNo")
    final_df = merged_transaction.merge(API_data,how='left',left_on = 'Date' , right_on = 'date')
    return final_df


def Transform_data(final_df):
    final_df["total_amount"] = final_df["Price"] * final_df["Quantity"]
    final_df['thb_amount'] = final_df['total_amount'] * final_df['gbp_thb']

    final_df["thb_amount"] = final_df.apply(lambda row: convert_rate(row["total_amount"], row["gbp_thb"]), axis=1)
    final_df = final_df.drop(["date", "gbp_thb"], axis=1)
    final_df = final_df.drop(["date", "gbp_thb"], axis=1)
    final_df.columns = ['transaction_id', 'date', 'product_id', 'price', 'quantity', 'customer_id',
        'product_name', 'customer_country', 'customer_name', 'total_amount','thb_amount']
    final_df.fillna({
        'CustomerNo':'Unknown'
    },inplace = True)


