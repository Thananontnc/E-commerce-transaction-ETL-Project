import pandas as pd 
import sqlalchemy
import requests


class Config:
  MYSQL_HOST = userdata.get("MYSQL_HOST")
  MYSQL_PORT = userdata.get("MYSQL_PORT") 
  MYSQL_USER = userdata.get("MYSQL_USER")
  MYSQL_PASSWORD = userdata.get("MYSQL_PASSWORD")
  MYSQL_DB = 'YOUR_DB'
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
url = "https://r2de3-currency-api-vmftiryt6q-as.a.run.app/gbp_thb"
r = requests.get(url)
result_conversion_rate = r.json()
convert_rate =  pd.DataFrame(result_conversion_rate)

#FROM SQL TO DATAFRAME
customer = pd.read_sql("SELECT * FROM r2de3.customer",engine)
transaction = pd.read_sql("SELECT * FROM r2de3.transaction",engine)
product = pd.read_sql("SELECT * FROM r2de3.product",engine)

merged_transaction = transaction.merge(product, how="left", left_on="ProductNo", right_on="ProductNo").merge(customer, how="left", left_on="CustomerNo", right_on="CustomerNo")

conversion_rate = conversion_rate.drop(columns=['id'])
conversion_rate['date'] = pd.to_datetime(conversion_rate['date'])

final_df = merged_transaction.merge(conversion_rate,how='left',left_on = 'Date' , right_on = 'date')
final_df["total_amount"] = final_df["Price"] * final_df["Quantity"]
final_df['thb_amount'] = final_df['total_amount'] * final_df['gbp_thb']

final_df["thb_amount"] = final_df.apply(lambda row: convert_rate(row["total_amount"], row["gbp_thb"]), axis=1)
final_df = final_df.drop(["date", "gbp_thb"], axis=1)
final_df = final_df.drop(["date", "gbp_thb"], axis=1)
final_df.columns = ['transaction_id', 'date', 'product_id', 'price', 'quantity', 'customer_id',
       'product_name', 'customer_country', 'customer_name', 'total_amount','thb_amount']