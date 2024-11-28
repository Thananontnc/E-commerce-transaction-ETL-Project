import pandas as pd 
import sqlalchemy


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

customer = pd.read_sql("SELECT * FROM r2de3.customer",engine)
transaction = pd.read_sql("SELECT * FROM r2de3.transaction",engine)
