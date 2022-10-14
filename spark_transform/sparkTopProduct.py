import pandas as pd

from sqlalchemy import create_engine
from pyspark.sql import SparkSession
from pyspark import SparkContext

if __name__ == "__main__":
    uname_db = 'postgres'
    pw_db = 'admin'
    db_dwh = 'final_project_dwh'
    db_mart = 'final_project_mart'
    ip_db = '172.22.109.180'

    try:
        engine_dwh = create_engine(f"postgresql://{uname_db}:{pw_db}@{ip_db}:5432/{db_dwh}")
        engine_mart = create_engine(f"postgresql://{uname_db}:{pw_db}@{ip_db}:5432/{db_mart}")
        print(f"[INFO] Success to Connect to DB")
    except:
        print(f"[INFO] Failed to Connect to DB")

    try:
        spark = SparkSession.builder \
            .master('spark://DESKTOP-8AHJDS9.localdomain:7077') \
                .appName('TopProduct') \
                    .getOrCreate()
                    
        print(f"[INFO] Success connect SPARK ENGINE .....")
    except:
        print(f"[INFO] Failed Can't SPARK ENGINE .....")

    # try:
    print(f"[INFO] Service ETL is Running .....")
    #read from sql
    df = pd.read_sql_query('SELECT * FROM bigdata_transaction', con=engine_dwh)
    print(f"{df}")

    # spark processing
    # sparkDF = spark.createDataFrame(df)
    # print(f"{sparkDF}")
    # print(sparkDF.groupBy("product_transaction").sum("amount_transaction").limit(5))
        # .toPandas() \
        #     .to_sql(f"top_product", con=engine_mart, if_exists = 'append', index=False)
                # .to_csv(f"/mnt/d/digitalskola/prj8/live/spark_transform/output.csv", index=False)

    #     print(f"[INFO] Service ETL is Success .....")
    # except:
    #     print(f"[INFO] Service ETL is Failed .....")
    