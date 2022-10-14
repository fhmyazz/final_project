#!python3

import pandas as pd

from sqlalchemy import create_engine

if __name__ == "__main__":
    uname_db = 'postgres'
    pw_db = 'admin'
    db_name = 'final_project_dwh'
    ip_db = '172.31.16.43'

    try:
        engine = create_engine(f"postgresql://{uname_db}:{pw_db}@{ip_db}:5432/{db_name}")
        print(f"[INFO] Success to Connect to DB")
    except:
        print(f"[INFO] Failed to Connect to DB")

    list_filename = ['customer', 'product', 'transaction']

    for file in list_filename:
        print(f"[INFO] Inprogress Dump {file}")
        pd.read_csv(f"bigdata_{file}.csv").to_sql(f"bigdata_{file}", con=engine, if_exists = 'append', index=False)
        print(f"[INFO] Success Dump {file} file")