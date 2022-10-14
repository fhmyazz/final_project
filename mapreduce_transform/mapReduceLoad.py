#!python3

import pandas as pd
import csv

from sqlalchemy import create_engine

if __name__ == "__main__":
    uname_db = 'postgres'
    pw_db = 'admin'
    db_name = 'final_project_mart'
    ip_db = '172.22.109.180'

    try:
        engine = create_engine(f"postgresql://{uname_db}:{pw_db}@{ip_db}:5432/{db_name}")
        print(f"[INFO] Success to Connect to DB")
    except:
        print(f"[INFO] Failed to Connect to DB")

    def csv_addheader(file)
        with open(f"{file}.csv",newline='') as f:
            r = csv.reader(f)
            data = [line for line in r]
        with open(f"{file}.csv",'w',newline='') as f:
            w = csv.writer(f)
            w.writerow(['value'])
            w.writerows(data)

    def csv_readline(line):
        """Given a sting CSV line, return a list of strings."""
        for row in csv.reader([line]):
            return row

    customer = pd.read_csv(f"mapReduceCountCustomer.csv")
    # customer.columns=['test']
    customer = pd.DataFrame(customer, columns=['value'])
    # customer[0].astype("string").str.split("\t", n = 1, expand = True)
    print(customer)
    # customer.columns=['gender_customer','total_customer']
    # customer.to_sql(f"mapReduceCountCustomer", con=engine, if_exists = 'append', index=False)

    # product = pd.read_csv(f"mapReduceCountProduct.csv")
    # product.columns=['name_product','total_product']
    # product.to_sql(f"mapReduceCountProduct", con=engine, if_exists = 'append', index=False)

    # transaction = pd.read_csv(f"mapReduceCountTransaction.csv")
    # transaction.columns=['order_date','total_transaction']
    # transaction.to_sql(f"mapReduceCountTransaction", con=engine, if_exists = 'append', index=False)
