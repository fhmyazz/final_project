#!python3

import pandas as pd

from sqlalchemy import create_engine

if __name__ == "__main__":
    uname_db = 'postgres'
    pw_db = 'admin'
    db_dwh = 'final_project_dwh'
    db_mart = 'final_project_mart'
    ip_db = '172.22.109.180'

    #connecting to db
    try:
        engine_dwh = create_engine(f"postgresql://{uname_db}:{pw_db}@{ip_db}:5432/{db_dwh}")
        engine_mart = create_engine(f"postgresql://{uname_db}:{pw_db}@{ip_db}:5432/{db_mart}")
        print(f"[INFO] Success to Connect to DB")
    except:
        print(f"[INFO] Failed to Connect to DB")

    #extracting data
    data = pd.read_sql_query(f"""
                                select * 
                                from search_log""", con=engine_dwh)

    #transforming data    
    data['date_search'] = data['date_search'].astype('string').str[0:7]
    grouped_data = data.groupby(['date_search'])['date_search'].count().reset_index(name='search_count')
    grouped_data['date_search'] = pd.to_datetime(grouped_data['date_search'])

    #load data
    grouped_data.to_sql(f"count_search", con=engine_mart, if_exists = 'append', index=False)