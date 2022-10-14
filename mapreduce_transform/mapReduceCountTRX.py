#!python3

from mrjob.job import MRJob
from mrjob.step import MRStep

from sqlalchemy import create_engine
import pandas as pd
import csv

cols = 'id_transaction,id_customer,date_transaction,product_transaction,amount_transaction'.split(',')

def csv_readline(line):
    """Given a sting CSV line, return a list of strings."""
    for row in csv.reader([line]):
        return row

#connect to sql
engine = create_engine(f"postgresql://postgres:admin@172.31.16.43:5432/final_project_mart")

class countTRX(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
            MRStep(reducer=self.sort)
        ]

    def mapper(self, _, line):
        # Convert each line into a dictionary
        row = dict(zip(cols, csv_readline(line)))

        #skip first row as header
        if row['id_transaction'] != 'id_transaction':
            # Yield the order_date
            yield row['date_transaction'][0:7], 1

    def reducer(self, key, values):
        #for 'order_date' compute
        yield None, (key,sum(values))
    
    def sort(self, key, values):
        data = []
        for order_date, order_count in values:
            data.append((order_date, order_count))
            data.sort()

        for order_date, order_count in data:
            yield order_date, order_count

if __name__ == '__main__':
    countTRX.run()
    # pd.read_csv('mapReduceCountTRX.csv').to_sql(f"mapreduce_count_trx", con=engine, if_exists = 'append', index=False)