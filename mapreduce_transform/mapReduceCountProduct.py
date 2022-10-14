#!python3

from mrjob.job import MRJob
from mrjob.step import MRStep

import pandas as pd
import csv

cols = 'id_transaction,id_customer,date_transaction,product_transaction,amount_transaction'.split(',')

def csv_readline(line):
    """Given a sting CSV line, return a list of strings."""
    for row in csv.reader([line]):
        return row

class countProducts(MRJob):
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
            yield row['product_transaction'], 1

    def reducer(self, key, values):
        #for 'order_date' compute
        yield None, (key,sum(values))
    
    def sort(self, key, values):
        data = []
        for product_name, total_product in values:
            data.append((product_name, total_product))
            data.sort()

        for product_name, total_product in data:
            yield product_name, total_product

if __name__ == '__main__':
    countProducts.run()