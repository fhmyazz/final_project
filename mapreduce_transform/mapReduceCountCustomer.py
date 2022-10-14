#!python3

from mrjob.job import MRJob
from mrjob.step import MRStep

import pandas as pd
import csv

cols = 'id_customer,name_customer,birthdate_customer,gender_customer,country_customer'.split(',')

def csv_readline(line):
    """Given a sting CSV line, return a list of strings."""
    for row in csv.reader([line]):
        return row

class countCustomer(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
            MRStep(reducer=self.sort)
        ]

    def mapper(self, _, line):
        # Convert each line into a dictionary
        row = dict(zip(cols, csv_readline(line)))

        #skip first row as header
        if row['id_customer'] != 'id_customer':
            # Yield the order_date
            yield row['gender_customer'], 1

    def reducer(self, key, values):
        #for 'order_date' compute
        yield None, (key,sum(values))
    
    def sort(self, key, values):
        data = []
        for gender_customer, total_customer in values:
            data.append((gender_customer, total_customer))
            data.sort()

        for gender_customer, total_customer in data:
            yield gender_customer, total_customer

if __name__ == '__main__':
    countCustomer.run()