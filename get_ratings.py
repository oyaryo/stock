import csv
import glob
import datetime
import os
import sqlite3
from dateutil.relativedelta import relativedelta


def generate_rating_from_csv_file(csv_file_name):
    with open(csv_file_name, encoding='UTF-8') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            d = (datetime.datetime.strptime(row[0], '%m/%d').date() + relativedelta(years=120)) #日付
            code = row[1]
            tt = row[4]
            r = row[5]
            tg = row[6].split('→')[1].split('円')[0] if '→' in row[6] else row[6].split('円')[0]
            print(d, code, tt, r, tg)
            yield d, code, tt, r, tg

def csv_file_to_db(db_file_name, csv_file_name):
    rating_generator = generate_rating_from_csv_file(csv_file_name)

    conn = sqlite3.connect(db_file_name)
    with conn:
        sql = """
        INSERT OR REPLACE INTO raw_ratings(date, code, think_tank, rating, target) VALUES(?,?,?,?,?)
        """
        conn.executemany(sql, rating_generator)

if __name__ == '__main__':
    csv_file_to_db('tse.db', 'ratings.csv')
