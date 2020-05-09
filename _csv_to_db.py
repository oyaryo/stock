import csv
import glob
import datetime
import os
# import pandas as pd
# import pandas_datareader as web
import sqlite3

db_file_name = 'tse.db'
csv_file_dir = 'save_csv'

def stock_data_download(ticker):
    if os.path.exists(csv_file_dir + '/' + ticker + '.csv'):
        print('既にCSVファイルが存在しています。')
    else:
        datum = web.DataReader(ticker, 'yahoo', '1970/1/1', )
        datum.drop('Adj Close', axis=1, inplace=True)
        datum['Volume'] = datum['Volume'].astype(int)
        datum.to_csv(csv_file_dir + '/' + ticker + '.csv', encoding='UTF-8')
        
def create_db(db_file_name):
    if os.path.exists(db_file_name):
        print('既にDBが存在します。')
    else:
        conn = sqlite3.connect(db_file_name)
        conn.close()

def create_table(db_file_name):
    conn = sqlite3.connect(db_file_name)
    curs = conn.cursor()
    curs.execute('CREATE TABLE IF NOT EXISTS prices(code TEXT, date TEXT, open REAL, high REAL, low REAL, close REAL, volume INTEGER,  AdjClose REAL,  PRIMARY KEY(code, date))')
    conn.commit()
    curs.close()
    conn.close()

def generate_price_from_csv_file(csv_file_name, code):
    with open(csv_file_name) as f:
        reader = csv.reader(f)
        next(reader) # 先頭行を飛ばす
        for row in reader:
            d = datetime.datetime.strptime(row[0], '%Y/%m/%d').date() # 日
            o = float(row[1]) # 始値
            h = float(row[2]) # 高値
            l = float(row[3]) # 安値
            c = float(row[4]) # 終値
            v = int(row[5])   # 出来高
            a = float(row[6]) # 調整後終値
            yield code, d, o, h, l, c, v, a

def generate_from_csv_dir(csv_dir, generate_func):
    for path in glob.glob(os.path.join(csv_dir, '*.T.csv')):
        file_name = os.path.basename(path)
        code = file_name.split('.')[0]
        for d in generate_func(path, code):
            yield d

def all_csv_file_to_db(db_file_name, csv_file_dir):
    price_generator = generate_from_csv_dir(csv_file_dir, generate_price_from_csv_file)
    conn = sqlite3.connect(db_file_name)
    with conn:
        sql = """
        INSERT INTO prices(code, date, open, high, low, close, volume, AdjClose)
        VALUES(?,?,?,?,?,?,?,?)
        """
        conn.executemany(sql, price_generator)

if __name__ == ('__main__'):
    create_table(db_file_name)
    all_csv_file_to_db(db_file_name, csv_file_dir)
