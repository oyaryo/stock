import csv
import glob
import datetime
import os
import sqlite3
import time
import pandas as pd
import numpy as np


def get_daily_and_making_csv(db_file_name, csv_file_dir):
    conn = sqlite3.connect(db_file_name)
    curs = conn.cursor()
    curs.execute('select code from brands')
    codes = curs.fetchall()
    for code in codes:
        url = 'https://kabutan.jp/stock/kabuka?code={}'.format(code[0])
        df = pd.read_html(url, index_col=0)
        # 通常はdfの[4]を選択（本日よりも過去のデータが欲しいときは[5]へ）
        df[4].to_csv(csv_file_dir + '/' + code[0] + '.D.csv')

def generate_daily_from_csv_file(csv_file_name, code):
    # 取得済みのCSVファイルから取り込むのに必要な行数を引数nrowsに設定
    # 通常はindexに「本日」を設定（df[5]の場合は「日付」へ変更）
    df = pd.read_csv(csv_file_name, index_col="本日", nrows=1)
    df.to_csv(csv_file_name)
    with open(csv_file_name, encoding="UTF-8") as f:
        reader = csv.reader(f)
        next(reader) # 先頭行を飛ばす
        for row in reader:
            row[0] = str('20' + row[0])
            d = (datetime.datetime.strptime(row[0], '%Y/%m/%d').date()) #日付
            o = row[1] if type(row[1]) is str else float(row[1]) # 始値
            h = row[2] if type(row[2]) is str else float(row[2]) # 高値
            l = row[3] if type(row[3]) is str else float(row[3]) # 安値
            c = row[4] if type(row[4]) is str else float(row[4]) # 終値
            v = row[7] if type(row[7]) is str else int(row[7])   # 出来高
            yield code, d, o, h, l, c, v

def generate_from_csv_dir(csv_dir, generate_func):
    for path in glob.glob(os.path.join(csv_dir, "*.D.csv")):
        file_name = os.path.basename(path)
        code = file_name.split('.')[0]
        for d in generate_func(path, code):
            yield d

def all_csv_file_to_db(db_file_name, csv_file_dir):
    price_generator = generate_from_csv_dir(csv_file_dir, generate_daily_from_csv_file)
    conn = sqlite3.connect(db_file_name)
    with conn:
        sql = """
        INSERT INTO prices(code, date, open, high, low, close, volume)
        VALUES(?,?,?,?,?,?,?)
        """
        conn.executemany(sql, price_generator)

if __name__ == '__main__':
    # 直近の時系列データを取り込み、CSVファイルを作成
    # get_daily_and_making_csv('tse.db', 'save_csv')

    # CSVから直近の日足データをDBへ書き込む
    all_csv_file_to_db('tse.db', 'save_csv')
