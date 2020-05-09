from pyquery import PyQuery
import datetime
import sqlite3


def create_table(db_file_name):
    conn = sqlite3.connect(db_file_name)
    curs = conn.cursor()
    curs.execute('CREATE TABLE if not exists new_brands (code TEXT, date TEXT, PRIMARY KEY(code, date))')
    curs.execute('CREATE TABLE if not exists delete_brands (code TEXT, date TEXT, PRIMARY KEY(code, date))')

def new_brands_generator():
    url = 'http://www.jpx.co.jp/listing/stocks/new/index.html'
    q = PyQuery(url)
    for d, i in zip(q.find('tbody > tr:even > td:eq(0)'), q.find('tbody > tr:even span')):
            date = datetime.datetime.strptime(d.text, '%Y/%m/%d').date()
            yield (i.get('id'), date)

def insert_new_brands_to_db(db_file_name):
    conn = sqlite3.connect(db_file_name)
    with conn:
        sql = 'INSERT INTO new_brands(code, date) VALUES(?, ?)'
        conn.executemany(sql, new_brands_generator())

def delete_brands_generator():
    url = 'https://www.jpx.co.jp/listing/stocks/delisted/index.html'
    q = PyQuery(url)
    for d, i in zip(q.find('tbody > tr > td:eq(0)'), q.find('tbody > tr > td:eq(2)')):
            date = datetime.datetime.strptime(d.text, '%Y/%m/%d').date()
            code = i.text
            yield (code, date)

def insert_delete_brands_to_db(db_file_name):
    conn = sqlite3.connect(db_file_name)
    with conn:
        sql = 'INSERT INTO delete_brands(code, date) VALUES(?, ?)'
        conn.executemany(sql, delete_brands_generator())

