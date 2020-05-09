import sqlite3
import time
import os
from pyquery import PyQuery


def CreateDB(db_file_name):
    if os.path.exists(db_file_name):
        print('既にDBが存在します')
    else:
        conn = sqlite3.connect(db_file_name)
        conn.close()

def CreateTB(db_file_name):
    conn = sqlite3.connect(db_file_name)
    curs = conn.cursor()
    curs.execute('CREATE TABLE IF NOT EXISTS brands(code TEXT PRIMARY KEY, name TEXT, short_name TEXT, market TEXT, sector TEXT, unit INTEGER)')
    conn.commit()
    curs.close()
    conn.close()

def get_brand(code):
    url = 'https://kabutan.jp/stock/?code={}'.format(code)

    q = PyQuery(url)

    if len(q.find('div.company_block')) == 0:
        return None

    try:
        name = q.find('div.company_block > h3').text()
        code_short_name = q.find("#stockinfo_i1 > div.si_i1_1 > h2").text()
        short_name = code_short_name[code_short_name.find(" ") + 1:]
        market = q.find('span.market').text()
        unit_str = q.find('#kobetsu_left > table:nth-child(4) > tbody > tr:nth-child(6) > td').text()
        print(unit_str)
        unit = int(unit_str.split()[0].replace(',', ''))
        sector = q.find('#stockinfo_i2 > div > a').text()
    except (ValueError, IndexError):
        return None

    return code, name, short_name, market, unit, sector

def brands_generator(code_range):
    for code in code_range:
        brand = get_brand(code)
        if brand:
            yield brand
        time.sleep(1)

def insert_brands_to_db(db_file_name, code_range):
    conn = sqlite3.connect(db_file_name)
    with conn:
        sql = 'INSERT INTO brands(code, name, short_name, market, unit, sector) VALUES(?,?,?,?,?,?)'
        conn.executemany(sql, brands_generator(code_range))


if __name__ == '__main__':
    db_file_name = 'tse.db'
    CreateDB(db_file_name)
    CreateTB(db_file_name)
    insert_brands_to_db(db_file_name, range(1301, 9997))
