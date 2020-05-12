# -*- coding: utf-8 -*-
import datetime
import sqlite3

def create_price_table():
    conn = sqlite3.connect(db_file_name)
    with conn:
        sql = """
        CREATE TABLE IF NOT EXISTS price (code TEXT, date TEXT, open REAL, high REAL, low REAL, close REAL, volume INTEGER, PRIMARY KEY(code, date))
        """
        conn.execute(sql)

def create_applied_divide_union_data_table():
    conn = sqlite3.connect(db_file_name)
    with conn:
        sql = """
        CREATE TABLE IF NOT EXISTS applied_divide_union_data (code TEXT, date_of_right_allotment TEXT, PRIMARY KEY(code, date_of_right_allotment))
        """
        conn.execute(sql)

def apply_divide_union_data(db_file_name, date_of_right_allotment):
    conn = sqlite3.connect(db_file_name)

    # date_of_right_allotment 以前の分割・併合データで未適用のものを取得する
    sql = """
    SELECT 
        d.code, d.date_of_right_allotment, d.before, d.after
    FROM 
        divide_union_data AS d
    WHERE
        d.date_of_right_allotment < ?
        AND NOT EXISTS (
            SELECT 
                * 
            FROM 
                applied_divide_union_data AS a
            WHERE 
                d.code = a.code
                AND d.date_of_right_allotment = a.date_of_right_allotment
            ) 
    ORDER BY 
        d.date_of_right_allotment
    """
    cur = conn.execute(sql, (date_of_right_allotment,))
    divide_union_data = cur.fetchall()
    
    with conn:
        conn.execute('BEGIN TRANSACTION')
        for code, date_of_right_allotment, before, after in divide_union_data:
            
            rate = before / after
            inv_rate = 1 / rate
            
            conn.execute(
              'UPDATE prices SET '
              ' open = open * :rate, '
              ' high = high * :rate, '
              ' low = low  * :rate, '
              ' close = close * :rate, '
              ' volume = volume * :inv_rate '
              'WHERE code = :code '
              '  AND date <= :date_of_right_allotment',
              {'code' : code,
               'date_of_right_allotment' : date_of_right_allotment,
               'rate' : rate,
               'inv_rate' : inv_rate})
                          
            conn.execute(
              'INSERT INTO '
              'applied_divide_union_data(code, date_of_right_allotment) '
              'VALUES(?,?)',
              (code, date_of_right_allotment))

if __name__ == "__main__":
    conn = sqlite3.connect('tse.db')
    curs = conn.cursor()
    curs.execute('SELECT date_of_right_allotment FROM divide_union_data')
    allotments = curs.fetchall()
    curs.close()
    conn.close()

    for allotment in allotments:
        print(allotment[0])
        apply_divide_union_data('tse.db', allotment[0])
