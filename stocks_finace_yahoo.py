import sqlite3
import time
import pandas as pd
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException

db_file_name = 'tse.db'
save_dir = '~/Document/Python/working/stock/Save_csv/'

def download_stock_csv(code_range, save_dir):

    # CSVファイルを自動でsave_dirに保存するための設定
    profile = webdriver.FirefoxProfile()
    profile.set_preference("browser.download.folderList", 2)
    profile.set_preference("browser.download.manager.showWhenStarting", False)
    profile.set_preference("browser.download.dir", save_dir)
    profile.set_preference("browser.helperApps.neverAsk.saveToDisk", "text/csv")
    
    driver = webdriver.Firefox(firefox_profile=profile)
    driver.get('https://www.yahoo.co.jp/')

    # ここで手動でログインを行う。ログインしたらenter
    input('After login, press enter: ')

    for code in code_range:
        code = int(code[0])
        if code > 7239:
            url = 'https://stocks.finance.yahoo.co.jp/stocks/history/?code={0}.T'.format(code)
            driver.get(url)

            try:
                driver.find_element_by_css_selector('a.stocksCsvBtn').click()
            except NoSuchElementException:
                pass

if __name__ == '__main__':
    import os

    conn = sqlite3.connect(db_file_name)
    curs = conn.cursor()
    codes = curs.execute('SELECT code FROM brands')
    download_stock_csv(codes, os.getcwd())
    curs.close()
    conn.close()
