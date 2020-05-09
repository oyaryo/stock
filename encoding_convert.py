import pandas as pd
from pathlib import Path

code_range = []
save_dir = 'save_csv'

def get_file_name():
    p = Path(save_csv)
    file_list = list(p.glob("*"))
    for code in file_list:
        code = code.name.split('.')[0]
        try:
            read_csv(code)
        except:
            pass

def read_csv(code):
    datum = pd.read_csv(save_dir + '/' + str(code) + '.T.csv', encoding='cp932')
    datum.columns = ['Date','Open', 'High', 'Low', 'Close', 'Volum', 'Adj Close']
    datum.set_index(['Date'], inplace=True)
    datum.to_csv(save_dir + '/' + str(code) + '.T.csv')

if __name__ == '__main__':
    get_file_name()
