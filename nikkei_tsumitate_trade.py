import datetime
import simulator as sim
from golden_core30 import create_stock_data


def simulate_nikkei_tsumitate(db_file_name, start_date, end_date, deposit, reserve):
    code = input('銘柄コードを入植してください：')
    stocks = create_stock_data(db_file_name, (code,), start_date, end_date)
    def get_open_price_func(date, code):
        return stocks[code]['prices']['open'][date]

    def get_close_price_func(date, code):
        return stocks[code]['prices']['close'][date]

    current_month = start_date.month - 1
    def trade_func(date, portfolio):
        nonlocal current_month
        if date.month != current_month:
            # 月初め => 入金 => 購入
            portfolio.add_deposit(reserve)
            current_month = date.month
            return [sim.BuyMarketOrderAsPossible(code, stocks[code]['unit'])]
        return []

    return sim.simulate(start_date, end_date, deposit, trade_func, get_open_price_func, get_close_price_func)

if __name__ == "__main__":
    portfolit, result = simulate_nikkei_tsumitate('tse.db', datetime.date(2008, 4, 1), datetime.date(2019, 4, 1), 1000000, 50000)

