import time
import datetime
import pandas as pd
import yfinance as yf
from constants import *
from utility import get_today_date
from my_fyers_model import MyFyersModel
from db_connection import get_mysql_connection

fy_model = MyFyersModel()

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)


def get_weekly_dates():
    df = pd.date_range(start='2005-10-17', end='2023-10-29', freq='D').to_series()
    dates = [d.strftime("%Y-%m-%d") for d in df]
    day_name = df.dt.day_name().tolist()
    month_name = df.dt.month_name().tolist()

    xx = pd.DataFrame({'dates': dates, 'day_name': day_name, 'month_name': month_name})
    final_dates = xx.loc[xx['day_name'] == 'Monday']
    return final_dates['dates'].tolist()


def get_weekly_data(table_name):
    try:
        symbol = None

        if table_name == 'finnifty_month':
            symbol = option_symbols_yahoo['finnifty']
        elif table_name == 'niftybank_month':
            symbol = option_symbols_yahoo['niftybank']
        elif table_name == 'nifty50_month':
            symbol = option_symbols_yahoo['nifty50']
        else:
            print(f'Invalid symbol name : "{table_name}"')

        print(f'Symbol Name : {symbol}')
        print(f"Resolution : {table_name.split('_')[-1]}")

        data = yf.Ticker(symbol)
        # df = data.history(period="max", interval="1mo")  # rounding=True
        df = data.history(start='2023-05-01', end=get_today_date(), interval="1mo")
        df = df.drop(['Dividends', 'Stock Splits'], axis=1)
        df.reset_index(inplace=True)

        df.columns = ["timestamp", "open", "high", "low", "close", "volume"]
        df['timestamp'] = pd.to_datetime(df['timestamp']).map(lambda x: x.tz_localize(None))
        df['volume'] = 0
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        # df.to_sql(name=table_name, con=get_mysql_connection(), index=False, if_exists='append')
        print(df)
        print(f'{table_name} is updated...')

    except Exception as e:
        print(e)


ask = input("do you want to update 1 day data: (y/n) ")

if ask.lower() == 'y':
    table_name = 'finnifty_month'
    get_weekly_data(table_name)
    pass
