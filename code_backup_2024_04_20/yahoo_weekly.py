import time
import datetime
import pandas as pd
import yfinance as yf
from constants import *
from sqlalchemy import text
from datetime import datetime
from datetime import timedelta
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


def get_all_table():
    query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'fnodatabase';"
    engine = get_mysql_connection()

    with engine.connect() as connection:
        result = connection.execute(text(query))
        all_tables = [table[0] for table in list(result)]
        return all_tables


def get_table_name_last_date():
    query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'fnodatabase';"
    engine = get_mysql_connection()
    table_date_time = {}
    old_date_time = {}

    with engine.connect() as connection:
        result = connection.execute(text(query))
        all_tables = [table[0] for table in list(result) if table[0].endswith('_week')]

        for table_name in all_tables:
            query = f"SELECT * FROM {table_name};"
            table_data = connection.execute(text(query))
            last_date = list(table_data)[-1]

            if issubclass(type(last_date[1]), str):
                last_date = last_date[1].split('+')[0]
                datetime_object = datetime.strptime(last_date, '%Y-%m-%d %H:%M:%S')
            else:
                datetime_object = last_date[1]
            old_date_time[table_name] = str(datetime_object.__format__("%Y-%m-%d"))

            # Yesterday date
            last_date = datetime_object + timedelta(days=1)
            table_date_time[table_name] = str(last_date.__format__("%Y-%m-%d"))
        print(table_date_time)
    return table_date_time


def get_weekly_data():
    for table_name, last_date in get_table_name_last_date().items():
        try:
            symbol = None
            if table_name == 'finnifty_week':
                symbol = option_symbols_yahoo['finnifty']
            elif table_name == 'niftybank_week':
                symbol = option_symbols_yahoo['niftybank']
            elif table_name == 'nifty50_week':
                symbol = option_symbols_yahoo['nifty50']
            else:
                print(f'Invalid symbol name : "{table_name}"')

            print(f'Symbol Name : {symbol}')
            print(f"Resolution : {table_name.split('_')[-1]}")

            data = yf.Ticker(symbol)
            df = data.history(start=last_date, end=get_today_date(), interval="1wk")  # , rounding=True
            df = df.drop(['Dividends', 'Stock Splits'], axis=1)
            df.reset_index(inplace=True)

            df.columns = ["timestamp", "open", "high", "low", "close", "volume"]
            df['timestamp'] = pd.to_datetime(df['timestamp']).map(lambda x: x.tz_localize(None))
            df['volume'] = 0
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.iloc[1:, :]
            df.to_sql(name=table_name, con=get_mysql_connection(), index=False, if_exists='append')
            print(f'{table_name} is updated...')
            print(df.head())

        except Exception as e:
            print(e)


ask = input("do you want to update 1 day data: (y/n) ")

if ask.lower() == 'y':
    get_weekly_data()