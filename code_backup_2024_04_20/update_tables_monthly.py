import time
import datetime
import pandas as pd
import yfinance as yf
from constants import *
from sqlalchemy import text
from datetime import datetime
from datetime import timedelta
from utility import get_today_date
from db_connection import get_mysql_connection


def get_table_name_last_date():
    query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'fnodatabase';"
    engine = get_mysql_connection()
    table_date_time = {}
    old_date_time = {}

    with engine.connect() as connection:
        result = connection.execute(text(query))
        all_tables = [table[0] for table in list(result) if table[0].endswith('_month')]

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
        print('\n')
    return table_date_time


def get_monthly_data():
    for table_name, last_date in get_table_name_last_date().items():
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

            data = yf.Ticker(symbol)
            df = data.history(start=last_date, end=get_today_date(), interval="1mo")  # , rounding=True
            df = df.drop(['Dividends', 'Stock Splits'], axis=1)
            df.reset_index(inplace=True)

            df.columns = ["timestamp", "open", "high", "low", "close", "volume"]
            df['timestamp'] = pd.to_datetime(df['timestamp']).map(lambda x: x.tz_localize(None))
            df['volume'] = 0
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.iloc[1:, :]
            # df.to_sql(name=table_name, con=get_mysql_connection(), index=False, if_exists='append')
            print(f'{table_name} is updated...')
            print(df)
            print('\n')

        except Exception as e:
            print(e)


ask = input("do you want to update monthly data: (y/n) ")

if ask.lower() == 'y':
    get_monthly_data()
