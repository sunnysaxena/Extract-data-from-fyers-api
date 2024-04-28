import os
import time
import pandas as pd
import yfinance as yf
from constants import *
from sqlalchemy import text
from datetime import datetime
from datetime import timedelta
from my_fyers_model import MyFyersModel
from db_connection import get_mysql_connection

fy_model = MyFyersModel()
today_date = datetime.today().strftime("%Y-%m-%d")


def get_table_name_last_date():
    query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'fnodatabase';"
    engine = get_mysql_connection()
    table_date_time = {}
    old_date_time = {}

    with engine.connect() as connection:
        result = connection.execute(text(query))
        all_tables = [table[0] for table in list(result) if table[0].endswith('_1d') or table[0].endswith('_1D')]
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


def tata_last_record():
    return {'tatapower_1d': '2013-4-2'}


init = 40
count = 1
master_data = []


def store_new_stock():
    global init, count, master_data
    try:
        for table_name, last_date in tata_last_record().items():
            print(last_date)

            if last_date != today_date:
                dates = pd.date_range(start=last_date, end=today_date).tolist()
                # master_data = []
                symbol = None

                for range_from, range_to in zip(dates, dates[1:]):
                    print(range_from.strftime("%Y-%m-%d"), range_to.strftime("%Y-%m-%d"))
                    if table_name == 'tatapower_1d':
                        symbol = stocks_option_symbols['tata_power']
                    else:
                        print(f'Invalid symbol name : "{table_name}"')

                    data = {
                        "symbol": symbol,
                        "resolution": "1D",
                        "date_format": "1",
                        "range_from": range_from.strftime("%Y-%m-%d"),
                        "range_to": range_to.strftime("%Y-%m-%d"),
                        "cont_flag": "1"
                    }

                    response = fy_model.get_history(data=data)
                    master_data += response['candles']

                    if len(master_data) != 0:
                        print(master_data[-1])

                    if count == init:
                        time.sleep(10)
                        init += 40
                    count += 1
                    print(count, init)

                df = pd.DataFrame(master_data, columns=["epoc", "open", "high", "low", "close", "volume"])
                df['timestamp'] = pd.to_datetime(df['epoc'], unit='s', utc=True).map(lambda x: x.tz_localize(None))
                df = df[["timestamp", "open", "high", "low", "close", "volume"]]
                df.drop_duplicates(inplace=True)
                df['volume'] = 0

                df.to_sql(name=table_name, con=get_mysql_connection(), index=False, if_exists='append')
                print(df.head())
                print(df.tail())
                time.sleep(2)
                print(f'{table_name} is updated...')
                print('\n')
            else:
                print(f'{table_name} : table is already up to date ...')
    except KeyboardInterrupt as ki:
        df = pd.DataFrame(master_data, columns=["epoc", "open", "high", "low", "close", "volume"])
        df['timestamp'] = pd.to_datetime(df['epoc'], unit='s', utc=True).map(lambda x: x.tz_localize(None))
        df = df[["timestamp", "open", "high", "low", "close", "volume"]]
        df.drop_duplicates(inplace=True)
        df['volume'] = 0
        df.to_csv(str(datetime.now().time()) + '.csv', index=False)
        # df.to_sql(name='ongc_1d', con=get_mysql_connection(), index=False, if_exists='append')
        print(df.head())
        print(df.tail())
        time.sleep(2)
        # print(f'{table_name} is updated...')
        print(ki, 'Error occurred...')
    except Exception as e:
        df = pd.DataFrame(master_data, columns=["epoc", "open", "high", "low", "close", "volume"])
        df['timestamp'] = pd.to_datetime(df['epoc'], unit='s', utc=True).map(lambda x: x.tz_localize(None))
        df = df[["timestamp", "open", "high", "low", "close", "volume"]]
        df.drop_duplicates(inplace=True)
        df['volume'] = 0

        df.to_csv(str(datetime.now().time()) + '.csv', index=False)
        df.to_sql(name='tatapower_1d', con=get_mysql_connection(), index=False, if_exists='append')
        print(df.head())
        print(df.tail())
        time.sleep(2)
        print('tatapower_1d is updated...')
        print(e, ' : Error occurred...')


ask = input("do you want to update 1 day data (Y/n) : ")

if ask.lower() == 'y':
    store_new_stock()
