import os
import time

import pandas as pd
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
        all_tables = [table[0] for table in list(result) if table[0].endswith('_15m')]

        for table_name in all_tables:
            query = f"SELECT * FROM {table_name};"
            table_data = connection.execute(text(query))
            # print(list(table_data))
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
    return table_date_time


def update_all_tables():
    for table_name, last_date in get_table_name_last_date().items():

        if last_date != today_date:
            dates = pd.date_range(start=last_date, end=today_date).tolist()
            master_data = []
            symbol = None

            for range_from, range_to in zip(dates, dates[1:]):
                if table_name == 'finnifty_15m':
                    symbol = option_symbols['finnifty']
                elif table_name == 'indiavix_15m':
                    symbol = option_symbols['indiavix']
                elif table_name == 'nifty50_15m':
                    symbol = option_symbols['nifty50']
                elif table_name == 'niftybank_15m':
                    symbol = option_symbols['niftybank']
                else:
                    print(f'Invalid symbol name : "{table_name}"')

                data = {
                    "symbol": symbol,
                    "resolution": "15",
                    "date_format": "1",
                    "range_from": range_from.strftime("%Y-%m-%d"),
                    "range_to": range_to.strftime("%Y-%m-%d"),
                    "cont_flag": "1"
                }

                response = fy_model.get_history(data=data)
                master_data += response['candles']

            df = pd.DataFrame(master_data, columns=["epoc", "open", "high", "low", "close", "volume"])
            df['timestamp'] = pd.to_datetime(df['epoc'], unit='s', utc=True).map(lambda x: x.tz_convert(time_zone))
            df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y-%m-%d %H:%M:%S')
            df['timestamp'] = df['timestamp'].dt.tz_localize(None)
            df = df[["timestamp", "open", "high", "low", "close", "volume"]]
            df.drop_duplicates(inplace=True)
            df['volume'] = 0
            df.to_sql(name=table_name, con=get_mysql_connection(), index=False, if_exists='append')
            print(df.tail())
            time.sleep(5)
            print(f'\n{table_name} is updated...')
        else:
            print(f'{table_name} : table is already up to date ...')


ask = input("do you want to update 15 min data (Y/n) : ")

if ask.lower() == 'y':
    update_all_tables()
