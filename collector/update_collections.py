import os
import json
import time
import pandas as pd
from constants import *
from configure import fyers
from datetime import datetime
from constants import option_symbols
from configparser import ConfigParser
from db_connection import db_connection

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)

client = db_connection()
config = ConfigParser()
config.read('credentials.ini')
database = config['mongo']['database']


def get_last_date(timeframe):
    if timeframe == "5m":
        last_date = pd.read_csv(f'data/nifty50_{timeframe}.csv')['timestamp'].tolist()[-1].split(' ')[0]
        print(last_date)
    else:
        last_date = pd.read_csv(f'data/nifty50_{timeframe}.csv')['timestamp'].tolist()[-1].split(' ')[0]
        print(last_date)
    return last_date


def download_history(symbol, timeframe, last_date):
    """
    Candles data containing array of following data for particular time stamp:
    :return:
        1.Current epoch time
        2.Open Value
        3.Highest Value
        4.Lowest Value
        5.Close Value
        6.Total traded quantity (volume)
    """

    dates = pd.date_range(start=last_date, end=datetime.today().strftime("%Y-%m-%d")).tolist()

    master_data = []

    for range_from, range_to in zip(dates, dates[1:]):
        data = {
            "symbol": symbol,
            "resolution": "5" if timeframe == "5m" else "1D",
            "date_format": "1",
            "range_from": range_from.strftime("%Y-%m-%d"),  # yyyy-mm-dd
            "range_to": range_to.strftime("%Y-%m-%d"),
            "cont_flag": "1"
        }

        response = fyers.history(data=data)
        master_data += response['candles']

    df = pd.DataFrame(master_data, columns=["epoc", "open", "high", "low", "close", "volume"])
    df['timestamp'] = pd.to_datetime(df['epoc'], unit='s', utc=True).map(lambda x: x.tz_convert(time_zone))
    df = df[["timestamp", "open", "high", "low", "close", "volume"]]

    print(df.shape)
    return df


# def save_latest_data(symbol, timeframe):
#     if timeframe == "5m":
#         df = download_history(symbol, timeframe)
#         df.drop_duplicates(inplace=True)
#     elif timeframe == "1D":
#         df = download_history(symbol, timeframe)
#         df.drop_duplicates(inplace=True)
#     else:
#         print('wrong time interval')

def update_collections():
    try:
        db = client[database]

        for collection in db.list_collection_names()[0]:

            collection_name = db[collection]
            list_cur = list(collection_name.find())
            # last_date = pd.DataFrame(list_cur)['timestamp'].tail(1).item().split(' ')[0]
            # dff = pd.DataFrame(list_cur)

            dfff = download_history('NSE:NIFTY50-INDEX', '5m', '2023-08-30')
            print(dfff)

            exit()
            last_date = 'dcd'

            if last_date != datetime.today().strftime("%Y-%m-%d"):
                symbol, timeframe = option_symbols[collection]
                df = download_history(symbol, timeframe, last_date)

                print(df.head())

                exit()

                df.drop_duplicates(inplace=True)
                df.reset_index(inplace=True)
                document = df.to_dict('records')
                collection_name.insert_many(document)
                time.sleep(10)

                print(f"{collection}, {timeframe}, {symbol}, {last_date}")
    except Exception as e:
        print("[+] Database connection error!")
        raise e

update_collections()
