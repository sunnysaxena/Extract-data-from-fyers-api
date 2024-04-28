import os
import json
import time
import pytz
import pandas as pd
from configure import fyers
from constants import *
from datetime import datetime

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)


def get_last_date(timeframe):
    if timeframe == "5m":
        last_date = pd.read_csv(f'data/nifty50_{timeframe}.csv')['timestamp'].tolist()[-1].split(' ')[0]
        print(last_date)
    else:
        last_date = pd.read_csv(f'data/nifty50_{timeframe}.csv')['timestamp'].tolist()[-1].split(' ')[0]
        print(last_date)
    return last_date


def download_history(symbol, timeframe=5):
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

    # pd.date_range(end=datetime.today(), periods=100).to_pydatetime().tolist()
    # OR

    # df = pd.date_range(start="2023-01-01", end='2023-03-31')
    # df = pd.date_range(start="2023-07-01", end='2023-03-30')

    if timeframe == "5m":
        df = pd.date_range(start=get_last_date(timeframe), end=datetime.today().strftime("%Y-%m-%d"))
    else:
        df = pd.date_range(start=get_last_date(timeframe), end=datetime.today().strftime("%Y-%m-%d"))
        print('One Day...')
    dates = df.tolist()

    # dates = [int(x.value / 10 ** 9) for x in list(dates)]
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
        print(response)
        master_data += response['candles']

    df = pd.DataFrame(master_data, columns=["epoc", "open", "high", "low", "close", "volume"])

    df['timestamp'] = pd.to_datetime(df['epoc'], unit='s', utc=True).map(lambda x: x.tz_convert(time_zone))
    df = df[["timestamp", "open", "high", "low", "close", "volume"]]

    print(df.shape)
    return df


def save_latest_data(symbol, timeframe):
    if timeframe == "5m":
        df1 = pd.read_csv(f'data/nifty50_{timeframe}.csv')
        df2 = download_history(symbol, timeframe)
        df = pd.concat([df1, df2], axis=0)
        df.drop_duplicates(inplace=True)
        df.to_csv(f'data/nifty50_{timeframe}.csv', index=False)
        print(f'{timeframe} interval file is updated...')
    elif timeframe == "1D":
        df1 = pd.read_csv(f'data/nifty50_{timeframe}.csv')
        df2 = download_history(symbol, timeframe)
        df = pd.concat([df1, df2], axis=0)
        df.drop_duplicates(inplace=True)
        df.to_csv(f'data/nifty50_{timeframe}.csv', index=False)
        print(f'{timeframe} interval file is updated...')
    else:
        print('wrong time interval')


if __name__ == '__main__':
    save_latest_data("NSE:NIFTY50-INDEX", "5m")
    time.sleep(5)
    save_latest_data("NSE:NIFTY50-INDEX", "1D")
