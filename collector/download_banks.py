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


def get_last_date():
    last_date = pd.read_csv('../data/options/finnifty_5m.csv')['timestamp'].tolist()[-1].split(' ')[0]
    return last_date


def download_history(bank_name):
    df1 = pd.date_range(start="2023-01-01", end='2023-03-31')
    df2 = pd.date_range(start="2023-04-01", end='2023-06-30')
    df3 = pd.date_range(start="2023-07-01", end=datetime.today().strftime("%Y-%m-%d"))
    # df = pd.date_range(start=get_last_date(), end=datetime.today().strftime("%Y-%m-%d"))

    date_list = [df1, df2, df3]

    master_data = []
    for index, df in enumerate(date_list):
        dates = df.tolist()
        for range_from, range_to in zip(dates, dates[1:]):
            data = {
                "symbol": f'NSE:{bank_name}-EQ',
                "resolution": "5",
                "date_format": "1",
                "range_from": range_from.strftime("%Y-%m-%d"),  # yyyy-mm-dd
                "range_to": range_to.strftime("%Y-%m-%d"),
                "cont_flag": "1"
            }
            response = fyers.history(data=data)
            master_data += response['candles']

        print(f'pause     :  {datetime.now()}')
        time.sleep(3 * 60)
        print(f'continue  :  {datetime.now()}')
        print('\n')

    df = pd.DataFrame(master_data, columns=["epoc", "open", "high", "low", "close", "volume"])

    df['timestamp'] = pd.to_datetime(df['epoc'], unit='s', utc=True).map(lambda x: x.tz_convert(time_zone))
    df = df[["timestamp", "open", "high", "low", "close", "volume"]]
    print(df.shape)
    df.to_csv(f'data/{bank_name}.csv', index=False)


# download_history("KOTAKBANK")
# download_history("HDFCBANK")
# download_history("ICICIBANK")
# download_history("AXISBANK")
# download_history("INDUSINDBK")
