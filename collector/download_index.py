import os
import json
import sys
import pytz
import pandas as pd
from configure import fyers
from constants import *
from datetime import datetime

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)


class Index(object):
    def __init__(self, index_name):
        self.index_name = index_name

    def get_last_date(self):

        # name = "Eric"
        # age = 74
        # f"Hello, {name}. You are {age}."
        try:
            last_date = pd.read_csv(f'data/{self.index_name}.csv')['timestamp'].tolist()[-1].split(' ')[0]
            print(last_date)
            return last_date
        except Exception as e:
            print(e)

    def download_latest(self):
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

        # df = pd.date_range(start="2023-07-01", end='2023-06-30')
        # df = pd.date_range(start="2023-01-01", end=datetime.today().strftime("%Y-%m-%d"))
        df = pd.date_range(start=self.get_last_date(), end=datetime.today().strftime("%Y-%m-%d"))
        dates = df.tolist()
        master_data = []
        global symbol

        # dates = [int(x.value / 10 ** 9) for x in list(dates)]

        if self.index_name == 'niftybank':
            symbol = 'NSE:NIFTYBANK-INDEX'
        elif self.index_name == 'nifty50':
            symbol = 'NSE:NIFTY50-INDEX'
        elif self.index_name == 'finnifty':
            symbol = 'NSE:FINNIFTY-INDEX'
        else:
            symbol = 'wrong index'
            sys.exit(1)

        for range_from, range_to in zip(dates, dates[1:]):
            data = {
                "symbol": symbol,
                "resolution": "5",
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

    def save_csv(self):
        df1 = pd.read_csv(f'data/{self.index_name}.csv')
        df2 = self.download_latest()
        df = pd.concat([df1, df2], axis=0)

        df.to_csv(f'data/{self.index_name}.csv', index=False)
        print('done...')


if __name__ == '__main__':
    index = Index('ggi')
    index.save_csv()

    # download_history("NSE:NIFTY50-INDEX")
    # download_history("NSE:NIFTY2381719600CE")
