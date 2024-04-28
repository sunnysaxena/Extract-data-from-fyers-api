# 7-Year history data in 1 minute time frame | fyers API V3 history data | algo trading
import os
import time
import math
import pandas as pd
import datetime as dt
from constants import *
from sqlalchemy import text
from datetime import datetime, date
from datetime import timedelta
from my_fyers_model import MyFyersModel
from db_connection import get_mysql_connection

fy_model = MyFyersModel()
today_date = datetime.today().strftime("%Y-%m-%d")

hist_data = pd.DataFrame()
start_date = date(2017, 1, 1)  # (2018, 1, 1), 6
end_date = date(2017, 4, 10)  # (2018, 4, 10), 9
# print(start_date, end_date)
loop = math.ceil((date.today() - start_date).days / 100)
print(loop)


def get_history_data(range_from, range_to, res, table_name):
    try:
        data = {
            "symbol": table_name,
            "resolution": str(res),
            "date_format": "1",
            "range_from": range_from.strftime("%Y-%m-%d"),
            "range_to": range_to.strftime("%Y-%m-%d"),
            "cont_flag": "1",
            "oi_flag": "1"
        }
        global hist_data

        response = fy_model.get_history(data=data)
        df = pd.DataFrame.from_dict(response['candles'])
        df.columns = ["epoc", "open", "high", "low", "close", "volume"]

        df['timestamp'] = pd.to_datetime(df['epoc'], unit='s')
        df['timestamp'] = df['timestamp'].dt.tz_localize('utc').dt.tz_convert(time_zone)
        df['timestamp'] = df['timestamp'].dt.tz_localize(None)
        df = df[["timestamp", "open", "high", "low", "close", "volume"]]
        df.drop_duplicates(inplace=True)
        hist_data = pd.concat([hist_data, df], axis=0)
    except Exception as e:
        print(e)


for _ in range(loop):
    get_history_data(start_date, end_date, '1', 'NSE:TATAPOWER-EQ')
    time.sleep(1)
    start_date = start_date + timedelta(days=100)
    end_date = end_date + timedelta(days=100)

hist_data.to_sql(name='tatapower_1m', con=get_mysql_connection(), index=False, if_exists='append')

#
#
