import time
import pandas as pd
import constants as ct
from my_fyers_model import MyFyersModel

fy_model = MyFyersModel()


def get_file_path(timeframe):
    return f'{ct.option_path}/finnifty_2020_2022_5min{timeframe}.csv'


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

    df = pd.date_range(end='2022-12-30', periods=1095).to_pydatetime().tolist()
    dates = [d.strftime("%Y-%m-%d") for d in df]

    # df = pd.date_range(start="2023-01-01", end='2023-03-31')
    # df = pd.date_range(start="2023-07-01", end='2023-03-30')

    # if timeframe == "5m":
    #     df = pd.date_range(start=get_last_date(timeframe), end=datetime.today().strftime("%Y-%m-%d"))
    # else:
    #     df = pd.date_range(start=get_last_date(timeframe), end=datetime.today().strftime("%Y-%m-%d"))
    # dates = df.tolist()

    # dates = [int(x.value / 10 ** 9) for x in list(dates)]
    master_data = []

    for range_from, range_to in zip(dates, dates[1:]):
        print(range_from, "===", range_to)
        time.sleep(3)
        data = {
            "symbol": symbol,
            "resolution": "5" if timeframe == "5m" else "1D",
            "date_format": "1",
            "range_from": range_from,
            "range_to": range_to,
            "cont_flag": "1"
        }

        response = fy_model.get_history(data=data)
        print(response)
        master_data += response['candles']

    df = pd.DataFrame(master_data, columns=["epoc", "open", "high", "low", "close", "volume"])
    df['timestamp'] = pd.to_datetime(df['epoc'], unit='s', utc=True).map(lambda x: x.tz_localize(None))
    df = df[["timestamp", "open", "high", "low", "close", "volume"]]
    df.drop_duplicates(inplace=True)
    df['volume'] = 0

    print(df.shape)
    return df


def save_latest_data(symbol, timeframe):
    if timeframe == "5m":
        df = download_history(symbol, timeframe)
        df.drop_duplicates(inplace=True)
        df.to_csv(get_file_path(timeframe), index=False)
        print(f'{timeframe} interval file is updated...')
    elif timeframe == "1D":
        df = download_history(symbol, timeframe)
        df.drop_duplicates(inplace=True)
        df.to_csv(get_file_path(timeframe), index=False)
        print(f'{timeframe} interval file is updated...')
    else:
        print('wrong time interval')


if __name__ == '__main__':
    save_latest_data("NSE:FINNIFTY-INDEX", "5m")
    # time.sleep(10)
    # save_latest_data("NSE:FINNIFTY-INDEX", "1D")
