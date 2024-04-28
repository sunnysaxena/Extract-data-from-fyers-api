import numpy as np
import yfinance as yf
from datetime import datetime


def get_index_data(symbol):
    start_date = datetime(2023, 1, 1)
    end_date = datetime.today().strftime("%Y-%m-%d")

    # '^NSEBANK', '^NSEI', 'NIFTY_FIN_SERVICE.NS'
    ticker = yf.Ticker(symbol)

    # pass the parameters as the taken dates for start and end
    df = ticker.history(start=start_date, end=end_date)

    df.reset_index(inplace=True)
    df.drop(['Dividends', 'Stock Splits'], axis=1, inplace=True)

    df.rename(columns={'Date': 'timestamp', 'Open': 'open',
                       'High': 'high', 'Low': 'low',
                       'Close': 'close', 'Volume': 'volume'}, inplace=True)

    df['open'] = np.round(df['open'], decimals=2)
    df['high'] = np.round(df['high'], decimals=2)
    df['low'] = np.round(df['low'], decimals=2)
    df['close'] = np.round(df['close'], decimals=2)

    return df

# df.to_csv('niftybank_1D.csv', index=False)
