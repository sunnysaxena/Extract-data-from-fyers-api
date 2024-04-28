import time
import threading
import pandas as pd
import pandas_ta as ta
from datetime import datetime

from my_fyers_model import client_id
from my_fyers_model import MyFyersModel
from my_fyers_model import get_access_token
from fyers_apiv3.FyersWebsocket import data_ws

fy_model = MyFyersModel()
time_frame = 60
live_data = pd.DataFrame(columns=['datetime', 'symbol', 'ltp'])


def on_message(message):
    """
    Callback function to handle incoming messages from the FyersDataSocket WebSocket.

    Parameters:
        message (dict): The received message from the WebSocket.
    """

    print(message)
    # global live_data
    # c = datetime.now()
    # current_time = c.strftime('%Y/%m/%d %H:%M:%S')
    # symb = message['symbol']
    # ltp = message['ltp']
    # # print(f"success :  {message}")
    # new_row = {'datetime': current_time, 'symbol': symb, 'ltp': ltp}
    # live_data.loc[len(live_data)] = new_row
    #
    # open_dict = {
    #     'ltp': 'first'
    # }
    #
    # low_dict = {
    #     'ltp': 'min'
    # }
    #
    # high_dict = {
    #     'ltp': 'max'
    # }
    #
    # close_dict = {
    #     'ltp': 'last'
    # }
    #
    # data = live_data.copy()
    # data['datetime'] = pd.to_datetime(data['datetime'])
    # data = data.set_index('datetime')
    #
    # one_minute = pd.DataFrame(columns=['open', 'high', 'low', 'close'])
    # one_minute['open'] = data.resample('1min').agg(open_dict).dropna()
    # one_minute['high'] = data.resample('1min').agg(high_dict).dropna()
    # one_minute['low'] = data.resample('1min').agg(low_dict).dropna()
    # one_minute['close'] = data.resample('1min').agg(close_dict).dropna()

    # df = one_minute.copy()

    # df['rsi_close_9'] = ta.rsi(df.close, length=9, scalar=100, talib=True, drift=1, offset=0)

    # print(df)


def on_error(message):
    """
    Callback function to handle WebSocket errors.

    Parameters:
        message (dict): The error message received from the WebSocket.
    """
    print("Error:", message)


def on_close(message):
    """
    Callback function to handle WebSocket connection close events.
    """
    print("Connection closed:", message)


def on_open():
    """
    Callback function to subscribe to data type and symbols upon WebSocket connection.
    """
    # Specify the data type and symbols you want to subscribe to
    data_type = "SymbolUpdate"
    # data_type = "DepthUpdate"

    # Subscribe to the specified symbols and data type
    symbols = ['NSE:NIFTYBANK-INDEX']
    fyers.subscribe(symbols=symbols, data_type=data_type)

    # Keep the socket running to receive real-time data
    fyers.keep_running()


# Replace the sample access token with your actual access token obtained from Fyers
access_token = f"{client_id}:{get_access_token()}"

# Create a FyersDataSocket instance with the provided parameters
fyers = data_ws.FyersDataSocket(
    access_token=access_token,  # Access token in the format "appid:accesstoken"
    log_path="logs",  # Path to save logs. Leave empty to auto-create logs in the current directory.
    litemode=False,  # Lite mode disabled. Set to True if you want a lite response.
    write_to_file=False,  # Save response in a log file instead of printing it.
    reconnect=True,  # Enable auto-reconnection to WebSocket on disconnection.
    on_connect=on_open,  # Callback function to subscribe to data upon connection.
    on_close=on_close,  # Callback function to handle WebSocket connection close events.
    on_error=on_error,  # Callback function to handle WebSocket errors.
    on_message=on_message  # Callback function to handle incoming messages from the WebSocket.
)

# Establish a connection to the Fyers WebSocket
fyers.connect()
