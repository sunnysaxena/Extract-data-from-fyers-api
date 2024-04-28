import datetime
import time
import threading
from my_fyers_model import client_id
from fyers_apiv3.FyersWebsocket import data_ws
from my_fyers_model import get_access_token

live_data = {}
time_frame = 60
data_type = "symbolData"
ws_access_token = f"{client_id}:{get_access_token()}"
symbol = ["NSE:INDIAVIX-INDEX"]
# symbol = ["NSE:FINNIFTY2390519900CE", "NSE:FINNIFTY2390519900PE"]
fs = data_ws.FyersDataSocket(access_token=ws_access_token, log_path="logs")


def start_websocket():
    def custom_message(msg):
        for symbol_data in msg:
            print(symbol_data)
            # print(f" ltp : {symbol_data['ltp']}, vol_traded_today :{symbol_data['vol_traded_today']}")
            # print(symbol_data['vol_traded_today'])
            # live_data[symbol_data]['symbol'] = {'LTP': symbol_data['ltp']}

    fs.websocket_data = custom_message

    def subscribe_new_symbol(symbol_list):
        fs.subscribe(symbol=symbol_list, data_type=data_type)

    threading.Thread(target=subscribe_new_symbol, args=(symbol,)).start()


start_websocket()

# Technical Analysis Loop
while True:
    print(live_data)
    interval = time_frame - datetime.datetime.now().second
    print(f"Start In : {interval}")
    time.sleep(interval)
