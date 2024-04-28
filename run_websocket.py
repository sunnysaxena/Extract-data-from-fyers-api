import time
import threading
from my_fyers_model import client_id
from my_fyers_model import MyFyersModel
from my_fyers_model import get_access_token
from fyers_apiv3.FyersWebsocket import data_ws

fy_model = MyFyersModel()

# Replace the sample access token with your actual access token obtained from Fyers
access_token = f"{client_id}:{get_access_token()}"
live_data = {}
# Specify the data type and symbols you want to subscribe to
# data_type = "DepthUpdate"
data_type = "symbolData"


def on_message(message):
    """
    Callback function to handle incoming messages from the FyersDataSocket WebSocket.

    Parameters:
        message (dict): The received message from the WebSocket.
    """
    print(message)


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


def on_open(fyers):
    """
    Callback function to subscribe to data type and symbols upon WebSocket connection.

    """

    # Subscribe to the specified symbols and data type
    symbols = ["NSE:ONGC-EQ"]
    fyers.subscribe(symbols=symbols, data_type=data_type)

    # Keep the socket running to receive real-time data
    fyers.keep_running()


def run_websocket():
    # Create a Fyers DataSocket instance with the provided parameters
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


if __name__ == "__main__":
    while True:
        run_websocket()
        time.sleep(60)  # 5 minutes
