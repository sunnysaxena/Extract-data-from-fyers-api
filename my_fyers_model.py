import os
import pytz
import pandas as pd
import configparser
from constants import *
from datetime import datetime
from fyers_apiv3 import fyersModel
from fyers_apiv3.fyersModel import FyersModel

config = configparser.ConfigParser()
config.read('credentials.ini')

client_id = config['fyers']['client_id']
secret_key = config['fyers']['secret_key']
redirect_url = config['fyers']['redirect_url']
response_type = config['fyers']['response_type']
state = config['fyers']['state']
grant_type = config['fyers']['grant_type']
log_dir = config['fyers']['log_dir']
file_name = config['fyers']['file_name']
time_zone = config['fyers']['time_zone']
verbose = config['fyers']['verbose']


def get_access_token():
    if not os.path.exists(file_name):
        session = fyersModel.SessionModel(
            client_id=client_id,
            secret_key=secret_key,
            redirect_uri=redirect_url,
            response_type=response_type,
            grant_type=grant_type
        )
        response = session.generate_authcode()
        print("Login Url : ", response)

        auth_code = input("Enter Auth Code : ")

        session.set_token(auth_code)
        access_token = session.generate_token()['access_token']

        with open(file_name, 'w') as f:
            f.write(access_token)
    else:
        with open(file_name, 'r') as f:
            access_token = f.read()
    return access_token


class MyFyersModel(object):

    def __init__(self):
        self.token = get_access_token()
        self.fyers_model = FyersModel(client_id=client_id, token=get_access_token(), log_path=log_dir, is_async=False)

    def get_fyre_model(self):
        return self.fyers_model

    def get_token(self):
        return self.token

    # User Details
    def get_profile(self):
        """This allows you to fetch basic details of the client.
        :return: json object
        """
        return self.fyers_model.get_profile()

    def get_fund(self, data=None):
        """Shows the balance available for the user for capital as well as the commodity market.
        :return: json object
        """
        return self.fyers_model.funds()

    def get_holdings(self, data=None):
        """Fetches the equity and mutual fund holdings which the user has in this demate account.
           This will include T1 and demat holdings.
        :return: json object
        """
        return self.fyers_model.holdings()

    # Transaction Info
    def get_order_book(self, data=None):
        """Fetches all the orders placed by the user across all platforms and exchanges in the current trading day.
        """
        return self.fyers_model.orderbook(data)

    def get_positions(self):
        """
        Fetches the current open and closed positions for the current trading day.
        Note that previous trading day’s closed positions will not be shown here.
        :return:
        """
        return self.fyers_model.positions()

    def get_tradebook(self, data=None):
        """Fetches all the trades for the current day across all platforms and exchanges in the current trading day."""
        return self.fyers_model.tradebook()

    # Order Placement
    def get_placeorder(self, data):
        """
        Single Order
           This allows the user to place an order to any exchange via Fyers
        :param data:
            data = {
             "symbol":"NSE:SBIN-EQ",
             "qty":1,
             "type":2,
             "side":1,
             "productType":"INTRADAY",
             "limitPrice":0,
             "stopPrice":0,
             "validity":"DAY",
             "disclosedQty":0,
             "offlineOrder":"False",
        }
        """
        return self.fyers_model.place_order(data=data)

    def get_place_basket_orders(self, data):
        """
        Multi Order
            You can place upto 10 orders simultaneously via the API.
            While Placing Multi orders you need to pass an ARRAY containing the orders request attributes
        :param data:
            data = [{
                "symbol":"NSE:SBIN-EQ",
                "qty":1,
                "type":2,
                "side":1,
                "productType":"INTRADAY",
                "limitPrice":0,
                "stopPrice":0,
                "validity":"DAY",
                "disclosedQty":0,
                "offlineOrder":"False",
            },
                {
                "symbol":"NSE:HDFC-EQ",
                "qty":1,
                "type":2,
                "side":1,
                "productType":"INTRADAY",
                "limitPrice":0,
                "stopPrice":0,
                "validity":"DAY",
                "disclosedQty":0,
                "offlineOrder":"False",
            }]
        """

    # Other Transactions
    def get_modify_order(self, data):
        """
        Modify Order
        This allows the user to modify a pending order. User can provide parameters which needs to be modified.
        In case a particular parameter has not been provided, the original value will be considered.
        :param data:
        orderId = "8102710298291"
        data = {
            "id":orderId,
            "type":1,
            "limitPrice": 61049,
            "qty":1
        }
        :return:
        """
        return self.fyers_model.modify_order(data=data)

    def get_modify_basket_orders(self, data):
        """
        Modify Multi Orders
        You can modify upto 10 orders simultaneously via the API.
        While Modifying Multi orders you need to pass an ARRAY containing the orders request attributes

        :param data:

        orderId = "8102710298291"
        data = [{
              "id": 8102710298291,
              "type": 1,
              "limitPrice": 61049,
              "qty": 1
            },
            {
              "id": 8102710298292,
              "type": 1,
              "limitPrice": 61049,
              "qty": 1
            }]
        :return:
        """
        return self.fyers_model.modify_basket_orders(data)

    # Cancel Order
    def get_cancel_order(self, data):
        """
        Cancel Order
            Cancel pending orders
        :param data
            data = {"id":'808058117761'}
        :return:
        """
        return self.fyers_model.cancel_order(data)

    # Cancel Multi Order
    def get_cancel_basket_orders(self, data):
        """
        Cancel Multi Order
            You can cancel upto 10 orders simultaneously via the API.
            While cancelling Multi orders you need to pass an ARRAY containing the orders request attributes
        :param data:
        data = [{
                "id": '808058117761'
                },
                {
                "id": '808058117762'
                }]
        :return:
        """
        return self.fyers_model.cancel_basket_orders(data=data)

    # Exit Position
    def get_exit_position(self, data):
        """
        Exit Position
            This allows the user to either exit all open positions or any specific open position.
        :param data:
            data = {}
        :return:
        """
        return self.fyers_model.exit_positions(data=data)

    # Exit Position - By Id
    def get_exit_position_by_id(self, data):
        """
        Exit Position - By Id
            This will only exit the open positions for a particular position id
        :param data:
        data = {
            "id":"NSE:SBIN-EQ-BO"
        }
        :return:
        """
        return self.fyers_model.exit_positions(data=data)

    # Pending Order Cancel
    def pending_order_cancel(self):
        pass

    # Convert Position
    def get_convert_position(self, data):
        """
        Convert Position
            This allows the user to convert an open position from one product type to another.
        :param data:
        data = {
            "symbol":"MCX:SILVERMIC20NOVFUT",
            "positionSide":1,
            "convertQty":1,
            "convertFrom":"INTRADAY",
            "convertTo":"CNC"
        }
        :return:
        """
        return self.fyers_model.convert_position(data=data)

    # Broker Config
    def get_market_status(self):
        """
        Market Status
            Fetches the current market status of all the exchanges and their segments
        :return:
        """
        return self.fyers_model.market_status()

    # Data API
    def get_history(self, data):
        """
        History
            The historical API provides archived data (up to date) for the symbols.
            across various exchanges within the given range. A historical record is presented in the form of a candle
            and the data is available in different resolutions
            like - minute, 10 minutes, 60 minutes...240 minutes and daily.
        :return: Candels data
        """
        return self.fyers_model.history(data=data)

    def get_quotes(self, data):
        """
        Quotes
            Quotes API retrieves the full market quotes for one or more symbols provided by the user.
        :param data:
        data = {
                "symbols":"NSE:SBIN-EQ,NSE:HDFC-EQ"
            }
        :return: json
        """
        return self.fyers_model.quotes(data=data)

    def get_market_depth(self, data):
        """
        Market Depth
            The Market Depth API returns the complete market data of the symbol provided.
            It will include the quantity, OHLC values, and Open Interest fields, and bid/ask prices.
        :param data:
            data = {
            "symbol":"NSE:SBIN-EQ",
            "ohlcv_flag":"1"
            }
        :return: json
        """
        return self.fyers_model.depth(data=data)
