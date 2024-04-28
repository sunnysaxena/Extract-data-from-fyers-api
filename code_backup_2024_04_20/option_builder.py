import json
import requests
import configparser
import pandas as pd
from datetime import date
from my_fyers_model import MyFyersModel

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)

config = configparser.ConfigParser()
config.read('credentials.ini')
fno_url = config['symbol_master']['nse_fo']

model = MyFyersModel()

allowed_segment = [14]
fno_symbol_list = pd.read_csv(fno_url, header=None)

fno_symbol_list.columns = ['FyToken', 'SymbolDetails', 'Instrument', 'MinLotSize', 'Tick', 'ISIN',
                           'TradingSession', 'LastUpdateDate', 'ExpiryDate', 'SymbolTicker', 'Exchange',
                           'Segment', 'Script', 'ScriptCode', 'UnderlyingScriptCode', 'StrikePrice',
                           'OptionType', 'UnderlyingFyToken', 'C_18']

fno_symbol_list = fno_symbol_list[fno_symbol_list['Instrument'].isin(allowed_segment)]
fno_symbol_list['ExpiryDate'] = pd.to_datetime(fno_symbol_list['ExpiryDate'], unit='s').apply(lambda x: x.date())

print(f"Token of the Exchange :  {set(fno_symbol_list['ScriptCode'].tolist())}")
# print('\n')

# fno_symbol_list = fno_symbol_list[(fno_symbol_list['ExpiryDate'] == date(2023, 8, 31)) & (fno_symbol_list['ScriptCode'] == 'NIFTY')]

fno_symbol_list = fno_symbol_list[fno_symbol_list['ScriptCode'] == 'NIFTY']
fno_symbol_list = fno_symbol_list[fno_symbol_list['ExpiryDate'] == date(2023, 9, 7)]

split_list = fno_symbol_list[fno_symbol_list.columns[1]].str.split(' ', expand=True)
col_len = len(split_list.columns) - 1

fno_symbol_list['StrikePrice'] = split_list[col_len - 1].astype(float, errors='ignore')
fno_symbol_list['OptionType'] = split_list[col_len]


def get_strike_price():
    symbol_oc = fno_symbol_list.copy()
    symbol_oc['StrikePrice'] = symbol_oc['StrikePrice'].astype(float, errors='ignore')

    premium = 60

    def get_ltp(symbols, f_model):
        data = {'symbols': symbols}
        response = f_model.get_quotes(data=data)
        return response['d'][0]['v']['lp']

    symbol = 'NIFTY'
    spot_ltp = get_ltp('NSE:NIFTY50-INDEX', model)
    # print(spot_ltp)

    ce_spot_ltp = spot_ltp * (1 - 0.03)
    filter_ce = symbol_oc[(symbol_oc['OptionType'] == 'CE') & (symbol_oc['StrikePrice'] >= ce_spot_ltp) & (
            symbol_oc['ScriptCode'] == symbol)].sort_values(by='StrikePrice')

    # print(filter_ce)

    pe_spot_ltp = spot_ltp * (1 - 0.03)
    filter_pe = symbol_oc[(symbol_oc['OptionType'] == 'PE') & (symbol_oc['StrikePrice'] >= pe_spot_ltp) & (
            symbol_oc['ScriptCode'] == symbol)].sort_values(by='StrikePrice', ascending=False)

    # print(filter_pe)

    symbolTicker = filter_ce[:49]['SymbolTicker'].tolist()
    # print(symbolTicker)

    symbols = ""
    for st in symbolTicker:
        symbols = f'{symbols}{st},'
    symbols = symbols[:-1]
    data = {'symbols': symbols}
    res = model.get_quotes(data)
    ltp_dict = {}

    if 's' in res and res['s'] == 'ok':
        for i in res['d']:
            ltp_dict.update({i['n']: i['v']['lp']})

    # print('\n')
    # print(ltp_dict)
    # print('\n')

    initial_value = 100000
    filter_stock = None
    for optsymbol, optltp in ltp_dict.items():
        if abs(optltp - premium) < initial_value:
            initial_value = abs(optltp - premium)
            filter_stock = optsymbol

    print(filter_stock)


# get_strike_price()


def func_1():
    data = {
        "symbol": get_strike_price(),
        "resolution": "5",
        "date_format": "1",
        "range_from": "2021-08-31",
        "range_to": "2021-09-01",
        "cont_flag": "1"
    }

    json_data = model.get_history(data)

    # Create Python object from JSON string data
    obj = json.loads(json.dumps(json_data))

    # Pretty Print JSON
    json_formatted_str = json.dumps(obj, indent=4)
    print(json_formatted_str)


func_1()
