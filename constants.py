time_zone = 'Asia/Kolkata'
option_path = 'data/options'
banks_path = 'data/banks'

option_symbols = {
    'indiavix': 'NSE:INDIAVIX-INDEX',

    'nifty50': 'NSE:NIFTY50-INDEX',

    'niftybank': 'NSE:NIFTYBANK-INDEX',

    'finnifty': 'NSE:FINNIFTY-INDEX'
}

option_symbols_yahoo = {
    'indiavix': '^INDIAVIX',

    'nifty50': '^NSEI',

    'niftybank': '^NSEBANK',

    'finnifty': 'NIFTY_FIN_SERVICE.NS'
}

stocks_option_symbols = {
    'tata_power': 'NSE:TATAPOWER-EQ',

    'ongc_oil': 'NSE:ONGC-EQ',

    'power_grid': 'NSE:POWERGRID-EQ',
}

trend_types = [
    'Uptrend',
    'Downtrend',
    'Sideways',
    'Reversal',
    'Volatility',
    'Seasonal',
    'Random or Chaotic',
    'Range Bound'
]