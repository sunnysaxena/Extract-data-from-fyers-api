def minute_1_to_five5(df):
    # set timestamp as index
    df.set_index('timestamp', inplace=True)

    # resample to 5 minutes intervals
    return df.resample('5T ').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    })


def minute_1_to_ten10(df):
    # set timestamp as index
    df.set_index('timestamp', inplace=True)

    # resample to 10 minutes intervals
    return df.resample('10T').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    })


def minute_1_to_fifteen15(df):
    # set timestamp as index
    df.set_index('timestamp', inplace=True)

    # resample to 15 minutes intervals
    return df.resample('15T').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    })


def minute_1_to_hourly(df):
    # set timestamp as index
    df.set_index('timestamp', inplace=True)

    # resample to hourly intervals
    return df.resample('H').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    })


def minute_1_to_day(df):
    # set timestamp as index
    df.set_index('timestamp', inplace=True)

    # resample to monthly intervals
    return df.resample('D').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    })


def minute_1_to_weekly(df):
    # set timestamp as index
    df.set_index('timestamp', inplace=True)

    # resample to weekly intervals
    return df.resample('W').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    })


def minute_1_to_monthly(df):
    # set timestamp as index
    df.set_index('timestamp', inplace=True)

    # resample to monthly intervals
    return df.resample('M').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    })


def minute_1_to_other(df, timeframe='5T'):
    # set timestamp as index
    df.set_index('timestamp', inplace=True)

    # resample to other intervals
    return df.resample(timeframe).agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    })
