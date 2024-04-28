import pandas as pd
from utility import delete_duplicate_rows
from sqlalchemy import text
from db_connection import get_mysql_connection


# 'finnifty_5m', 'finnifty_15m', 'finnifty_1D', 'finnifty_week', 'finnifty_month'

# 'nifty50_5m', 'nifty50_15m', 'nifty50_1D', 'nifty50_week', 'nifty50_month'

# 'niftybank_5m', niftybank_15m', 'niftybank_1D', 'niftybank_week' 'niftybank_month'

# 'indiavix_5m', 'indiavix_15m', 'indiavix_1D', 'indiavix_week', 'indiavix_month'


def update_table(table_name='indiavix_5m'):
    query = f"SELECT * FROM {table_name};"
    drop_table = f"DROP TABLE {table_name};"
    engine = get_mysql_connection()

    try:
        with engine.connect() as connection:

            # df1 = pd.read_csv('/home/sunny/StockBook/zxtra/finnifty_2017_2022_5min.csv')
            df = pd.read_csv('/home/sunny/StockBook/zxtra/indiavix_2017_2023_5min.csv')
            # df2 = pd.read_sql(query, engine)

            print(table_name)

            # df = pd.concat([df1, df2], axis=0)
            # df.drop_duplicates(inplace=True)
            # df['volume'] = 0
            #
            df = delete_duplicate_rows(df)
            # df.to_csv('finnifty_5min_backup.csv', index=False)

            print('\n')
            # print(df.head())
            # print(df.tail())
            # print(df.info())
            # print(df.shape)

            # connection.execute(text(drop_table))
            df.to_sql(name=table_name, con=get_mysql_connection(), index=False, if_exists='append')
            print('donnee...')
    except Exception as e:
        print('Something went wrong : ', e)


# update_table()


def save_data(table_name='indiavix_5m'):
    query = f"SELECT * FROM {table_name};"
    drop_table = f"DROP TABLE {table_name};"
    engine = get_mysql_connection()

    try:
        with engine.connect() as connection:

            df = pd.read_sql(query, engine)

            print(table_name)

            df.to_csv('all_nifty_50_5min_backup.csv', index=False)

    except Exception as e:
        print('Something went wrong : ', e)

# save_data()
