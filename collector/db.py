import os
import pandas as pd
import constants as ct
from configparser import ConfigParser
from db_connection import db_connection

config = ConfigParser()
config.read('credentials.ini')

database = config['mongo']['database']
client = db_connection()


def store_data(collection_name, df):

    try:
        db = client[database]
        index = db[collection_name]

        df.reset_index(inplace=True)
        document = df.to_dict('records')
        index.insert_many(document)

        print(f"{collection_name} : inserted")
    except Exception as e:
        print("[+] Database connection error!")
        raise e


all_files = os.listdir(ct.option_path)

for file in all_files:
    df = pd.read_csv(f'{ct.option_path}/{file}')
    df.drop_duplicates(inplace=True)
    # store_data(collection_name=file.split('.')[0], df=df)



