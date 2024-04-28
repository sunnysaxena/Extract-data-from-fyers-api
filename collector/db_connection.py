import urllib.parse
from pymongo import MongoClient
from configparser import ConfigParser

config = ConfigParser()
config.read('credentials.ini')

username = config['mongo']['username']
passwd = config['mongo']['passwd']
host = config['mongo']['host']
database = config['mongo']['database']
port = config['mongo']['port']

username = urllib.parse.quote_plus(username)
password = urllib.parse.quote_plus(passwd)


def db_connection():
    try:
        client = MongoClient(f"mongodb://{username}:{password}@{host}:{port}")
        print("[+] Database connected!")
    except Exception as e:
        print("[+] Database connection error!")
        raise e
    return client

