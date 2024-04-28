import os
import pymysql
import sqlalchemy
import urllib.parse
from configparser import ConfigParser

config = ConfigParser()
config.read('credentials.ini')

host = config['mysql']['host']
port = config['mysql']['port']
username = config['mysql']['username']
password = config['mysql']['password']
database = config['mysql']['database']

username = urllib.parse.quote_plus(username)
password = urllib.parse.quote_plus(password)


def get_mysql_connection():
    try:
        return sqlalchemy.create_engine(f'mysql+pymysql://{username}:{password}@{host}:{port}/{database}')
    except Exception as e:
        print("Something went wrong:", e)


def get_mongo_connection():
    try:
        return sqlalchemy.create_engine(f'mysql+pymysql://{username}:{password}@{host}:{port}/{database}')
    except Exception as e:
        print("Something went wrong:", e)


def get_mysql_pymysql_connection():
    try:
        return pymysql.connect(host='localhost',
                                       user='root',
                                       password='',
                                       db='tutorfall2016',
                                       charset='utf8',
                                       cursorclass=pymysql.cursors.DictCursor)
    except Exception as e:
        print("Something went wrong:", e)


if __name__ == '__main__':
    get_mysql_connection()
