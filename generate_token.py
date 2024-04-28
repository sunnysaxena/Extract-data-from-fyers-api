import os
import webbrowser
import configparser

from constants import *
from fyers_apiv3 import fyersModel

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


def generate_access_token():
    session = fyersModel.SessionModel(
        client_id=client_id,
        secret_key=secret_key,
        redirect_uri=redirect_url,
        response_type=response_type,
        grant_type=grant_type
    )

    response = session.generate_authcode()

    print("Login Url : ", response)

    # This command is used to open the url in default system browser
    webbrowser.open(response, new=1)

    auth_code = input("Auth Code : ")

    session.set_token(auth_code)
    access_token = session.generate_token()['access_token']

    if os.path.exists(file_name):
        os.remove(file_name)

    with open(file_name, 'w') as f:
        f.write(access_token)


if __name__ == '__main__':
    generate_access_token()
