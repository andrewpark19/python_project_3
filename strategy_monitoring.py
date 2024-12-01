import ccxt
import pandas as pd
import numpy as np
import time
from collections import deque
from alpaca_trade_api.rest import REST
import requests
import json
import numpy as np
#from alpaca.trading.client import TradingClient

api_key = 'AKW6V8Y7VDE6Y13I6CLX'
api_secret = 'qg0BEcaGbnBNmeLJPd3xJgptfjcGrmiOuBBF9feT'
base_url = 'https://paper-api.alpaca.markets'



def get_positions(api_key, api_secret):

    url = "https://paper-api.alpaca.markets/v2/positions"

    headers = {
        "accept": "application/json",
        "APCA-API-KEY-ID": "PKI5T4EWCKE65UWKW0MC",
        "APCA-API-SECRET-KEY": "IzBbUa6ufG1WKjYlYfgZgLw6vyIfRTnCNB4Lowv1"
    }

    response = requests.get(url, headers=headers)
    #portfolio = trading_client.get_all_positions()
    return print(response.text)

def get_PnL(api_key, api_secret):

    url = "https://paper-api.alpaca.markets/v2/account/portfolio/history?intraday_reporting=continuous&pnl_reset=no_reset"

    headers = {
        "accept": "application/json",
        "APCA-API-KEY-ID": "PKI5T4EWCKE65UWKW0MC",
        "APCA-API-SECRET-KEY": "IzBbUa6ufG1WKjYlYfgZgLw6vyIfRTnCNB4Lowv1"
    }

    response = requests.get(url, headers=headers)

    # print(response.text)
    data = response.json()

    profit_loss = data["profit_loss"]
    print("Current PnL: " + str(sum(profit_loss) + 100000))
    return sum(profit_loss) + 100000

def successful_trades(api_key, api_secret):

    url = "https://paper-api.alpaca.markets/v2/account/portfolio/history?intraday_reporting=continuous&pnl_reset=no_reset"

    headers = {
        "accept": "application/json",
        "APCA-API-KEY-ID": "PKI5T4EWCKE65UWKW0MC",
        "APCA-API-SECRET-KEY": "IzBbUa6ufG1WKjYlYfgZgLw6vyIfRTnCNB4Lowv1"
    }

    response = requests.get(url, headers=headers)

    # print(response.text)
    data = response.json()

    profit_loss = data["profit_loss"]
    successful_trades = len([x for x in profit_loss if x > 0])
    unsuccessful_trades = len([x for x in profit_loss if x <= 0])
    print(str(successful_trades) + str(" successful trades out of ") + str(successful_trades+ unsuccessful_trades) + str(" trades"))
    return str(successful_trades) + str(" successful trades out of ") + str(successful_trades+ unsuccessful_trades) + str(" trades")

def drawdowns(api_key, api_secret):

    # Your equity data from the JSON
    url = "https://paper-api.alpaca.markets/v2/account/portfolio/history?intraday_reporting=continuous&pnl_reset=no_reset"

    headers = {
        "accept": "application/json",
        "APCA-API-KEY-ID": "PKI5T4EWCKE65UWKW0MC",
        "APCA-API-SECRET-KEY": "IzBbUa6ufG1WKjYlYfgZgLw6vyIfRTnCNB4Lowv1"
    }

    response = requests.get(url, headers=headers)

    # print(response.text)
    data = response.json()
    equity = data["equity"]

    # Calculate the peak equity at each point
    peaks = np.maximum.accumulate(equity)

    # Calculate drawdowns at each time step
    drawdowns = peaks - equity

    # Calculate drawdown as a percentage of the peak equity
    drawdown_pct = (drawdowns / peaks) * 100

    # Find the maximum drawdown
    max_drawdown = max(drawdowns)  # Absolute drawdown
    max_drawdown_pct = max(drawdown_pct)  # Maximum drawdown percentage

    # Results
    #print(f"Drawdowns: {drawdowns}")
    #print(f"Drawdowns (%): {drawdown_pct}")
    print(f"Maximum Drawdown: {max_drawdown}")
    print(f"Maximum Drawdown (%): {max_drawdown_pct:.2f}%")

def test_PnL():
    alpaca_client = REST(
            'PKI5T4EWCKE65UWKW0MC',
            'IzBbUa6ufG1WKjYlYfgZgLw6vyIfRTnCNB4Lowv1', 
            'https://paper-api.alpaca.markets'
    )
    

    alpaca_client.submit_order(
        symbol='USDT/USD',
        qty=1,
        side='buy',
        type='market',
        time_in_force='gtc'
    )





test_PnL()
get_PnL(api_key, api_secret)
successful_trades(api_key, api_secret)
drawdowns(api_key, api_secret)
get_positions(api_key, api_secret)