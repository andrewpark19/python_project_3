import ccxt
import pandas as pd
import numpy as np
import time
from collections import deque
from alpaca_trade_api.rest import REST

api_key = 'AKW6V8Y7VDE6Y13I6CLX'
api_secret = 'qg0BEcaGbnBNmeLJPd3xJgptfjcGrmiOuBBF9feT'
base_url = 'https://paper-api.alpaca.markets'


class PairTradingStrategy:
    def __init__(self, alpaca_api_key, alpaca_secret, alpaca_base_url):

        self.exchange = getattr(ccxt, 'alpaca')({
            'apiKey': alpaca_api_key,
            'secret': alpaca_secret,
            'enableRateLimit': False,
        })

        self.alpaca_client = REST(
            alpaca_api_key,
            alpaca_secret,
            alpaca_base_url
        )

        # Data storage for spreads (30 day rolling window, will pull from actual database)
        self.spread_history = deque(maxlen=30*60*12)

    def fetch_prices(self):
        usdc_ticker = self.exchange.fetch_ticker('USDC/USD')
        usdt_ticker = self.exchange.fetch_ticker('USDT/USD')
        usdc_price = usdc_ticker['last']
        usdt_price = usdt_ticker['last']
        return usdc_price, usdt_price

    def update_spread(self, usdc_price, usdt_price):
        spread = usdc_price - usdt_price
        self.spread_history.append(spread)
        return spread

    def create_signal(self):
        if len(self.spread_history) < 30*60*12:
            print('Not enough data to create signal.'.format(len(self.spread_history))) 
            return None 
        mean_spread = np.mean(self.spread_history)
        std_dev = np.std(self.spread_history)
        current_spread = self.spread_history[-1]
        if current_spread > mean_spread + 1 * std_dev:
            return 'SELL_USDC_BUY_USDT'
        elif current_spread < mean_spread - 1 * std_dev:
            return 'BUY_USDC_SELL_USDT'
        else:
            return None

    def execute_trade(self, signal):
        amount = 1000
        if signal == 'SELL_USDC_BUY_USDT':
            self.alpaca_client.submit_order(
                symbol='USDC/USD',
                qty=amount,
                side='sell',
                type='market',
                time_in_force='gtc'
            )

            self.alpaca_client.submit_order(
                symbol='USDT/USD',
                qty=amount,
                side='buy',
                type='market',
                time_in_force='gtc'
            )
            print("Executed SELL_USDC_BUY_USDT")
        elif signal == 'BUY_USDC_SELL_USDT':

            self.alpaca_client.submit_order(
                symbol='USDC/USD',
                qty=amount,
                side='buy',
                type='market',
                time_in_force='gtc'
            )

            self.alpaca_client.submit_order(
                symbol='USDT/USD',
                qty=amount,
                side='sell',
                type='market',
                time_in_force='gtc'
            )
            print("Executed BUY_USDC_SELL_USDT")
        else:
            print("No valid signal to execute.")

    def run(self):
        while True:
            try:
                usdc_price, usdt_price = self.fetch_prices()
                spread = self.update_spread(usdc_price, usdt_price)
                signal = self.create_signal()
                if signal:
                    self.execute_trade(signal)

                time.sleep(10)
            except Exception as e:
                print(f"Error: {e}")
                time.sleep(10)

strategy = PairTradingStrategy(
    alpaca_api_key=api_key,
    alpaca_secret=api_secret,
    alpaca_base_url=base_url
)
strategy.run()
        