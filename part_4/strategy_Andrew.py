import ccxt
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta, timezone
from collections import deque
from alpaca_trade_api.rest import REST, TimeFrame
from strategy_monitoring import get_positions, get_PnL, successful_trades, drawdowns

# api_key = 'AKW6V8Y7VDE6Y13I6CLX'
# api_secret = 'qg0BEcaGbnBNmeLJPd3xJgptfjcGrmiOuBBF9feT'
# base_url = 'https://paper-api.alpaca.markets'

# CAN SOMEONE CHECK THIS PART FOR AUTHORIZATION
api_key = "PKI5T4EWCKE65UWKW0MC"
api_secret = "IzBbUa6ufG1WKjYlYfgZgLw6vyIfRTnCNB4Lowv1"
base_url = "https://paper-api.alpaca.markets/v2"

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

        # THIS IS WHERE DATA NEEDS TO BE PULLED FROM THE STORAGE DATABASE
        # Data storage for spreads (30 day rolling window, will pull from actual database)
        # Currently can't test data since I need 30 days worth of 1 minute tick data
        self.spread_history = deque(maxlen=30*60*24)
        self.initialize_spread_history()

    def initialize_spread_history(self):
        print("Initializing spread history with historical data...")

        # Calculate the start and end dates for the data fetch
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=30)

        # Symbols for USDC and USDT on Alpaca
        usdc_symbol = 'USDC/USD'
        usdt_symbol = 'USDT/USD'

        # Fetch historical bars for USDCUSD
        print(f"Fetching historical data for {usdc_symbol}")
        usdc_bars = self.alpaca_client.get_crypto_bars(
            usdc_symbol,
            TimeFrame.Minute,
            start=start_date.isoformat(),
            end=end_date.isoformat()
        ).df

        # Fetch historical bars for USDTUSD
        print(f"Fetching historical data for {usdt_symbol}")
        usdt_bars = self.alpaca_client.get_crypto_bars(
            usdt_symbol,
            TimeFrame.Minute,
            start=start_date.isoformat(),
            end=end_date.isoformat()
        ).df

        if usdc_bars.empty or usdt_bars.empty:
            print("Error: Historical data for USDCUSD or USDTUSD is empty.")
            return

        # Reset index to have datetime as a column
        usdc_bars.reset_index(inplace=True)
        usdt_bars.reset_index(inplace=True)

        # Rename 'symbol' columns to differentiate
        usdc_bars.rename(columns={'close': 'close_usdc'}, inplace=True)
        usdt_bars.rename(columns={'close': 'close_usdt'}, inplace=True)

        # Merge the dataframes on 'timestamp'
        merged_df = pd.merge(usdc_bars[['timestamp', 'close_usdc']], usdt_bars[['timestamp', 'close_usdt']], on='timestamp', how='inner')

        # Calculate the spread
        merged_df['spread'] = merged_df['close_usdc'] - merged_df['close_usdt']

        # Populate the spread_history deque
        self.spread_history.extend(merged_df['spread'].tolist())

        print("Spread history initialization complete.")

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
            print(f'Not enough data to create signal. Current data points: {len(self.spread_history)}') 
            return None 
        mean_spread = np.mean(self.spread_history)
        std_dev = np.std(self.spread_history)
        current_spread = self.spread_history[-1]
        if current_spread > mean_spread +  1.5 * std_dev:
            return 'SELL_USDC_BUY_USDT'
        elif current_spread < mean_spread - 1.5 * std_dev:
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

                # get_PnL(api_key, api_secret)
                # time.sleep(2)
                # successful_trades(api_key, api_secret)
                # time.sleep(2)
                # drawdowns(api_key, api_secret)
                # time.sleep(2)
                # get_positions(api_key, api_secret)

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

        