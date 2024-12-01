from ccxt import alpaca
from datetime import datetime, timezone
import os

class CustomAlpaca(alpaca):
    """
    Custom implementation of the Alpaca API with overridden test URLs.
    """
    def describe(self):
        return self.deep_extend(super(CustomAlpaca, self).describe(), {
            'urls': {
                'test': {
                    'broker': 'https://broker-api.sandbox.{hostname}',
                    'trader': 'https://paper-api.{hostname}',
                    'market': 'https://data.{hostname}',  # Overriding test market URL
                    'wss': 'wss://paper-api.{hostname}/stream'  # WebSocket URL override
                },
            },
        })

class CustomAlpacaAPI:
    """
    Generalized class to interact with the Alpaca API for multiple use cases.
    """

    def __init__(self, api_key, secret_key, sandbox_mode=True):
        """
        Initialize the CustomAlpaca client with API credentials.
        """
        self.alpaca = CustomAlpaca({
            'apiKey': os.getenv('ALPACA_API_KEY'),
            'secret': os.getenv('ALPACA_SECRET_KEY'),
            'enableRateLimit': True
        })
        if sandbox_mode:
            self.alpaca.set_sandbox_mode(enabled=True)

        # Load markets for faster symbol recognition
        self.alpaca.load_markets()
        print("CustomAlpacaAPI initialized.")

    def fetch_ohlcv(self, symbol, timeframe='1m'):
        """
        Fetch OHLCV data for a given symbol and timeframe.

        :param symbol: Trading pair symbol (e.g., 'BTC/USD').
        :param timeframe: Timeframe for OHLCV data (e.g., '1m').
        :return: List of OHLCV data rows.
        """
        print(f"Fetching OHLCV data for {symbol} with timeframe {timeframe}...")
        response = self.alpaca.fetch_ohlcv(symbol, timeframe=timeframe)

        # Convert timestamps to human-readable UTC format
        ohlcv_data = [
            {
                'timestamp': datetime.fromtimestamp(row[0] / 1000, tz=timezone.utc).isoformat(),
                'open': row[1],
                'high': row[2],
                'low': row[3],
                'close': row[4],
                'volume': row[5]
            }
            for row in response
        ]

        print(f"Fetched {len(ohlcv_data)} rows of OHLCV data for {symbol}.")
        return ohlcv_data

    def fetch_order_book(self, symbol, limit=10):
        """
        Fetch the order book for a given symbol.

        :param symbol: Trading pair symbol (e.g., 'BTC/USD').
        :param limit: The maximum number of order book entries to fetch.
        :return: The order book data.
        """
        print(f"Fetching order book for {symbol} with limit {limit}...")
        response = self.alpaca.fetch_order_book(symbol, limit=limit)
        print(f"Fetched order book for {symbol}.")
        return response

    def fetch_balance(self):
        """
        Fetch the account balance.

        :return: Account balance data.
        """
        print("Fetching account balance...")
        response = self.alpaca.fetch_balance()
        print("Fetched account balance.")
        return response

    def api_call(self, method, *args, **kwargs):
        """
        Generalized method to call any supported API functionality.

        :param method: The API method to call (e.g., 'fetch_ohlcv', 'fetch_balance').
        :param args: Positional arguments for the API method.
        :param kwargs: Keyword arguments for the API method.
        :return: The response from the API call.
        """
        if not hasattr(self.alpaca, method):
            raise ValueError(f"Method '{method}' is not supported by the Alpaca API.")
        print(f"Calling API method: {method} with args: {args} and kwargs: {kwargs}...")
        response = getattr(self.alpaca, method)(*args, **kwargs)
        print(f"API method {method} executed successfully.")
        return response

    def close(self):
        """
        Cleanup resources if necessary.
        """
        print("Closing CustomAlpacaAPI...")
        # Perform any necessary cleanup
        print("CustomAlpacaAPI closed.")


# Example usage
if __name__ == "__main__":

    # Alpaca API credentials from environment variables
    api_key = os.getenv("ALPACA_API_KEY")
    secret_key = os.getenv("ALPACA_SECRET_KEY")

    # Initialize the custom API handler
    alpaca_api = CustomAlpacaAPI(api_key, secret_key)

    # Example 1: Fetch OHLCV data
    symbol = 'BTC/USD'
    timeframe = '1m'
    ohlcv_data = alpaca_api.fetch_ohlcv(symbol, timeframe)
    print("OHLCV Data:", ohlcv_data[:5])  # Print first 5 rows

    # Example 2: Fetch order book
    order_book = alpaca_api.fetch_order_book(symbol, limit=5)
    print("Order Book:", order_book)

    # Example 3: Fetch account balance
    balance = alpaca_api.fetch_balance()
    print("Balance:", balance)

    # Example 4: Generalized API call
    ohlcv_via_api_call = alpaca_api.api_call('fetch_ohlcv', symbol, timeframe='5m')
    print("OHLCV Data (via api_call):", ohlcv_via_api_call[:5])
