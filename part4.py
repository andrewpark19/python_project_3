pip install alpaca-trade-api

# Set your API key and secret
api_key = 'AKW6V8Y7VDE6Y13I6CLX'
api_secret = 'qg0BEcaGbnBNmeLJPd3xJgptfjcGrmiOuBBF9feT'
base_url = 'https://paper-api.alpaca.markets'  # Use paper trading base URL for testing

# Initialize Alpaca API
api = tradeapi.REST(api_key, api_secret, base_url, api_version='v2')

# Get historical market data
symbol = 'AAPL'  # Replace with your desired stock symbol
timeframe = '1D'  # Daily timeframe
historical_data = api.get_barset(symbol, timeframe).df