from socket_streaming import AlpacaWebSocket
from  api_calls import CustomAlpacaAPI
import part_4.SUBMISSION.database as database
import os
from config import API_KEY, API_SECRET

ticker = "BTC/USD"
# Alpaca API credentials from environment variables
api_key = API_KEY
secret_key = API_SECRET

# Initialize the custom API handler
alpaca_api = CustomAlpacaAPI(api_key, secret_key)

# Example 1: Fetch OHLCV data
symbol = 'BTC/USD'
timeframe = '1m'
api_instance = CustomAlpacaAPI(api_key, secret_key)
ohlcv_data = api_instance.fetch_ohlcv(symbol=symbol, timeframe= timeframe)
print(ohlcv_data)
# Database 클래스 사용

db = database.Database()
db.insert_data(ticker=symbol, data = ohlcv_data)
db.close()

print("데이터가 성공적으로 삽입되었습니다.")

symbol = "BTC/USD"
timeframe = "1m"

# Initialize the WebSocket streaming class
alpaca_ws = AlpacaWebSocket(
    api_key=api_key,
    secret_key=secret_key,
    symbol=symbol,
    timeframe=timeframe,
    sandbox_mode=True
)

# Run the WebSocket stream
alpaca_ws.run()

