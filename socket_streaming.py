import ccxt.pro as ccxtpro
import asyncio
import os

class CustomAlpacaWs(ccxtpro.alpaca):
    def describe(self):
        return self.deep_extend(super(CustomAlpacaWs, self).describe(), {
            'urls': {
                'api': {
                    'ws': {
                        'crypto': 'wss://stream.data.sandbox.alpaca.markets/v1beta3/crypto',  # Sandbox WebSocket URL for crypto
                        'trading': 'wss://paper-api.alpaca.markets/stream',  # Sandbox WebSocket URL for trading
                    },
                },
                'test': {  # Optional: Override test endpoints if needed
                    'ws': {
                        'crypto': 'wss://stream.data.alpaca.markets/v1beta3/crypto/us',
                        'trading': 'wss://paper-api.alpaca.markets/stream/us',
                    },
                },
            },
        })

class AlpacaWebSocket:
    def __init__(self, api_key, secret_key, symbol, timeframe='1m', sandbox_mode=True):
        self.api_key = os.getenv('ALPACA_API_KEY')
        self.secret_key = os.getenv('ALPACA_SECRET_KEY')
        self.symbol = symbol
        self.timeframe = timeframe
        self.sandbox_mode = sandbox_mode
        self.exchange = None

    async def initialize_exchange(self):
        self.exchange = CustomAlpacaWs({
            'apiKey': self.api_key,
            'secret': self.secret_key,
        })
        if self.sandbox_mode:
            self.exchange.set_sandbox_mode(enabled=True)
        await self.exchange.load_markets()

    async def stream_data(self):

        try:
            await self.initialize_exchange()
            print(f"Streaming data for {self.symbol} on timeframe {self.timeframe}")
            while True:
                try:
                    # Watch order book data
                    orderbook = await self.exchange.watch_order_book(self.symbol)
                    print("Orderbook Data:", orderbook)

                    # Uncomment to watch OHLCV data
                    # ohlcv = await self.exchange.watch_ohlcv(self.symbol, self.timeframe)
                    # print("OHLCV Data:", ohlcv)
                except Exception as e:
                    print("Error in data fetch:", e)
                    await asyncio.sleep(3)
        except Exception as e:
            print("Fatal error in WebSocket connection:", e)
        finally:
            if self.exchange:
                await self.exchange.close()

    def run(self):
        if os.name == "nt":  # For Windows compatibility with asyncio
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(self.stream_data())



# Example usage
if __name__ == "__main__":
    api_key = os.getenv("ALPACA_API_KEY")
    secret_key = os.getenv("ALPACA_SECRET_KEY")

    # Symbol and timeframe to stream
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