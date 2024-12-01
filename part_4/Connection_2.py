import os
import numpy as np
import pandas as pd
import ccxt
import sqlite3
from ccxt import Exchange
from ccxt.abstract.alpaca import ImplicitAPI
from ccxt import alpaca
import json

# ENUM
api_key = 'PKI5T4EWCKE65UWKW0MC'
secret_key = 'IzBbUa6ufG1WKjYlYfgZgLw6vyIfRTnCNB4Lowv1'
# api_key = os.getenv("ALPACA_API_KEY")
# secret_key = os.getenv("ALPACA_SECRET_KEY")

class CustomAlpaca(alpaca):
    def describe(self):
        return self.deep_extend(super(CustomAlpaca, self).describe(), {
            'id': 'alpaca',
            'name': 'Alpaca',
            'countries': ['US'],
            # 3 req/s for free
            # 150 req/s for subscribers: https://alpaca.markets/data
            # for brokers: https://alpaca.markets/docs/api-references/broker-api/#authentication-and-rate-limit
            'rateLimit': 333,
            'hostname': 'alpaca.markets',
            'pro': True,
            'urls': {
                'logo': 'https://github.com/user-attachments/assets/e9476df8-a450-4c3e-ab9a-1a7794219e1b',
                'www': 'https://alpaca.markets',
                'api': {
                    'broker': 'https://broker-api.{hostname}',
                    'trader': 'https://api.{hostname}',
                    'market': 'https://data.{hostname}',
                },
                'test': {
                    'broker': 'https://broker-api.sandbox.{hostname}',
                    'trader': 'https://paper-api.{hostname}',
                    'market': 'https://data.{hostname}',  # Overriding test market URL
                },
                'doc': 'https://alpaca.markets/docs/',
                'fees': 'https://docs.alpaca.markets/docs/crypto-fees',
            },
        })


alpaca_ms = CustomAlpaca(config={
    'apiKey': api_key,          # Alpaca API Key
    'secret': secret_key,       # Alpaca Secret Key
    'enableRateLimit': True
    })

alpaca_ms.set_sandbox_mode(enabled=True) # 그냥 alpaca()는 위에 overriding을 하는듯 보였으나, ccxt,alpaca는 안됨

print("Test market URL:", alpaca_ms.describe()['urls']['test']['market'])
alpaca_ms.load_markets() # unnecessary but make it faster
alpaca_ms.verbose = True

alpaca_ms.fetch_ohlcv('BTC/USD', timeframe='1m')


# alpaca = ccxt.alpaca.describe()
# print(alpaca)
##############################################
# alpaca class instance
# alpaca = ccxt.alpaca(config={
#     'apiKey': api_key,          # Alpaca API Key
#     'secret': secret_key,       # Alpaca Secret Key
#     'enableRateLimit': True
#     })
# alpaca.set_sandbox_mode(enabled=True) # testnet - broker/trader/market endpoint depends on which method you call afterwards
# print("Test market URL:", alpaca.urls['test']['market'])
# # symbol = 'BTC/USD'
# # timeframe = '1m'
# # print("Market URL:", alpaca.urls['api']['market'])  # Verify URL override
# # print("Requesting OHLCV for:", symbol, "Timeframe:", timeframe)
# #
# # ohlcv = alpaca.fetch_ohlcv(symbol, timeframe)
#
#
#
#
# alpaca.load_markets() # unnecessary but make it faster
# alpaca.verbose = True
#
# alpaca.fetch_ohlcv('BTC/USD', timeframe='1m')
# print(pd.DataFrame(a['AAVE/USD']).columns)


##############################################
# alpaca.verbose = True  # 요청과 응답 로그 출력
# alpaca.describe()
#
# # 거래소 상태 확인
# try:
#     markets = alpaca.load_markets()
#     print("거래소가 성공적으로 설정되었습니다!")
#     print(markets)
# except Exception as e:
#     print(f"오류 발생: {e}")
# # params = {'asset_class': 'crypto', 'status': 'active'}
# temp = alpaca.fetch_trades(symbol='BTC/USD',limit=10) # 음...sandbox 라서 안되나...
# print(temp)

#
#
#
# # 요청 및 디버깅 로그 활성화
# alpaca.verbose = True
#
# # 시장 데이터 로드
# try:
#     alpaca.load_markets()
#     print("시장 데이터 로드 성공")
# except Exception as e:
#     print(f"오류 발생: {e}")




# # Alpaca Paper Trading 인스턴스 생성
# alpaca = ccxt.alpaca({
#     'apiKey': api_key,          # Alpaca API Key
#     'secret': secret_key,       # Alpaca Secret Key
#     'enableRateLimit': True,           # 요청 속도 제한
#     'options': {
#         'test': True                  # Paper Trading 활성화
#     }
# })
# alpaca.verbose = True  # 요청과 응답 로그 출력
# alpaca.load_markets()
# # ex = ccxt.alpaca()
# # 거래소 상태 확인
# try:
#     markets = alpaca.load_markets()
#     print("거래소가 성공적으로 설정되었습니다!")
#     print(markets)
# except Exception as e:
#     print(f"오류 발생: {e}")


