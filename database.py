import sqlite3
from datetime import datetime, timezone

class Database:
    def __init__(self, db_name='ohlcv_data.db'):
        self.db_name = db_name
        self.conn = sqlite3.connect(self.db_name)
        self.cursor = self.conn.cursor()
        # self.create_table()

    def create_table(self,query=None):
        if query is None:
            self.cursor.execute(f'''{query}''')
        else:
            self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS ohlcv_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            UTC TEXT NOT NULL,
            Ticker TEXT NOT NULL,
            Close REAL NOT NULL,
            High REAL NOT NULL,
            Low REAL NOT NULL,
            Open REAL NOT NULL,
            Volume REAL NOT NULL
        )
        ''')
        self.conn.commit()

    def insert_data(self, ticker, data):
        """
        Insert OHLCV data into the database.

        :param ticker: The ticker symbol (e.g., 'BTC/USD').
        :param data: A list of OHLCV data, where each row is a dictionary with keys:
                     'timestamp', 'close', 'high', 'low', 'open', 'volume'.
        """
        for row in data:
            # Parse the timestamp to UTC ISO format
            utc_time = datetime.fromisoformat(row['timestamp']).astimezone(timezone.utc).isoformat()

            # Extract data for insertion
            close = row['close']
            high = row['high']
            low = row['low']
            open_price = row['open']
            volume = row['volume']

            # Execute the SQL insertion
            self.cursor.execute('''
            INSERT INTO ohlcv_data (UTC, Ticker, Close, High, Low, Open, Volume)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (utc_time, ticker, close, high, low, open_price, volume))

        # Commit the transaction
        self.conn.commit()

    def delete_data(self, ticker=None, before_date=None):
        """
        Delete data from the database based on conditions.

        :param ticker: The ticker symbol to delete data for (optional).
        :param before_date: A UTC ISO date string. Deletes data before this date (optional).
        """
        query = "DELETE FROM ohlcv_data WHERE 1=1"  # Base query to allow conditional appending
        params = []

        if ticker:
            query += " AND Ticker = ?"
            params.append(ticker)

        if before_date:
            query += " AND UTC < ?"
            params.append(before_date)

        self.cursor.execute(query, tuple(params))
        self.conn.commit()

    def close(self):
        """
        Commit changes and close the database connection.
        """
        self.conn.commit()
        self.conn.close()


# 사용 예시
if __name__ == "__main__":
    # 예제 데이터 (timestamp, close, high, low, open, volume)
    response = [
        [1638307200000, 57000.0, 58000.0, 56000.0, 56500.0, 120.5],
        [1638393600000, 58000.0, 59000.0, 57000.0, 57500.0, 150.7],
    ]
    ticker = "BTC/USD"

    # Database 클래스 사용
    db = Database()
    db.insert_data(ticker, response)
    db.close()

    print("데이터가 성공적으로 삽입되었습니다.")
