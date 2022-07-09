import json
import sqlite3, config_polygon
from alpaca_trade_api.rest import TimeFrame
import alpaca_trade_api as tradeapi
import config
import psycopg2
import psycopg2.extras 
import datetime, time
import urllib.request, json 
from psycopg2.extras import execute_values

start = time.time()

connection = psycopg2.connect(host=config_polygon.DB_HOST,database=config_polygon.DB_NAME, user=config_polygon.DB_USER, password=config_polygon.DB_PASS, port=config_polygon.DB_PORT)

cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

cursor.execute("""
    SELECT * FROM stock WHERE symbol IN ('AAPL','TSLA','MSFT','META','GOOG','NFLX')
    """)

stocks = cursor.fetchall()

symbol_urls = {}
for stock in stocks:
    symbol_urls[stock['id']] = f"https://api.polygon.io/v2/aggs/ticker/{stock['symbol']}/range/1/minute/2022-01-01/2022-06-30?apiKey={config.API_KEY}&limit=50000"

for id, value in symbol_urls.items():
    print(f"Reading {value}")
    try:
        with urllib.request.urlopen(value) as url:
            response = json.loads(url.read().decode())
            params = [(id, datetime.datetime.fromtimestamp(bar['t'] / 1000.0), round(bar['o'], 2), round(bar['h'], 2), round(bar['l'], 2), round(bar['c'], 2), bar['v'], round(bar['vw'], 2), bar['n']) for bar in response['results']]
            execute_values(cursor," INSERT INTO stock_price (stock_id, dt, open, high, low, close, volume, vwap, ntrades) VALUES %s", params)

    except Exception as e:
        print("Unable to get url {} due to {}.".format(value, e.__class__))

        
connection.commit()

end = time.time()

print("Took {} seconds.".format(end - start))

# chunk_size = 50000 

# for i in range(0, len(symbols), chunk_size):
#     symbol_chunk = symbols[i:i+chunk_size]

#     barsets = api.get_bars(symbol_chunk,TimeFrame.Day,"2022-01-31", "2022-03-31")

#     for bar in barsets:
#         symbol = bar.S
#         print(f"processing symbol {symbol}")

#         stock_id = stock_dict[symbol]
#         cursor.execute("""
#             INSERT INTO stock_price (stock_id, date, open, high, low, close, volume)
#             VALUES (?, ?, ?, ?, ?, ?, ?)
#         """, (stock_id, bar.t.date(), bar.o, bar.h, bar.l, bar.c, bar.v))
