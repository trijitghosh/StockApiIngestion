import csv
from json import dumps

import requests
from kafka import KafkaProducer

CSV_URL = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY_EXTENDED&symbol=IBM&interval=1min&apikey=77KYLZ992AAAMOD0&outputsize=full'

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))

with requests.Session() as s:
    download = s.get(CSV_URL)
    decoded_content = download.content.decode('utf-8')
    cr = csv.reader(decoded_content.splitlines(), delimiter=',')
    data = "'time': {time}, 'open': {open}, 'high': {high}, 'low':{low}, 'close': {close}, 'symbol':'IBM'"
    next(cr)
    while True:
        try:
            row = next(cr)
            producer.send('time_series_intraday_prices', '{'+data.format(time=row[0], open=float(row[1]), high=float(row[2]), low=float(row[3]), close=float(row[4]))+'}')
        except StopIteration as e:
            break

    producer.flush()
