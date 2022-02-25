import csv
import time
from json import dumps

import requests
from kafka import KafkaProducer

CSV_URL = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY_EXTENDED&symbol=IBM&interval=1min&apikey=77KYLZ992AAAMOD0&outputsize=full'

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))


def return_epoch(elem):
    return int(time.mktime(time.strptime(elem[0], '%Y-%m-%d %H:%M:%S')))


with requests.Session() as s:
    download = s.get(CSV_URL)
    decoded_content = download.content.decode('utf-8')
    cr = csv.reader(decoded_content.splitlines(), delimiter=',')
    my_list = list(cr)
    my_list.pop(0)
    my_list = sorted(my_list, key=return_epoch)
    for row in my_list:
        producer.send('time_series_intraday_prices', dict(time=row[0], open=float(row[1]), high=float(row[2]),
                                                          low=float(row[3]), close=float(row[4]), symbol='IBM'))

    producer.flush()
