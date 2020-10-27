import psycopg2
from datetime import datetime
import os
from random import choice
import time

dsn = 'dbname={dbname} ' \
      'user={user} ' \
      'password={password} ' \
      'port={port} ' \
      'host={host} '.format(dbname='trans',
                            user=os.environ['user'],
                            password=os.environ['password'],
                            port=5439,
                            host=os.environ['host'])

conn = psycopg2.connect(dsn)
conn.set_session(autocommit=True)
cur = conn.cursor()
cur.execute('create table if not exists orders('
            'created_at timestamp,'
            'order_id varchar(64),'
            'product_name varchar(100)'
            'value float);')

products = {'casa': 500000.00, 'carro': 69900.00, 'moto': 7900.00, 'caminhao': 230000.00, 'laranja': 0.5, 'borracha': 0.3, 'iphone': 1000000.00}
idx = 0

while True:
    idx += 1
    created_at = datetime.now().strftime('%Y-%m-%d %H-%M-%S.%f')
    product_name, value = choice(list(products.items()))
    cur.execute(f'insert into orders values ({created_at}, {idx}, {product_name}, {value})')
    time.sleep(0.5)