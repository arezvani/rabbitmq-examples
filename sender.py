import pika
from pika.exchange_type import ExchangeType
import time
import json

username = '###########'
password = '***********'
host = '###########'
port = ****

cred = pika.credentials.PlainCredentials(username, password)
params = pika.ConnectionParameters(host=host, port=port, credentials=cred)
conn = pika.BlockingConnection(parameters=params)

ch = conn.channel()

# Direct Exchange
def direct_exchange(message, queue=None, exchange=None, routing_key=None, properties=None):
    # Method 1
    if queue:
        ch.queue_declare(queue=queue)
        ch.basic_publish(
            exchange='',
            routing_key=queue,
            body=json.dumps(message),
            properties=properties
        )

    #  Method 2
    else:
        ch.exchange_declare(exchange=exchange, exchange_type='direct')
        ch.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=json.dumps(message),
            properties=properties
        )



# Fanout Exchange
def fanout_exchange(exchange, message, durable=True, properties=None):
    ch.exchange_declare(
        exchange=exchange,
        exchange_type='fanout', # ExchangeType.fanout
        durable=durable
    )
    ch.basic_publish(
        exchange=exchange,
        routing_key='',
        body=json.dumps(message),
        properties=properties
    )

# Topic Exchange
# Message should be dictionary
def topic_exchange(exchange, message, durable=True, properties=None):
    ch.exchange_declare(
        exchange=exchange,
        exchange_type='topic', # ExchangeType.topic
        durable=durable
    )

    for k,v in message.items():
        ch.basic_publish(
            exchange=exchange,
            routing_key=k,
            body=v,
            properties=properties
        )

def headers_exchange(exchange, message, durable=True, properties=None):
    ch.exchange_declare(
        exchange=exchange,
        exchange_type='headers', # ExchangeType.headers
        durable=durable
    )

    ch.basic_publish(
        exchange=exchange,
        routing_key='',
        body=json.dumps(message),
        properties=properties
    )   


if __name__ == '__main__':
    props = pika.BasicProperties(
        content_type='application/json',
        content_encoding='gzip',
        delivery_mode=2,
        # type='direct.one',
        # user_id='1',
        app_id='2',
        headers={
            'reciever': 'ali'
        },
        timestamp=int(time.time()),
        expiration='30000'
    )

    msg = {
        'error.warning.important': 'This is an important message'
    }

    # direct_exchange(queue='one', message=msg, properties=props)
    # direct_exchange(exchange='main', routing_key='home', message=msg)
    # fanout_exchange(exchange='fanout', message=msg, properties=props)
    # topic_exchange(exchange='topic', message=msg, properties=props)
    # headers_exchange(exchange='headers', message=msg, properties=props)
    print('Message sent ...')
    conn.close() 
