import pika
import time

username = '###########'
password = '***********'
host = '###########'
port = ****

cred = pika.credentials.PlainCredentials(username, password)
params = pika.ConnectionParameters(host=host, port=port, credentials=cred)
conn = pika.BlockingConnection(parameters=params)

ch = conn.channel()

def callback(channel, method, properties, body):
    body = body.decode('ascii')
    print(f'Recieved {body} with properties: {properties}')
    time.sleep(5)
    channel.basic_ack(delivery_tag=method.delivery_tag)

    # Write to file
    with open('error_logs.log', 'a') as el:
        el.write(f'{str(body)} \n')

# Direct Exchange
def direct_exchange(queue, prefetch_count=0, auto_ack=False):
    ch.queue_declare(queue=queue)
    ch.basic_qos(prefetch_count=prefetch_count)
    ch.basic_consume(
        queue=queue,
        on_message_callback=callback,
        auto_ack=auto_ack
    )

# Fanout Exchange
def fanout_exchange(exchange, durable=True, exclusive=True, prefetch_count=0, auto_ack=False):
    ch.exchange_declare(exchange=exchange, exchange_type='fanout', durable=durable)
    queue_res = ch.queue_declare(queue='', exclusive=exclusive)
    ch.queue_bind(exchange=exchange, queue=queue_res.method.queue)
    ch.basic_qos(prefetch_count=prefetch_count)
    ch.basic_consume(
        queue=queue_res.method.queue,
        on_message_callback=callback,
        auto_ack=auto_ack
    )    

def topic_exchange(exchange, routing_key, durable=True, exclusive=True, prefetch_count=0, auto_ack=False):
    ch.exchange_declare(exchange=exchange, exchange_type='topic', durable=durable)
    queue_res = ch.queue_declare(queue='', exclusive=exclusive)
    ch.queue_bind(exchange=exchange, queue=queue_res.method.queue, routing_key=routing_key)
    ch.basic_consume(
        queue=queue_res.method.queue,
        on_message_callback=callback,
        auto_ack=auto_ack
    )      

def headers_exchange(exchange, queue, arguments, durable=True, exclusive=True, prefetch_count=0, auto_ack=False):
    ch.exchange_declare(exchange=exchange, exchange_type='headers', durable=durable)
    queue_res = ch.queue_declare(queue=queue, exclusive=exclusive)
    ch.queue_bind(exchange=exchange, queue=queue_res.method.queue, arguments=arguments)
    ch.basic_consume(
        queue=queue,
        on_message_callback=callback,
        auto_ack=auto_ack
    )  


if __name__ == '__main__':
    print('Waiting for message, to exit press ^c')

    # direct_exchange(queue='one')
    # fanout_exchange(exchange='fanout')
    # topic_exchange(exchange='topic', routing_key='#.important') # .*.*.important

    bind_args = {'x-match': 'all', 'reciever': 'ali'}
    headers_exchange(exchange='headers', queue='ali', arguments=bind_args)

    ch.start_consuming()
