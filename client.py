import pika
import uuid
import time

username = 'admin'
password = 'SAra131064!@#'
host = 'new-dbaas.abriment.com'
port = 32660

cred = pika.credentials.PlainCredentials(username, password)
params = pika.ConnectionParameters(host=host, port=port, credentials=cred)
conn = pika.BlockingConnection(parameters=params)
ch = conn.channel()

def client_with_reply(request_queue, body, exclusive=True):
    reply_queue = ch.queue_declare(queue='', exclusive=exclusive)

    def on_reply_message_recieved(ch, method, properties, body):
        print(f'Reply recieved: {body}')

    ch.basic_consume(
        queue=reply_queue.method.queue,
        on_message_callback=on_reply_message_recieved,
        auto_ack=True
    )  

    ch.queue_declare(queue=request_queue)

    cor_id = str(uuid.uuid4())
    print(f'Sending request: {cor_id}')

    reply_props = pika.BasicProperties(
        reply_to=reply_queue.method.queue,
        correlation_id=cor_id
    )
    ch.basic_publish('', routing_key=request_queue, properties=reply_props, body=body)

    ch.start_consuming()

def ex_to_ex(first_exchange, first_exchange_type, second_exchange, second_exchange_type, body, durable=True):
    ch.exchange_declare(exchange=first_exchange, exchange_type=first_exchange_type, durable=durable)
    ch.exchange_declare(exchange=second_exchange, exchange_type=second_exchange_type, durable=durable)
    ch.exchange_bind(second_exchange, first_exchange)

    ch.basic_publish(exchange=first_exchange, routing_key='', body=body)

def alt_ex(exchange, exchange_type, alt_exchange, alt_exchange_type, routing_key, body):
    ch.exchange_declare(exchange=alt_exchange, exchange_type=alt_exchange_type)
    ch.exchange_declare(exchange=exchange, exchange_type=exchange_type, arguments={'alternate-exchange':f'{alt_exchange}'})

    ch.basic_publish(exchange=exchange, routing_key=routing_key, body=body)

def nack(exchange, exchange_type, routing_key, body):
    ch.exchange_declare(exchange=exchange, exchange_type=exchange_type)

    while True:
        ch.basic_publish(exchange=exchange, routing_key=routing_key, body=body)
        print('Sent ...!')
        input('Press enter to continue ...!')

def confirm(exchange, exchange_type, body, routing_key, durable=True, exclusive=False, auto_delete=False):
    ch.exchange_declare(exchange=exchange, exchange_type=exchange_type, durable=durable, auto_delete=auto_delete)
    
    ch.confirm_delivery()

    props = pika.BasicProperties(
        content_type='text/plain',
        delivery_mode=2
    )

    for i in range(20):
        try:
            ch.basic_publish(exchange=exchange, routing_key=routing_key, properties=props, body=body)
            print(f'Message {i} confirmed ...!')

        except Exception as e:
            print(f'Exception: {type(e).__name__}')

        time.sleep(2)

if __name__ == '__main__':
    # client_with_reply(request_queue='request-queue', body='Can I request a reply?')

    # ex_to_ex(
    #     first_exchange='a',
    #     first_exchange_type='direct',
    #     second_exchange='z',
    #     second_exchange_type='fanout',
    #     body='Hello world ...!'
    # )

    # alt_ex(
    #     exchange='main',
    #     exchange_type='direct',
    #     alt_exchange='alt',
    #     alt_exchange_type='fanout',
    #     routing_key='bad',
    #     body='Hello world ...!'
    # )

    # nack(
    #     exchange='aj',
    #     exchange_type='fanout',
    #     routing_key='home',
    #     body='Hello world ...!'
    # )

    confirm(
        exchange='main',
        exchange_type='direct',
        routing_key='home',
        body='Hello world ...!'
    )

    conn.close()


