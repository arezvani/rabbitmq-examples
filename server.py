import pika

username = 'admin'
password = 'SAra131064!@#'
host = 'new-dbaas.abriment.com'
port = 32660

cred = pika.credentials.PlainCredentials(username, password)
params = pika.ConnectionParameters(host=host, port=port, credentials=cred)
conn = pika.BlockingConnection(parameters=params)
ch = conn.channel()

def server_with_reply(request_queue, auto_ack=True):
    def on_request_message_recieved(ch, method, properties, body):
        print(f'Recieved request: {properties.correlation_id}')
        ch.basic_publish('', routing_key=properties.reply_to, body=f'Reply to {properties.correlation_id}')

    ch.queue_declare(queue=request_queue)

    ch.basic_consume(queue=request_queue, auto_ack=auto_ack, on_message_callback=on_request_message_recieved)

    ch.start_consuming()

def alt_ex(exchange, exchange_type, queue, alt_exchange, alt_exchange_type, alt_queue, routing_key, auto_ack=True):
    ch.exchange_declare(exchange=alt_exchange, exchange_type=alt_exchange_type)
    ch.exchange_declare(exchange=exchange, exchange_type=exchange_type, arguments={'alternate-exchange':f'{alt_exchange}'})

    ch.queue_declare(alt_queue)
    ch.queue_bind(queue=alt_queue, exchange=alt_exchange)
    ch.queue_declare(queue)
    ch.queue_bind(queue=queue, exchange=exchange, routing_key=routing_key)

    def alt_callback(ch, method, properties, body):
        print(f'Alt: {body}')

    def main_callback(ch, method, properties, body):
        print(f'Main: {body}')

    ch.basic_consume(queue=alt_queue, on_message_callback=alt_callback, auto_ack=auto_ack)
    ch.basic_consume(queue=queue, on_message_callback=main_callback, auto_ack=auto_ack)

    ch.start_consuming()

def dlx_ex(exchange, exchange_type, queue, dlx_exchange, dlx_exchange_type, dlx_queue, routing_key, extra_args, auto_ack=True, dlx_routing_key=None):
    arguments = {'x-dead-letter-exchange':f'{dlx_exchange}'}
    arguments = arguments | extra_args
    ch.exchange_declare(exchange=exchange, exchange_type=exchange_type)
    ch.exchange_declare(exchange=dlx_exchange, exchange_type=dlx_exchange_type)

    ch.queue_declare(queue=queue, arguments=arguments)
    ch.queue_bind(queue=queue, exchange=exchange, routing_key=routing_key)
    ch.queue_declare(queue=dlx_queue)

    if dlx_routing_key:
        ch.queue_bind(queue=dlx_queue, exchange=dlx_exchange, routing_key=dlx_routing_key)
    else:
        ch.queue_bind(queue=dlx_queue, exchange=dlx_exchange)

    def dlx_callback(ch, method, properties, body):
        print(f'Dead Letter: {body}')

    ch.basic_consume(queue=dlx_queue, on_message_callback=dlx_callback, auto_ack=auto_ack)

    ch.start_consuming()

def nack(exchange, exchange_type, queue):
    ch.exchange_declare(exchange=exchange, exchange_type=exchange_type)
    ch.queue_declare(queue=queue)
    ch.queue_bind(queue=queue, exchange=exchange)

    def callback(ch, method, properties, body):
        if method.delivery_tag % 5 == 0:
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False, multiple=True)
        print(f'Recieved: {method.delivery_tag}')

    ch.basic_consume(queue=queue, on_message_callback=callback)

    ch.start_consuming()

def confirm(exchange, exchange_type, queue, routing_key, durable=True, exclusive=False, auto_delete=False):
    ch.exchange_declare(exchange=exchange, exchange_type=exchange_type, durable=durable, auto_delete=auto_delete)
    ch.queue_declare(queue=queue, durable=durable, exclusive=exclusive, auto_delete=auto_delete)
    ch.queue_bind(queue=queue, exchange=exchange, routing_key=routing_key)

    def callback(ch, method, properties, body):
        print(body)

    ch.basic_consume(queue=queue, on_message_callback=callback)

    ch.start_consuming()

if __name__ == '__main__':
    # server_with_reply(request_queue='request-queue')

    # alt_ex(
    #     exchange='main',
    #     exchange_type='direct',
    #     queue='mainq',
    #     alt_exchange='alt',
    #     alt_exchange_type='fanout',
    #     alt_queue='altq',
    #     routing_key='home'
    # )

    # message-ttl is set on a queue. You can bind multiple exchanges to the same queue.
    # All messages which are routed to the queue will get this message-ttl set.
    # expiration is set on a message by the sender.
    # If the message is routed to a queue which has a message-ttl the lower of both values is applied.

    # args = {
    #     'x-message-ttl': 5000,
    #     'x-queue-length': 10
    # }

    # dlx_ex(
    #     exchange='main',
    #     exchange_type='direct',
    #     queue='mainq',
    #     dlx_exchange='dlx',
    #     dlx_exchange_type='fanout',
    #     dlx_queue='dlxq',
    #     routing_key='home', 
    #     extra_args=args,
    # )

    # nack(
    #     exchange='aj',
    #     exchange_type='fanout',
    #     queue='main'
    # )

    confirm(
        exchange='main',
        exchange_type='direct',
        queue='mainq',
        routing_key='home'
    )