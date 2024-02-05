import pika
from pika.exchange_type import ExchangeType

def callback(ch, method, properties, body):
    print("First Consumer Received %r" % body)

connection_params = pika.ConnectionParameters('localhost')

connection = pika.BlockingConnection(connection_params)

channel = connection.channel()

channel.exchange_declare(exchange='pubsub', exchange_type=ExchangeType.fanout)

queue = channel.queue_declare(queue='', exclusive=True)

channel.queue_bind(exchange='pubsub', queue=queue.method.queue)

channel.basic_consume(queue=queue.method.queue, on_message_callback=callback, auto_ack=True)

print("First Consumer Waiting for Messages")

channel.start_consuming()