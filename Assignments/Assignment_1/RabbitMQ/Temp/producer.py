import pika
from pika.exchange_type import ExchangeType

connection_params = pika.ConnectionParameters('localhost')

connection = pika.BlockingConnection(connection_params)

channel = connection.channel()

channel.exchange_declare(exchange='pubsub', exchange_type=ExchangeType.fanout)

message = "I want to broadcast this message to all the subscribers"

channel.basic_publish(exchange='pubsub', routing_key='', body=message)

print(f"Sent Message : {message}")

connection.close()