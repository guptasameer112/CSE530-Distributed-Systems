import sys
import pika
from pika.exchange_type import ExchangeType

def updateSubscription(user_name, subs_string, youtuber_name, channel, connection):
    message = user_name + " "
    if subs_string[0] == True:
        message += "s" + " " + youtuber_name
    else:
        message += "u" + " " + youtuber_name

    channel.basic_publish(exchange='routing', routing_key='usersend', body=message)
    print(f"Sent Message : {message}")

def receiveNotification(user_name, channel):

    def callback(ch, method, properties, body):
        
        body = body.decode('utf-8')
        body = body.split()
        youtuber = body[0]
        video_name = ""
        for i in range(1, len(body)):
            video_name += body[i] + " "

        print(f"New Video Published by {youtuber} : {video_name}")

    queue = channel.queue_declare(queue='', exclusive=True)
    channel.queue_bind(exchange='routing', queue=queue.method.queue, routing_key=user_name)
    channel.basic_consume(queue=queue.method.queue, on_message_callback=callback, auto_ack=True)
    channel.start_consuming()

def add_user(user_name, channel, connection):

    channel.basic_publish(exchange='routing', routing_key='usersend', body=user_name)
    print(f"Sent Message : {user_name}")



connection_params = pika.ConnectionParameters('localhost')
connection = pika.BlockingConnection(connection_params)
channel = connection.channel()
channel.exchange_declare(exchange='routing', exchange_type=ExchangeType.direct)

n = len(sys.argv)
user_name = sys.argv[1]

if n == 2:
    add_user(user_name, channel, connection)

elif n == 4:
    subs_string = [True if sys.argv[2] == 's' or sys.argv[2] == 'S' else False]
    youtuber_name = sys.argv[3]
    updateSubscription(user_name, subs_string, youtuber_name, channel, connection)

receiveNotification(user_name, channel)







