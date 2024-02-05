import pika
from pika.exchange_type import ExchangeType

videos ={}
subscribers = {}

def consume_user_request(channel):
    
    def callback(ch, method, properties, body):
        user_query = body.split()

        user_name = user_query[0]
        user_name = user_name.decode('utf-8')
        print(user_name, "has logged in")

        if(len(user_query) > 1):
            subs_char = user_query[1]
            youtuber_name = user_query[2]

            subs_char = subs_char.decode('utf-8')
            youtuber_name = youtuber_name.decode('utf-8')

            if subs_char == 's' or subs_char == 'S':
                if youtuber_name in subscribers:
                    subscribers[youtuber_name].append(user_name)
                else:
                    if(youtuber_name in videos):
                        subscribers[youtuber_name] = [user_name]

            elif subs_char == 'u' or subs_char == 'U':
                if youtuber_name in subscribers:
                    subscribers[youtuber_name].remove(user_name)
                else:
                    print("No such Youtuber")

            print(subscribers)
            print(videos)

    queue = channel.queue_declare(queue='', exclusive=True)
    channel.queue_bind(exchange='routing', queue=queue.method.queue, routing_key='usersend')
    channel.basic_consume(queue=queue.method.queue, on_message_callback=callback, auto_ack=True)

    print("Waiting for Users")

def consume_youtuber_request(channel, connection):

    def callback(ch, method, properties, body):
        body = body.decode('utf-8')
        body = body.split()
        youtuber = body[0]
        video_name = ""
        for i in range(1, len(body)):
            video_name += body[i] + " "

        print(f"New Video Published by {youtuber} : {video_name}")
        videos[youtuber] = [video_name]

        if youtuber in subscribers:
            for user in subscribers[youtuber]:
                notify_user(channel, user, youtuber, video_name, connection)

    queue = channel.queue_declare(queue='', exclusive=True)
    channel.queue_bind(exchange='routing', queue=queue.method.queue, routing_key='youtuber')
    channel.basic_consume(queue=queue.method.queue, on_message_callback=callback, auto_ack=True)

    print("Waiting for Youtbers")

def notify_user(channel, user, youtuber, video, connection):
    message = youtuber + " " + video
    channel.basic_publish(exchange='routing', routing_key=user, body=message)


print("Youtube Server Started")

connection_params = pika.ConnectionParameters('localhost')
connection = pika.BlockingConnection(connection_params)
channel = connection.channel()
channel.exchange_declare(exchange='routing', exchange_type=ExchangeType.direct)

consume_youtuber_request(channel, connection)
consume_user_request(channel)

channel.start_consuming()