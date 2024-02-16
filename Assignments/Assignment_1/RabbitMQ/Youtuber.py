import sys 
import pika
from pika.exchange_type import ExchangeType

def publishVideo(youtuber_name, video_name):
    
    # connection_params = pika.ConnectionParameters('localhost')
    host = '34.172.131.86'
    connection_params = pika.ConnectionParameters(host=host, port=5672, virtual_host='/', credentials=pika.PlainCredentials('abhay', 'abhay'), )
    connection = pika.BlockingConnection(connection_params)
    channel = connection.channel()

    channel.exchange_declare(exchange='routing', exchange_type=ExchangeType.direct)

    message = youtuber_name + " " + video_name

    channel.basic_publish(exchange='routing', routing_key='youtuber', body=message)

    print(f"SUCESS! Sent Message : {message}")

    connection.close()

    return


n = len(sys.argv)
youtuber_name = sys.argv[1]
video_name = ''
for i in range(2, n):
    video_name += sys.argv[i] + ' '

publishVideo(youtuber_name, video_name)


