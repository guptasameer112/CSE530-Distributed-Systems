import zmq
from pprint import pprint
import time

class Group:
    def __init__(self, group_name, group_ip, group_port):
        self.server_address = "tcp://localhost:6000"
        self.group_name = group_name
        self.group_ip = group_ip
        self.group_port = group_port
        self.users = set()
        # Set to store user IDs
        self.messages = [] 
        # List to store messages [{timestamp, sender_id, content}]

        # Registering the group with the server
        self.register_group()

        # Listening for messages
        self.listen_messages()

    def register_group(self):
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.connect(self.server_address)
        message = {
            "type": "register_group",
            "group_name": self.group_name,
            "group_ip": self.group_ip,
            "group_port": self.group_port
        }
        socket.send_json(message)
        response = socket.recv_string()
        print("Server response: ", response)

    def listen_messages(self):
        context = zmq.Context()
        socket = context.socket(zmq.REP)
        socket.bind("tcp://*:" + str(self.group_port))
        print("Listening for messages on port", self.group_port)

        while True:
            message = socket.recv_json()
            print("Received message: ", message)
            if message["type"] == "join_group":
                user_id = message["user_id"]
                self.users.add(user_id)
                socket.send_string("success")
                print("SUCCESS: User joined the group")

            elif message["type"] == "leave_group":
                user_id = message["user_id"]
                self.users.remove(user_id)
                socket.send_string("success")
                print("SUCCESS: User left the group")

            elif message["type"] == "send_message":
                sender_id = message["sender_id"]
                content = message["content"]
                timestamp = time.time()
                self.messages.append({"timestamp": timestamp, "sender_id": sender_id, "content": content})
                socket.send_string("success")
                print("SUCCESS: Message sent")

            elif message["type"] == "get_messages":
                socket.send_json(self.messages)
                print("SUCCESS: Messages sent")


if __name__ == "__main__":
    group1 = Group("group1", "localhost", 6001)



    

    