import zmq
from pprint import pprint

class User:
    def __init__(self, user_ip, user_id):
        self.user_ip = user_ip
        self.user_id = user_id

    def get_group_list(self):
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.connect("tcp://localhost:6000")
        message = {
            "type": "group_list",
            "user_ip": self.user_ip,
            "user_id": self.user_id
        }
        socket.send_json(message)
        group_list = socket.recv_json()
        print("Available groups: ")
        pprint(group_list)

    def join_group(self, group_name, group_ip, group_port):
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.connect("tcp://" + group_ip + ":" + str(group_port))
        message = {
            "type": "join_group",
            "user_id": self.user_id
        }
        socket.send_json(message)
        response = socket.recv_string()
        print("Server response: ", response)

    def leave_group(self, group_name, group_ip, group_port):
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.connect("tcp://" + group_ip + ":" + str(group_port))
        message = {
            "type": "leave_group",
            "user_id": self.user_id
        }
        socket.send_json(message)
        response = socket.recv_string()
        print("Server response: ", response)

    def send_message(self, group_name, group_ip, group_port, content):
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.connect("tcp://" + group_ip + ":" + str(group_port))
        message = {
            "type": "send_message",
            "sender_id": self.user_id,
            "content": content
        }
        socket.send_json(message)
        response = socket.recv_string()
        print("Server response: ", response)

    def get_messages(self, group_name, group_ip, group_port):
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.connect("tcp://" + group_ip + ":" + str(group_port))
        message = {
            "type": "get_messages",
            "user_id": self.user_id,
            "date": "00:00:00"
        }
        socket.send_json(message)
        messages = socket.recv_json()
        print("Messages: ")
        pprint(messages)

if __name__ == "__main__":
    user1 = User("localhost", "001")
    user2 = User("localhost", "002")

    user1.get_group_list()
    user2.get_group_list()

    user1.join_group("group1", "localhost", 6001)
    user2.join_group("group1", "localhost", 6001)

    # user1.send_message("group1", "localhost", 6001, "Hello, everyone from user1!")
    # user2.send_message("group1", "localhost", 6001, "Hello, everyone from user2!")

    # user1.get_messages("group1", "localhost", 6001)
    user2.get_messages("group1", "localhost", 6001)

    user1.leave_group("group1", "localhost", 6001)
    user2.leave_group("group1", "localhost", 6001)