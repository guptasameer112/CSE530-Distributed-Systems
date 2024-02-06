import zmq
from datetime import datetime

class Group:
    def __init__(self, group_name, group_ip, group_port):
        '''
            Constructor.
            group_name: Name of the group.
            group_ip: IP of the group.
            group_port: Port of the group.
            users: Set to store the user_ids of the users in the group.
            messages: List to store the messages sent in the group.
        '''
        self.server_address = "tcp://localhost:6000"
        self.group_name = group_name
        self.group_ip = group_ip
        self.group_port = group_port
        self.users = set()
        self.messages = [] 

        self.register_group()
        self.listen_messages()

    def register_group(self):
        '''
            Register the group with the server.
        '''

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

        # Debug
        response = socket.recv_string()
        print("Server response for register group: ", response)

    def listen_messages(self):
        '''
            Listen for incoming messages.
        '''
        context = zmq.Context()
        socket = context.socket(zmq.REP)
        socket.bind("tcp://*:" + str(self.group_port))
        print("Listening for messages on port", self.group_port)

        while True:
            message = socket.recv_json()
            print("Received message: ", message)
            if message["type"] == "join_group":
                user_id = message["user_id"]
                print("JOIN REQUEST FROM", user_id)
                self.users.add(user_id)
                socket.send_string("SUCCESS")

            elif message["type"] == "leave_group":
                user_id = message["user_id"]
                print("LEAVE REQUEST FROM", user_id)
                self.users.remove(user_id)
                socket.send_string("SUCCESS")

            elif message["type"] == "send_message":
                sender_id = message["sender_id"]
                content = message["content"]
                if sender_id not in self.users:
                    socket.send_string("FAILURE")
                    print("FAILURE: User not in group")
                    continue

                time = datetime.now().strftime("%H:%M:%S")
                self.messages.append({"timestamp": time, "sender_id": sender_id, "content": content})
                socket.send_string("SUCCESS")
                print("SUCCESS: Message sent")

            elif message["type"] == "get_messages":
                user_id = message["user_id"]
                date = message["date"]
                if user_id not in self.users:
                    socket.send_string("FAILURE")
                    print("FAILURE: User not in group")
                    continue

                print("MESSAGE REQUEST FROM", user_id)
                if date == "":
                    messages = self.messages
                else:
                    date_obj = datetime.strptime(date, "%H:%M:%S")
                    messages = [message for message in self.messages if datetime.strptime(message["timestamp"], "%H:%M:%S") > date_obj]
                    print(messages)
                socket.send_json(messages)

if __name__ == "__main__":
    group1 = Group("group1", "localhost", 6001)