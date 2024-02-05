import zmq

class User:
    def __init__(self, user_id, server_address):
        self.user_id = user_id
        self.server_address = server_address

    def get_group_list(self):
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.connect(self.server_address)
        socket.send_json({"action": "get_groups"})
        response = socket.recv_json()
        if response["status"] == "success":
            groups = response["groups"]
            for group in groups:
                print(f"{group['group_name']}: {group['ip_address']}:{group['port']}")
        else:
            print(f"Failed to fetch group list: {response['message']}")

    def join_group(self, group_name, group_address):
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.connect(group_address)
        socket.send_json({"action": "join_group", "group_name": group_name, "user_id": self.user_id})
        response = socket.recv_json()
        if response["status"] == "success":
            print(f"Group prints: JOIN REQUEST FROM {self.user_id} [{group_name}]")
            print(f"User prints: SUCCESS")
        else:
            print(f"Failed to join group {group_name}: {response['message']}")

    def leave_group(self, group_name, group_address):
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.connect(group_address)
        socket.send_json({"action": "leave_group", "group_name": group_name, "user_id": self.user_id})
        response = socket.recv_json()
        if response["status"] == "success":
            print(f"Group prints: LEAVE REQUEST FROM {self.user_id} [{group_name}]")
            print("User prints: SUCCESS")
        else:
            print(f"Failed to leave group {group_name}: {response['message']}")

    def get_messages(self, group_name, group_address, since_timestamp=None):
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.connect(group_address)
        request = {"action": "get_messages", "group_name": group_name, "user_id": self.user_id}
        if since_timestamp:
            request["since_timestamp"] = since_timestamp
        socket.send_json(request)
        response = socket.recv_json()
        if response["status"] == "success":
            messages = response["messages"]
            for message in messages:
                print(f"{message['timestamp']}: {message['sender_id']} - {message['content']}")
        else:
            print(f"Failed to fetch messages from group {group_name}: {response['message']}")

    def send_message(self, group_name, group_address, content):
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.connect(group_address)
        socket.send_json({"action": "send_message", "group_name": group_name, "user_id": self.user_id, "content": content})
        response = socket.recv_json()
        if response["status"] == "success":
            print(f"Group prints: MESSAGE SEND FROM {self.user_id} [{group_name}]")
            print("User prints: SUCCESS")
        else:
            print(f"Failed to send message to group {group_name}: {response['message']}")
