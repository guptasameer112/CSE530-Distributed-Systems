import zmq

class Group:
    def __init__(self, name, server_address, ip_address, port):
        self.name = name
        self.users = set()  # Set to store user IDs
        self.messages = []  # List to store messages [{timestamp, sender_id, content}]
        self.server_address = server_address
        self.ip_address = ip_address
        self.port = port
        
    def register_group(self):
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.connect(self.server_address)
        socket.send_json({"action": "register_group", "group_name": self.name, "ip_address": self.ip_address, "port": self.port})
        response = socket.recv_json()
        if response["status"] == "success":
            print(f"Group '{self.name}' registered successfully")
        else:
            print(f"Failed to register group '{self.name}': {response['message']}")

    def join_group(self, user_id):
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        response = socket.recv_json()  
        if user_id not in self.users:
            self.users.add(user_id)
            print(f"User {user_id} joined group {self.name}")
        else:
            print(f"User {user_id} is already a member of group {self.name}")

    def leave_group(self, user_id):
        if user_id in self.users:
            self.users.remove(user_id)
            print(f"User {user_id} left group {self.name}")
        else:
            print(f"User {user_id} is not a member of group {self.name}")

# Usage example:
# group = Group("group1", "tcp://localhost:6000", "localhost", 6001)
# group.register_group()
# group.join_group("user123")
# group.leave_group("user123")
