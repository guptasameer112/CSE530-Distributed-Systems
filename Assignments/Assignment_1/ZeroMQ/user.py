import zmq
from pprint import pprint
import uuid


class User:
    def __init__(self, user_ip, user_id):
        self.user_ip = user_ip
        self.user_id = user_id

    def get_group_list(self):
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.connect("tcp://34.41.214.53:6000")
        message = {
            "type": "group_list",
            "user_ip": self.user_ip,
            "user_id": self.user_id
        }
        socket.send_json(message)
        group_list = socket.recv_json()
        print("Available groups: ")
        pprint(group_list)
        return group_list

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

    def get_messages(self, group_name, group_ip, group_port, time):
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.connect("tcp://" + group_ip + ":" + str(group_port))
        message = {
            "type": "get_messages",
            "user_id": self.user_id,
            "date": time
        }
        socket.send_json(message)
        messages = socket.recv_json()
        print("Messages: ")
        pprint(messages)

if __name__ == "__main__":
    users = []
    groups = []

    admin_user = User("localhost", "u_00")
    groups = admin_user.get_group_list()

    while True:
        print("\n1. Create User")
        print("2. Get Group List")
        print("3. Join Group")
        print("4. Leave Group")
        print("5. Send Message")
        print("6. Get Messages")
        print("7. Exit")

        choice = input("Enter your choice: ")

        if choice == "1":
            user = User("localhost", str(uuid.uuid4()))
            users.append(user)
            print("User ID:", user.user_id)  # Print the generated user ID
            print("User created successfully.")
        elif choice == "2":
            user_id = input("Enter user ID: ")
            for user in users:
                if user.user_id == user_id:
                    user.get_group_list()
                    break
        elif choice == "3":
            user_id = input("Enter user ID: ")
            for user in users:
                if user.user_id == user_id:
                    group_name = input("Enter group name: ")
                    group_ip = groups[group_name][0]
                    group_port = groups[group_name][1]
                    user.join_group(group_name, group_ip, group_port)
                    break
        elif choice == "4":
            user_id = input("Enter user ID: ")
            for user in users:
                if user.user_id == user_id:
                    group_name = input("Enter group name: ")
                    group_ip = groups[group_name][0]
                    group_port = groups[group_name][1]
                    user.leave_group(group_name, group_ip, group_port)
                    break
        elif choice == "5":
            user_id = input("Enter user ID: ")
            for user in users:
                if user.user_id == user_id:
                    group_name = input("Enter group name: ")
                    group_ip = groups[group_name][0]
                    group_port = groups[group_name][1]
                    content = input("Enter message content: ")
                    user.send_message(group_name, group_ip, group_port, content)
                    break
        elif choice == "6":
            user_id = input("Enter user ID: ")
            for user in users:
                if user.user_id == user_id:
                    group_name = input("Enter group name: ")
                    group_ip = groups[group_name][0]
                    group_port = groups[group_name][1]
                    time = input("Enter time (HH:MM:SS): ")
                    user.get_messages(group_name, group_ip, group_port, time)
                    break
        elif choice == "7":
            print("Exiting...")
            break
        else:
            print("Invalid choice. Please try again.")