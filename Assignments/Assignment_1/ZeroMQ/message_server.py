import zmq

class Server:
    def __init__(self):
        self.groups = {}

    def start(self):
        context = zmq.Context()
        socket = context.socket(zmq.REP)
        socket.bind("tcp://*:6000") # Listen on port 6000
        print("Server started..")

        while True:
            message = socket.recv_json()
            print("Received request: ", message)

            if message["type"] == "register_group":
                # Register the group
                group_name = message["group_name"]
                group_ip = message["group_ip"]
                group_port = message["group_port"]
                print("JOIN REQUEST FROM", group_ip, ":", group_port)
                if group_name not in self.groups:
                    self.groups[group_name] = (group_ip, group_port)
                    socket.send_string("success")
                    print("SUCCESS: Group registered successfully")
                else:
                    socket.send_string("failure")
                    print("FAILURE: Group already exists")

            elif message["type"] == "group_list":
                # Send the list of groups to user
                user_ip = message["user_ip"]
                user_id = message["user_id"]
                print("GROUP LIST REQUEST FROM", user_ip, ":", user_id)
                group_list = {group: (ip, port) for group, (ip, port) in self.groups.items()}
                socket.send_json(group_list)

if __name__ == "__main__":
    server = Server()
    server.start()