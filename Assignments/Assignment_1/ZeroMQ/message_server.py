import zmq

class Server:
    def __init__(self):
        self.groups = {}  # Dictionary to store group information {group_name: (IP, Port)}
        
    def start(self):
        context = zmq.Context()
        socket = context.socket(zmq.REP)
        socket.bind("tcp://*:6000")
        print("Server started...")
        
        while True:
            message = socket.recv_json()
            if message["action"] == "register_group":
                group_name = message["group_name"]
                ip_address = message["ip_address"]
                port = message["port"]
                print(f"JOIN REQUEST FROM {ip_address} [{group_name}]")
                if group_name not in self.groups:
                    self.groups[group_name] = (ip_address, port)
                    print("Server prints: SUCCESS")
                    socket.send_json({"status": "success"})
                else:
                    print(f"Group server '{group_name}' already registered")
                    socket.send_json({"status": "error", "message": "Group server already registered"})
            elif message["action"] == "get_groups":
                print("Sending group list...")
                groups_info = [{"group_name": group, "ip_address": ip_address, "port": port} 
                               for group, (ip_address, port) in self.groups.items()]
                socket.send_json({"status": "success", "groups": groups_info})
            else:
                socket.send_json({"status": "error", "message": "Invalid action"})

if __name__ == "__main__":
    server = Server()
    server.start()
