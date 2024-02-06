# <----------- Library Imports ------------>
import zmq


# <----------- Server Class ------------>
class Server:
    def __init__(self):
        '''
            Constructor.
            groups: Dictionary to store group_name: (group_ip, group_port).
        '''
        self.groups = {} 

    def listen(self):
        '''
            Listen for incoming requests.
            Listening port: 6000.
        '''
        context = zmq.Context()
        socket = context.socket(zmq.REP)
        socket.bind("tcp://*:6000")
        print("Server has started listening. \nWaiting for incoming requests..\n\n")
        
        message_types = ["register_group", "group_list"]
        while True:
            message = socket.recv_json()
            
            # Debug
            print("Server received a message.")

            # Error Checking
            if message["type"] not in message_types:
                print("Invalid Operation.")
                socket.send_string("Failure: Invalid Operation Demanded.")
                quit()
            else:
                if message["type"] == "register_group":
                    '''
                        Register the group.
                        group_name: Name of the group.
                        group_ip: IP of the group.
                        group_port: Port of the group.
                    '''
                    group_name = message["group_name"]
                    group_ip = message["group_ip"]
                    group_port = message["group_port"]
                    print("JOIN REQUEST FROM", group_ip, ":", group_port)

                    if group_name not in self.groups:
                        self.groups[group_name] = (group_ip, group_port)
                        print("SUCCESS: Group registered successfully")

                        # Debug
                        socket.send_string("SUCCESS")

                    else:
                        print("FAILURE: Group already exists")

                        # Debug
                        socket.send_string("FAILURE")

                elif message["type"] == "group_list":
                    '''
                        Send the list of groups to user.
                    '''
                    user_ip = message["user_ip"]
                    user_id = message["user_id"]
                    print("GROUP LIST REQUEST FROM", user_ip, ":", user_id)
                    group_list = {group: (ip, port) for group, (ip, port) in self.groups.items()}
                    socket.send_json(group_list)

                    # Debug
                    print("Group list sent to user.")

if __name__ == "__main__":
    server = Server()
    server.listen()