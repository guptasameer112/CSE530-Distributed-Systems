import grpc
import raft_pb2
import raft_pb2_grpc

class RaftClient:
    def __init__(self, address):
        self.channel = grpc.insecure_channel(address)
        self.stub = raft_pb2_grpc.RaftStub(self.channel)
        self.node_ips = ['localhost:50051', 'localhost:50052', 'localhost:50053', 'localhost:50054', 'localhost:50055']
        self.leader_id = 0

    def set_leader(self):
        self.channel = grpc.insecure_channel(self.node_ips[self.leader_id])
        self.stub = raft_pb2_grpc.RaftStub(self.channel)

    def send_command(self, command):
        request = raft_pb2.ServeClientArgs(Request=command)
        response = self.stub.ServeClient(request)

        if (response.Success) and (command.split()[0] == 'SET'):
            print('Entry Updated')
        elif (response.Success) and (command.split()[0] == 'GET'):
            print(f'Value: {response.Data}')
        else:
            print(f'Failed, Resend to Leader: {response.LeaderID}')
            self.leader_id = response.LeaderID
            self.set_leader()
            self.send_command(command)


def main():
    raft_client = RaftClient('localhost:50051')
    # raft_client.send_command('SET 5')
    # menu driven code for SET K V and GET K commands
    while True:
        print("\nMenu:")
        print("1. SET K V")
        print("2. GET K")
        print("3. Exit")
        choice = input("Enter choice: ")
        if choice == '1':
            key = input("Enter key: ")
            value = input("Enter value: ")
            raft_client.send_command(f'SET {key} {value}')
        elif choice == '2':
            key = input("Enter key: ")
            raft_client.send_command(f'GET {key}')
        elif choice == '3':
            break
        else:
            print("Invalid choice")



