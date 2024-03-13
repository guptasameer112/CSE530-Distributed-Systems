import grpc
from .proto import raft_pb2_grpc
from .proto.raft_pb2 import ServeClientArgs

class RaftClient:
    def __init__(self, nodes_info):
        self.nodes_info = nodes_info

    def serve_client(self, request):
        # Iterate through nodes_info and try sending request to each node until success
        for node_ip, node_port in self.nodes_info:
            try:
                # Create a gRPC channel to the node
                channel = grpc.insecure_channel(f"{node_ip}:{node_port}")

                # Create a stub for the Raft service
                stub = raft_pb2_grpc.RaftStub(channel)

                # Create a request message
                serve_client_args = ServeClientArgs(Request=request)

                # Send the request to the node
                response = stub.ServeClient(serve_client_args)

                # Check if the request was successful
                if response.Success:
                    return response.Data, response.LeaderID
            except Exception as e:
                print(f"Error connecting to node {node_ip}:{node_port}: {e}")

        # If no node responds successfully, return None
        return None, None