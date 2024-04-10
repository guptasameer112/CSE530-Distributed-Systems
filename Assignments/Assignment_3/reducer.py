import os
import sys
import grpc
from concurrent import futures
import numpy as np
import grpc
import master_mapper_pb2
import master_mapper_pb2_grpc

# Calculate new centroid based on points
def calculate_new_centroid(points):
    # Implement centroid calculation method here
    return np.mean(points, axis=0)

# Implement the Shuffle and Sort function
def shuffle_and_sort(mapped_results):
    sorted_results = {}
    for centroid_id, point in mapped_results:
        if centroid_id not in sorted_results:
            sorted_results[centroid_id] = []
        sorted_results[centroid_id].append(point)
    return sorted_results

# Implement the Reduce function
def reduce_function(sorted_results):
    updated_centroids = {}
    for centroid_id, points in sorted_results.items():
        updated_centroid = calculate_new_centroid(points)
        updated_centroids[centroid_id] = updated_centroid
    return updated_centroids

# Reducer service
class ReducerServicer(master_mapper_pb2_grpc.ReducerServicer):
    def __init__(self, reducer_id):
        self.reducer_id = reducer_id
        self.sorted_results = {}

    def StartReduce(self, request, context):

        data_points = []
        # send rpc to all mappers to get their output
        for mapper_id in request.mapper_ids:
            channel = grpc.insecure_channel(f'localhost:{50051 + mapper_id}')
            stub = master_mapper_pb2_grpc.MapperStub(channel)
            print(f"Reducer {self.reducer_id} requesting data from Mapper {mapper_id}")
            mapper_request = master_mapper_pb2.ReturnDataRequest(reducer_id=self.reducer_id)
            response = stub.ReturnData(mapper_request)
            print(f"Reducer {self.reducer_id} received data from Mapper {mapper_id}")
            data_points.extend([(data_point.centroid_id, [data_point.x, data_point.y]) for data_point in response.data_points])
            print(f"Data points: {data_points}")

        # Call Shuffle and Sort function
        sorted_results = shuffle_and_sort(data_points)
        print(f"Sorted results: {sorted_results}")

        # # Call Reduce function
        updated_centroids = reduce_function(sorted_results)
        print(f"Updated centroids: {updated_centroids}")

        # Write updated centroids to file
        output_file = f'Data/Reducer/R{self.reducer_id}/reducer_output.txt'
        with open(output_file, 'w') as f:
            for centroid_id, centroid in updated_centroids.items():
                f.write(f"{centroid_id} {centroid[0]} {centroid[1]}\n")
        

        return master_mapper_pb2.StartReduceResponse(success=True)
        # # Send updated centroids to master
        # for centroid_id, centroid in updated_centroids.items():
        #     yield master_mapper_pb2.ReduceResponse(centroid_id=centroid_id, values=centroid)

def serve(reducer_id, port):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    master_mapper_pb2_grpc.add_ReducerServicer_to_server(ReducerServicer(reducer_id), server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    print(f"Reducer server started on port {port}")
    server.wait_for_termination()

if __name__ == '__main__':
    reducer_id = int(sys.argv[1])
    port = int(sys.argv[2])
    serve(reducer_id, port)