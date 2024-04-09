import os
import sys
import grpc
from concurrent import futures
import numpy as np
import grpc
import master_reducer_pb2
import master_reducer_pb2_grpc
import reducer_mapper_pb2
import reducer_mapper_pb2_grpc

# Calculate new centroid based on points
def calculate_new_centroid(points):
    # Implement centroid calculation method here
    return np.mean(points, axis=0)

# Implement the Shuffle and Sort function
def shuffle_and_sort(mapped_results):
    sorted_results = {}
    for partition in mapped_results:
        for centroid_id, point in partition:
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
class ReducerMapperServicer(master_reducer_pb2_grpc.ReducerMapperServicer):
    def __init__(self, reducer_id):
        self.reducer_id = reducer_id
        self.sorted_results = {}

    def Reduce(self, request, context):
        # Extract sorted results
        partitions = request.partitions

        # Call Shuffle and Sort function
        sorted_results = shuffle_and_sort(partitions)

        # Call Reduce function
        updated_centroids = reduce_function(sorted_results)

        # Write updated centroids to file
        output_file = f'reducer_output.txt'
        with open(output_file, 'w') as f:
            for centroid_id, centroid in updated_centroids.items():
                f.write(f"{centroid_id} {centroid}\n")
        
        # Send updated centroids to master
        for centroid_id, centroid in updated_centroids.items():
            yield master_reducer_pb2.ReduceResponse(centroid_id=centroid_id, values=centroid)

def serve(reducer_id, port):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    master_reducer_pb2_grpc.add_ReducerMapperServicer_to_server(ReducerMapperServicer(reducer_id), server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    print(f"Reducer server started on port {port}")
    server.wait_for_termination()

if __name__ == '__main__':
    reducer_id = int(sys.argv[1])
    port = int(sys.argv[2])
    serve(reducer_id, port)