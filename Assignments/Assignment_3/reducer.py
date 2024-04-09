import os
import sys
import grpc
from concurrent import futures
import numpy as np
# import k_means_pb2
# import k_means_pb2_grpc
import grpc
import reducer_mapper_pb2
import reducer_mapper_pb2_grpc

# Calculate new centroid based on points
def calculate_new_centroid(points):
    # Implement centroid calculation method here
    return np.mean(points, axis=0)

# Implement the Shuffle and Sort function
def shuffle_and_sort(partitions):
    sorted_results = {}
    for partition in partitions:
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
class ReducerServicer(k_means_pb2_grpc.ReducerServicer):
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
        
        return k_means_pb2.ReduceResponse(status="Success")

def serve(port):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    k_means_pb2_grpc.add_ReducerServicer_to_server(ReducerServicer(), server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    port = int(sys.argv[1])
    serve(port)