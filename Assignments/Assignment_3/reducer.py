import os
import sys
import grpc
from concurrent import futures
import numpy as np
import grpc
import master_mapper_reducer_pb2
import master_mapper_reducer_pb2_grpc
import random

dump_file = 'test_outputs/dump.txt'
dump_file = open(dump_file, 'w')

def calculate_new_centroid(points):
    '''
    Calculate the new centroid based on the points assigned to it

    Input:
    points: [[0.4, 7.2], [-1.5, 7.3], [7.3, 2.3], [8.9, 0.2], [11.5, -1.9]]

    Output:
    [5.32, 3.02]
    '''
    return np.mean(points, axis=0)

def shuffle_and_sort(data_point_to_centroid_map):
    '''
    Sort the mapped results based on the centroid_id

    Input:
    mapped_results: [(0, [1.0, 2.0]), (1, [3.0, 4.0]), (0, [5.0, 6.0]), (1, [7.0, 8.0]), ...]

    Output:
    sorted_results: {0: [[1.0, 2.0], [5.0, 6.0]], 1: [[3.0, 4.0], [7.0, 8.0]]}
    '''
    sorted_data_point_to_centroid_map = {}
    for centroid_id, point in data_point_to_centroid_map:
        if centroid_id not in sorted_data_point_to_centroid_map:
            sorted_data_point_to_centroid_map[centroid_id] = []
        sorted_data_point_to_centroid_map[centroid_id].append(point)
    return sorted_data_point_to_centroid_map

def reduce_function(reducer_id, sorted_results, is_retry):
    '''
    This function reads the sorted results and reduces them to update the centroids, also store the updated centroids in a file
    
    Input:
    sorted_results: {0: [[1.0, 2.0], [5.0, 6.0]], 1: [[3.0, 4.0], [7.0, 8.0]]}
    
    Output:
    updated_centroids: {0: [3.0, 4.0], 1: [6.0, 7.0]}
    '''
    updated_centroids = {}
    for centroid_id, points in sorted_results.items():
        updated_centroids[centroid_id] = calculate_new_centroid(points)

    if is_retry:
        with open(f'Data/Reducer/R{reducer_id}/reducer_output.txt', 'a') as f:
            for centroid_id, centroid in updated_centroids.items():
                f.write(f'{centroid_id} {centroid[0]} {centroid[1]}\n')
    else:
        with open(f'Data/Reducer/R{reducer_id}/reducer_output.txt', 'w') as f:
            for centroid_id, centroid in updated_centroids.items():
                f.write(f'{centroid_id} {centroid[0]} {centroid[1]}\n')
    return updated_centroids

class ReducerServicer(master_mapper_reducer_pb2_grpc.ReducerServicer):
    def __init__(self, reducer_id):
        self.reducer_id = reducer_id
        self.sorted_results = {}

    def StartReduce(self, request, context):
        data_point_to_centroid_map = [] 
        for mapper_id in request.mapper_ids:
            channel = grpc.insecure_channel(f'localhost:{50051 + mapper_id}')
            stub = master_mapper_reducer_pb2_grpc.MapperStub(channel)

            # Printing that the reducer is requesting data from the mapper
            print(f"Reducer {self.reducer_id} is requesting data from Mapper {mapper_id}\n")

            mapper_request = master_mapper_reducer_pb2.ReturnDataRequest(reducer_id=request.reducer_id)
            # Printing that the reducer has received the data from the mapper
            print(f"Reducer {self.reducer_id} has received the data from Mapper {mapper_id}\n")
            response = stub.ReturnData(mapper_request)

            data_point_to_centroid_map.extend([(data_point.centroid_id, [data_point.x, data_point.y]) for data_point in response.data_points])

        # Priting the centroid_id handled by this reducer
        print(f"Reducer {self.reducer_id} is handling the following centroid_ids: {set([centroid_id for centroid_id, _ in data_point_to_centroid_map])}\n")

        sorted_results = shuffle_and_sort(data_point_to_centroid_map)
        # Printing that the shuffle and sort is called and returned for this reducer
        print(f"Reducer {self.reducer_id} has called the shuffle and sort function and returned\n")
        updated_centroids = reduce_function(self.reducer_id, sorted_results, request.is_retry)
        # Printing that the reduce function is called and returned for this reducer
        print(f"Reducer {self.reducer_id} has called the reduce function and returned\n")
        
        self.sorted_results = {}

        # Printing that the reducer has updated the centroids
        print(f"Reducer {self.reducer_id} has updated the centroids and stored them in the file\n")
        return master_mapper_reducer_pb2.StartReduceResponse(updated_centroids=[master_mapper_reducer_pb2.DataPoint(centroid_id=centroid_id, x=centroid[0], y=centroid[1]) for centroid_id, centroid in updated_centroids.items()], success=True)

def serve(reducer_id, port):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    master_mapper_reducer_pb2_grpc.add_ReducerServicer_to_server(ReducerServicer(reducer_id), server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    print(f"Reducer server started on port {port}")
    server.wait_for_termination()

if __name__ == '__main__':
    reducer_id = int(sys.argv[1])
    port = int(sys.argv[2])
    serve(reducer_id, port)