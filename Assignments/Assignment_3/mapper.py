import sys
import grpc
from concurrent import futures
import master_mapper_reducer_pb2
import master_mapper_reducer_pb2_grpc
import numpy as np
import random
import time

dump_file = 'test_outputs/dump.txt'
dump_file = open(dump_file, 'w')

def calculate_distance(point1, point2):
    '''
    This function calculates the Euclidean distance between two points
    
    Input:
    point1: np.array
    point2: np.array

    Output:
    distance: float
    '''

    return np.linalg.norm(point1 - point2)

def map_function(start_line, end_line, centroids):
    '''
    This function reads the data points from the file and maps them to the nearest centroid

    Input:
    start_line: int
    end_line: int
    centroids: list of centroids

    Output:
    mapped_results: [(0, [1.0, 2.0], 1), (1, [3.0, 4.0], 1), (0, [5.0, 6.0], 1), (1, [7.0, 8.0], 1), ...]
    '''

    data_point_to_centroid_map = []
    data_input_file_path = 'Data/Input/points.txt'

    with open(data_input_file_path, 'r') as file:
        for idx, line in enumerate(file):
            if idx >= start_line and idx < end_line:
                point = np.array(list(map(float, line.strip().split(','))))

                min_distance = float('inf')
                nearest_centroid = None

                for centroid_id, centroid in enumerate(centroids):
                    distance = calculate_distance(point, centroid)
                    if distance < min_distance:
                        min_distance = distance
                        nearest_centroid = centroid_id
                data_point_to_centroid_map.append((nearest_centroid, point))
    return data_point_to_centroid_map

def write_partitions_to_files(data_point_to_centroid_map, mapper_id, num_reducers):
    '''
    Based on hash of the centroid_id, write the data points to the respective partition files
    
    Input:
    mapped_results: [(0, [1.0, 2.0], 1), (1, [3.0, 4.0], 1), (0, [5.0, 6.0], 1), (1, [7.0, 8.0], 1), ...]
    mapper_id: int
    num_reducers: int
    
    Output:
    partition files
    data_point_to_centroid_map: [(0, [1.0, 2.0]), (1, [3.0, 4.0]), (0, [5.0, 6.0]), (1, [7.0, 8.0]), ...]
    '''

    for reducer_id in range(num_reducers):
        file_path = f'Data/Mapper/M{mapper_id}/partition_{reducer_id}.txt'
        with open(file_path, 'w') as file:
            file.write('')

    for centroid_id, point in data_point_to_centroid_map:
        reducer_id = centroid_id % num_reducers
        file_path = f'Data/Mapper/M{mapper_id}/partition_{reducer_id}.txt'
        with open(file_path, 'a') as file:
            file.write(f'{centroid_id}\t{point[0]}\t{point[1]}\n')

    return data_point_to_centroid_map

def write_partitions_to_files_retry(data_point_to_centroid_map, mapper_id, num_reducers):
    '''
    Based on hash of the centroid_id, write the data points to the respective partition files
    
    Input:
    mapped_results: [(0, [1.0, 2.0]), (1, [3.0, 4.0]), (0, [5.0, 6.0]), (1, [7.0, 8.0]), ...]
    mapper_id: int
    num_reducers: int
    
    Output:
    partition files
    '''

    for centroid_id, point in data_point_to_centroid_map:
        reducer_id = centroid_id % num_reducers
        file_path = f'Data/Mapper/M{mapper_id}/partition_{reducer_id}.txt'
        with open(file_path, 'a') as file:
            file.write(f'{centroid_id}\t{point[0]}\t{point[1]}\n')

# Mapper service
class MapperServicer(master_mapper_reducer_pb2_grpc.MapperServicer):
    def __init__(self, mapper_id):
        self.mapper_id = mapper_id
        self.centroids = []
        self.data_point_to_centroid_map = [] # (centroid_id, datapoint)
        self.num_reducers = 0

    def Map(self, request, context):
        line_numbers = [(x, y) for x,y in zip(request.start_index, request.end_index)]
        self.centroids = [[point.x, point.y] for point in request.centroids]

        self.num_reducers = request.num_reducers
        
        try:
            data_point_to_centroid_map = map_function(line_numbers[mapper_id][0], line_numbers[mapper_id][1], self.centroids)
            # Printing that the map function has been called and returned for this mapper
            print(f"Mapper {self.mapper_id} has called the map function and returned\n")

            # Printing the centroid_id obtained from the map function for this mapper
            centroid_id_handled_by_mapper = [centroid_id for centroid_id, point in data_point_to_centroid_map]
            print(f"Mapper {self.mapper_id} has handled the following centroid_ids: {set(centroid_id_handled_by_mapper)}\n")

            self.data_point_to_centroid_map.extend(data_point_to_centroid_map)

            if (request.is_retry):
                write_partitions_to_files_retry(data_point_to_centroid_map, self.mapper_id, num_reducers=request.num_reducers)
            else:
                write_partitions_to_files(data_point_to_centroid_map, self.mapper_id, num_reducers=request.num_reducers)
                # Printing that the partitions have been written to the files for this mapper
                print(f"Mapper {self.mapper_id} has written the partitions to the files\n")
            return master_mapper_reducer_pb2.MapResponse(success=True)
        
        except Exception as e:
            print(f'Error in Map: {e}')
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f'Error in Map: {e}')
            return master_mapper_reducer_pb2.MapResponse(success=False)

    def ReturnData(self, request, context):
        reducer_id = request.reducer_id

        data_points = []
        for centroid_id, [x, y] in self.data_point_to_centroid_map:
            if centroid_id % self.num_reducers == reducer_id: 
                data_points.append((centroid_id, [x, y]))

        # Printing that the data points are sent for reducer
        print(f"Mapper {self.mapper_id} has sent the data points for Reducer {reducer_id}\n")
        return master_mapper_reducer_pb2.ReturnDataResponse(data_points= [master_mapper_reducer_pb2.DataPoint(centroid_id=centroid_id, x=x, y=y) for centroid_id, [x, y] in data_points])


def serve(mapper_id, port):
    '''
    Implement the serve function to start the gRPC server
    '''
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    master_mapper_reducer_pb2_grpc.add_MapperServicer_to_server(MapperServicer(mapper_id), server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    print(f"Mapper {mapper_id} started on port {port}")
    server.wait_for_termination()


if __name__ == '__main__':
    inputs = [int(x) for x in sys.argv[1:]]
    mapper_id = inputs[0]
    port = inputs[1]
    serve(mapper_id, port)


