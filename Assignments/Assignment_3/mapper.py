import sys
import grpc
from concurrent import futures
import master_mapper_reducer_pb2
import master_mapper_reducer_pb2_grpc
import numpy as np

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
    mapped_results: [(0, [1.0, 2.0]), (1, [3.0, 4.0]), (0, [5.0, 6.0]), (1, [7.0, 8.0]), ...]
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
                    print(f"centroid_id: {centroid_id}, centroid: {centroid}")
                    distance = calculate_distance(point, centroid)
                    print(f"distance of point {point} from centroid {centroid} is {distance}")
                    if distance < min_distance:
                        min_distance = distance
                        nearest_centroid = centroid_id
                        print(f"min_distance for point {point} is {min_distance} and nearest_centroid is {nearest_centroid}")
                data_point_to_centroid_map.append((nearest_centroid, point))
                print(f"point {point} is mapped to centroid {nearest_centroid}")
    return data_point_to_centroid_map

def write_partitions_to_files(data_point_to_centroid_map, mapper_id, num_reducers):
    '''
    Based on hash of the centroid_id, write the data points to the respective partition files
    
    Input:
    mapped_results: [(0, [1.0, 2.0]), (1, [3.0, 4.0]), (0, [5.0, 6.0]), (1, [7.0, 8.0]), ...]
    mapper_id: int
    num_reducers: int
    
    Output:
    partition files
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

# Mapper service
class MapperServicer(master_mapper_reducer_pb2_grpc.MapperServicer):
    def __init__(self, mapper_id):
        self.mapper_id = mapper_id
        self.centroids = []
        self.data_point_to_centroid_map = []

    def Map(self, request, context):
        # Obtain line numbers and centroids from gRPC request
        line_numbers = [(x, y) for x,y in zip(request.start_index, request.end_index)]
        self.centroids = [[point.x, point.y] for point in request.centroids]

        # print("Centroids received by Mapper: ", self.centroids)
        
        try:
            # Call Map function
            data_point_to_centroid_map = map_function(line_numbers[mapper_id][0], line_numbers[mapper_id][1], self.centroids)
            self.data_point_to_centroid_map.extend(data_point_to_centroid_map)
            # print(f'Mapper {self.mapper_id} processed {len(data_point_to_centroid_map)} data points and mapped them to centroids.')
            # print(f"first 5 data points from mapper {self.mapper_id} are: {self.data_point_to_centroid_map[:5]}")

            # Call Partition function
            write_partitions_to_files(data_point_to_centroid_map, self.mapper_id, num_reducers=request.num_reducers)
            # print(f'Mapper {self.mapper_id} wrote partitions to files.')
            return master_mapper_reducer_pb2.MapResponse(success=True)
        
        except Exception as e:
            print(f'Error in Map: {e}')
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f'Error in Map: {e}')
            return master_mapper_reducer_pb2.MapResponse(success=False)

# class ReducerMapperServicer(reducer_mapper_pb2_grpc.ReducerMapperServicer):
    def ReturnData(self, request, context):
        # print(f'Reducer {request.reducer_id} requesting data from Mapper {self.mapper_id}')
        reducer_id = request.reducer_id

        # Send data points to the reducer from the data_point_to_centroid_map
        data_points = []
        for centroid_id, [x, y] in self.data_point_to_centroid_map:
            if centroid_id == reducer_id:
                data_points.append((centroid_id, [x, y]))

        # print(f'Mapper {self.mapper_id} sending {len(data_points)} data points to Reducer {reducer_id}')
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
    # Inputs
    inputs = [int(x) for x in sys.argv[1:]]
    mapper_id = inputs[0]
    port = inputs[1]
    serve(mapper_id, port)


