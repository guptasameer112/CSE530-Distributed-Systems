import os
import sys
# import grpc
# from concurrent import futures
# import k_means_pb2
# import k_means_pb2_grpc
# import master_mapper_pb2
# import master_mapper_pb2_grpc
import numpy as np

# Calculate distance between two points
def calculate_distance(point1, point2):
    # Implement distance calculation method here
    return np.linalg.norm(point1 - point2)

def map_function(start_line, end_line, centroids):
    mapped_results = []
    file_path = 'Data/Input/points.txt'
    with open(file_path, 'r') as file:
        for idx, line in enumerate(file):
            if idx >= start_line and idx < end_line:
                # Process line only if it falls within the specified range
                # points are comma separated
                point = np.array(list(map(float, line.strip().split(','))))
                # print("Point:", point)
                min_distance = float('inf')
                nearest_centroid = None
                for centroid_id, centroid in enumerate(centroids):
                    distance = calculate_distance(point, centroid)
                    if distance < min_distance:
                        min_distance = distance
                        nearest_centroid = centroid_id
                mapped_results.append((nearest_centroid, point))
    return mapped_results

# Implement the Partition function
def partition(mapped_results, num_reducers):
    partitions = [[] for _ in range(num_reducers)]
    for centroid_id, point in mapped_results:
        reducer_id = centroid_id % num_reducers
        partitions[reducer_id].append((centroid_id, point))
    return partitions

# Mapper service
class MapperServicer(k_means_pb2_grpc.MapperServicer):
    def __init__(self, mapper_id, centroids):
        self.mapper_id = mapper_id
        self.centroids = centroids
        self.data_points = []

    def Map(self, request, context):
        # Extract line numbers to be processed
        line_numbers = request.line_numbers
        num_reducers = request.num_reducers
        try:
            # line_numbers = (start_line, end_line)
            data_points = map_function(line_numbers[0], line_numbers[1], self.centroids)
            self.data_points.extend(data_points)
            '''
            data_points = [(1, array([0.4, 7.2])), (1, array([0.8, 9.8]))]
            '''
        except Exception as e:
            print(f'Error in Map: {e}')
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f'Error in Map: {e}')
            return k_means_pb2.MapResponse(status="Failed")
        
    def Partition(self, request, context):
        # Using the number of reducers and a mod function along with data_points obtained from Map, assign each data point to a reducer, and by that I mean write it to a file partition_{reducer_id}.txt inside the Data/Input/Mapper/M{self.mapper_id} directory

        # Extract data points
        mapped_results = request.data_points

        # Call Partition function
        partitions = partition(mapped_results, request.num_reducers)

        # Write partitions to file
        for reducer_id, partition in enumerate(partitions):
            partition_file = f'Data/Input/Mapper/M{self.mapper_id}/partition_{reducer_id}.txt'
            os.makedirs(os.path.dirname(partition_file), exist_ok=True)
            with open(partition_file, 'w') as f:
                for centroid_id, point in partition:
                    f.write(f"{centroid_id} {point}\n")

        # Send success message to master
        return k_means_pb2.PartitionResponse(status="Success")
    
    def GetPartitions(self, request, context):
        # Read requested partition from file and send to reducer
        reducer_id = request.reducer_id
        input_directory = f'Data/Input/Mapper/M{self.mapper_id}'
        partition_file_path = f'{input_directory}/partition_{reducer_id}.txt'
        with open(partition_file_path, 'r') as f:
            for line in f:
                point = eval(line.strip())
                yield reducer_mapper_pb2.GetPartitionsResponse(partition=point)


def serve(mapper_id, centroids, port):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    k_means_pb2_grpc.add_MapperServicer_to_server(MapperServicer(mapper_id, centroids), server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    mapper_id = int(sys.argv[1])
    centroids = []  # Provide centroids
    port = int(sys.argv[2])
    serve(mapper_id, centroids, port)

    # Test map_function
    centroids = [[1, 2], [3, 4]]
    mapped_results = map_function(0, 2, centroids)
    print(mapped_results)