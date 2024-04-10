import os
import sys
import grpc
from concurrent import futures
import master_mapper_pb2
import master_mapper_pb2_grpc
import reducer_mapper_pb2
import numpy as np

# Calculate distance between two points
def calculate_distance(point1, point2):
    # Implement distance calculation method here
    return np.linalg.norm(point1 - point2)

def map_function(start_line, end_line, centroids):
    print("Centroids:", centroids)
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
def make_partition(mapped_results, mapper_id, num_reducers):
    for centroid_id, point in mapped_results:
        file_id = centroid_id % num_reducers
        file_path = f'Data/Mapper/M{mapper_id}/partition_{file_id}.txt'
        with open(file_path, 'a') as file:
            file.write(f'{centroid_id}\t{point[0]}\t{point[1]}\n')


# Mapper service
class MapperServicer(master_mapper_pb2_grpc.MapperServicer):
    def __init__(self, mapper_id):
        self.mapper_id = mapper_id
        self.centroids = []
        self.data_points = []

    def Map(self, request, context):
        # Extract line numbers to be processed
        print(request.end_index)
        line_numbers = [(x, y) for x,y in zip(request.start_index, request.end_index)]
        self.centroids = [[point.x, point.y] for point in request.centroids]
        print(request.centroids)
        # num_reducers = request.num_reducers
        try:
            # line_numbers = (start_line, end_line)
            data_points = map_function(line_numbers[mapper_id][0], line_numbers[mapper_id][1], self.centroids)
            self.data_points.extend(data_points)
            print(f'Mapper {self.mapper_id} processed {len(data_points)} data points')
            mapped_results = data_points

        # Call Partition function
            make_partition(mapped_results, self.mapper_id, num_reducers=request.num_reducers)
            print(f'Wrote partitions to files.')

            return master_mapper_pb2.MapResponse(success=True)
            
            '''
            data_points = [(1, array([0.4, 7.2])), (1, array([0.8, 9.8]))]
            '''
        except Exception as e:
            print(f'Error in Map: {e}')
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f'Error in Map: {e}')
            return master_mapper_pb2.MapResponse(success=False)

# class ReducerMapperServicer(reducer_mapper_pb2_grpc.ReducerMapperServicer):
    def ReturnData(self, request, context):
        print(f'Reducer {request.reducer_id} requesting data from Mapper {self.mapper_id}')
        reducer_id = request.reducer_id

        # read from partition file and send to reducer
        file_path = f'Data/Mapper/M{self.mapper_id}/partition_{reducer_id}.txt'
        data_points = []
        with open(file_path, 'r') as file:
            for line in file:
                centroid_id, x, y = map(float, line.strip().split('\t'))
                centroid_id = int(centroid_id)
                data_points.append((centroid_id, [x, y]))


        print(f'Mapper {self.mapper_id} sending {len(data_points)} data points to Reducer {reducer_id}')
        return master_mapper_pb2.ReturnDataResponse(data_points= [master_mapper_pb2.DataPoint(centroid_id=centroid_id, x=x, y=y) for centroid_id, [x, y] in data_points])



def serve(mapper_id, port):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    master_mapper_pb2_grpc.add_MapperServicer_to_server(MapperServicer(mapper_id), server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    mapper_id = int(sys.argv[1])
    port = int(sys.argv[2])
    serve(mapper_id, port)