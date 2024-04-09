import grpc
import sys
# import k_means_pb2
# import k_means_pb2_grpc
import random

def split_input_data(num_lines, num_mappers):
    lines_per_mapper = num_lines // num_mappers
    input_splits = []
    start_line = 0
    for i in range(num_mappers):
        end_line = start_line + lines_per_mapper
        if i == num_mappers - 1:  # Last mapper gets remaining lines
            end_line = num_lines
        input_splits.append((start_line, end_line))
        start_line = end_line
    print(f'Input splits: {input_splits}')
    return input_splits

def compile_centroids(reducer_outputs):
    # collect all the centroids from all the reducers
    updated_centroids = {}
    for reducer_output in reducer_outputs:
        for centroid_id, centroid in reducer_output.items():
            if centroid_id not in updated_centroids:
                updated_centroids[centroid_id] = []
            updated_centroids[centroid_id].append(centroid)

    print(f'Updated centroids: {updated_centroids}')

    # Store the updated centroids for the next iteration in comma separated format
    with open('Data/centroids.txt', 'w') as f:
        for centroid_id, centroids in updated_centroids.items():
            updated_centroid = [sum(x) / len(centroids) for x in zip(*centroids)]
            f.write(','.join(map(str, updated_centroid)) + '\n')

    return updated_centroids

def run_iteration(input_data, num_mappers, num_reducers, centroids):
    # Split input data
    number_of_lines = len(input_data)
    input_splits = split_input_data(number_of_lines, num_mappers)

    # Initialize gRPC channels to mappers
    mapper_channels = [grpc.insecure_channel(f'localhost:{50051 + i}') for i in range(num_mappers)]
    mapper_stubs = [k_means_pb2_grpc.MapperStub(channel) for channel in mapper_channels]


    # Step 1: Map phase
    for mapper_id, mapper_stub in enumerate(mapper_stubs):
        mapper_request = k_means_pb2.MapRequest(data_points=input_splits[mapper_id])
        mapper_stub.Map(mapper_request)

    # Step 2: Partition phase
    partition_responses = [mapper_stub.data_points for mapper_stub in mapper_stubs]

    # Initialize gRPC channels to reducers
    reducer_channels = [grpc.insecure_channel(f'localhost:{50051 + num_mappers + i}') for i in range(num_reducers)]
    reducer_stubs = [k_means_pb2_grpc.ReducerStub(channel) for channel in reducer_channels]

    # Step 3: Shuffle and Sort phase
    shuffle_sort_request = k_means_pb2.ShuffleSortRequest(partitions=partition_responses)
    for reducer_stub in reducer_stubs:
        reducer_stub.ShuffleSort(shuffle_sort_request)

    # Step 4: Reduce phase
    reducer_outputs = []
    for reducer_id, reducer_stub in enumerate(reducer_stubs):
        reducer_request = k_means_pb2.ReduceRequest(reducer_id=reducer_id)
        reducer_output = reducer_stub.Reduce(reducer_request)
        reducer_outputs.append(reducer_output)

    # Step 5: Compile centroids
    updated_centroids = compile_centroids(reducer_outputs)

    return updated_centroids

if __name__ == '__main__':

    # Parse command line arguments
    num_mappers = int(sys.argv[1])
    num_reducers = int(sys.argv[2])
    num_iterations = int(sys.argv[3])
    num_clusters = 2

    print(f'Number of mappers: {num_mappers}')
    print(f'Number of reducers: {num_reducers}')
    print(f'Number of iterations: {num_iterations}')

    input_data_file = "Data/Input/points.txt"
    centroids_file = "Data/centroids.txt"

    # read input data
    with open(input_data_file, 'r') as f:
        input_data = [list(map(float, line.strip().split(','))) for line in f]
    print(f'Input data: {input_data}')

    # Generate random initial centroids
    with open(centroids_file, 'w') as f:
        for _ in range(num_clusters):
            # Choose k initial means µ1, . . . , µk uniformly at random from the set X.
            centroid = random.choice(input_data)
            print(f'Initial centroid: {centroid}')
            f.write(' '.join(map(str, centroid)) + '\n')

    # Run iterations and if they converge before num_iterations, stop early
    for i in range(num_iterations):
        updated_centroids = run_iteration(input_data, num_mappers, num_reducers, centroids)
        print(f'Iteration {i + 1}: {updated_centroids}')
        if updated_centroids == centroids:
            break
        centroids = updated_centroids

    print(f'Final centroids: {updated_centroids}')
