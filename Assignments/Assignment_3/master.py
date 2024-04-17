import grpc
import sys
import os
import utils
import master_mapper_reducer_pb2
import master_mapper_reducer_pb2_grpc
import random
import subprocess
from concurrent.futures import ThreadPoolExecutor
import asyncio

def compile_centroids(reducer_outputs):
    '''
    Concatenates from reducer outputs to get the updated centroids

    Input:
    - reducer_outputs: List of dictionaries, where each dictionary represents the output of a reducer
    example: {0: [3.0, 4.0], 1: [6.0, 7.0], 2: [9.0, 10.0], 3: [12.0, 13.0]}

    Output:
    - List of updated centroids which will be used in the next iteration
    example: [[3.0, 4.0], [6.0, 7.0], [9.0, 10.0], [12.0, 13.0]]
    - write to centroids.txt
    format: 3.0 , 4.0
            6.0 , 7.0
    '''
    updated_centroids = []
    for reducer_output in reducer_outputs:
        for centroid_id, centroid in reducer_output.items():
            updated_centroids.append(centroid)

    with open('Data/centroids.txt', 'w') as f:
        for centroid in updated_centroids:
            f.write(','.join(map(str, centroid)) + '\n')

    return updated_centroids

async def map_rpc(mapper_stub, mapper_request, max_retries=10):
    count = 0
    res = mapper_stub.Map(mapper_request)
    while ((isinstance(res, Exception) or not res.success) and count < max_retries):
        print(f"Error in map phase. Retry count: {count}/{max_retries}")
        res = mapper_stub.Map(mapper_request)
        count += 1
    return res

async def map_phase(input_splits, num_mappers, num_reducers, centroids, mapper_stubs, max_map_retries=10, is_retry=False):
    
    try:
        tasks = []
        for mapper_id, mapper_stub in enumerate(mapper_stubs):
            response_centroids = [master_mapper_reducer_pb2.Point(x=a, y=b) for a, b in centroids]
            mapper_request = master_mapper_reducer_pb2.MapRequest(start_index=[x[0] for x in input_splits],
                                                                    end_index=[x[1] for x in input_splits],
                                                                    num_reducers=num_reducers,
                                                                    centroids=response_centroids,
                                                                    is_retry=is_retry)
            tasks.append(map_rpc(mapper_stub, mapper_request, max_map_retries))
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        return responses
    except Exception as e:
        print(f"Error in map phase. Retrying with other mappers.")
        print(e)

async def reduce_rpc(reducer_stub, reducer_request, max_retries=10):
    count = 0
    res = reducer_stub.StartReduce(reducer_request)
    while ((isinstance(res, Exception) or not res.success) and count < max_retries):
        print(f"Error in reduce phase. Retry count: {count}/{max_retries}")
        res = reducer_stub.StartReduce(reducer_request)
        count += 1
    return res

async def reduce_phase(num_mappers, reducer_stubs, successful_map_indices,  max_reduce_retries=3, is_retry=False):
    try:
        tasks = []
        for reducer_id, reducer_stub in enumerate(reducer_stubs):
            reducer_request = master_mapper_reducer_pb2.StartReduceRequest(mapper_ids=successful_map_indices, is_retry=is_retry)
            tasks.append(reduce_rpc(reducer_stub, reducer_request, max_reduce_retries))
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        return responses
    except Exception as e:
        print(f"Error in reducer phase. Retrying with other reducers.")
    raise RuntimeError("Max retry count exceeded for reduce phase.")

def run_iteration(input_splits, num_mappers, num_reducers, centroids):
    # Initialize gRPC channels to mappers

    mapper_channels = [grpc.insecure_channel(f'localhost:{50051 + i}') for i in range(num_mappers)]
    mapper_stubs = [master_mapper_reducer_pb2_grpc.MapperStub(channel) for channel in mapper_channels]

    # Initialize gRPC channels to reducers
    reducer_channels = [grpc.insecure_channel(f'localhost:{50051 + num_mappers + i}') for i in range(num_reducers)]
    reducer_stubs = [master_mapper_reducer_pb2_grpc.ReducerStub(channel) for channel in reducer_channels]
    
    # Step 1: Map phase and Partition phase
    map_responses = asyncio.run(map_phase(input_splits, num_mappers, num_reducers, centroids, mapper_stubs, is_retry=False))
    print(f'map_responses:{map_responses}')
    # Check if all mappers have completed their task
    successful_map_indices = [i for i, response in enumerate(map_responses) if not isinstance(response, Exception) and response.success]
    if len(successful_map_indices) < num_mappers:
        print("Error in map phase. Retrying with other mappers.")
        mapper_stubs = [mapper_stubs[i] for i in successful_map_indices]
        # update input splits in a way that the indexes of the successful mappers contain the splits of the unsuccessful mappers and those of unscessful mappers have just (0,0)
        unsuccessful_splits = [input_splits[i] for i in range(num_mappers) if i not in successful_map_indices]
        updated_input_splits = []
        for i in range(num_mappers):
            if i in successful_map_indices and len(unsuccessful_splits) > 0:
                updated_input_splits.append(unsuccessful_splits.pop(0))
            else:
                updated_input_splits.append((0, 0))

        map_responses = asyncio.run(map_phase(updated_input_splits, len(successful_map_indices), num_reducers, centroids, mapper_stubs, is_retry=True))

    
    # Step 2: Reduce phase
    reduce_responses = asyncio.run(reduce_phase(num_mappers, reducer_stubs, successful_map_indices, is_retry=False))

    # Check if all reducers have completed their task
    successful_reduce_indices = [i for i, response in enumerate(reduce_responses) if not isinstance(response, Exception) and response.success]
    print(successful_reduce_indices, num_reducers)
    if len(successful_reduce_indices) < num_reducers:
        print("Error in reducer phase. Retrying with other reducers.")
        reducer_stubs = [reducer_stubs[i] for i in successful_reduce_indices]
        reduce_responses = asyncio.run(reduce_phase(num_mappers, reducer_stubs, successful_map_indices, is_retry=True))

    # print([res.updated_centroids for res in reduce_responses])
    compiled_reducers_output_ = [{data_point.centroid_id: [data_point.x, data_point.y]} for res in reduce_responses for data_point in res.updated_centroids]

    '''
    parse output files generated by reducers

    Input: 
    centroid_id x y

    Output:
    [{0: [1.0, 2.0], 1: [3.0, 4.0]}, {2: [5.0, 6.0], 3: [7.0, 8.0]}]
    '''
    # compiled_reducers_output_ = []
    # for i in range(num_reducers):
    #     with open(f'Data/Reducer/R{i}/reducer_output.txt', 'r') as f:
    #         reducer_output = {}
    #         for line in f:
    #             parts = line.strip().split()
    #             centroid_id = int(parts[0])
    #             x = float(parts[1])
    #             y = float(parts[2])
    #             reducer_output[centroid_id] = [x, y]
    #         compiled_reducers_output_.append(reducer_output)

    return compile_centroids(compiled_reducers_output_)



# def run_iteration(input_splits, num_mappers, num_reducers, centroids):
#     # Initialize gRPC channels to mappers
#     mapper_channels = [grpc.insecure_channel(f'localhost:{50051 + i}') for i in range(num_mappers)]
#     mapper_stubs = [master_mapper_reducer_pb2_grpc.MapperStub(channel) for channel in mapper_channels]

#     # Initialize gRPC channels to reducers
#     reducer_channels = [grpc.insecure_channel(f'localhost:{50051 + num_mappers + i}') for i in range(num_reducers)]
#     reducer_stubs = [master_mapper_reducer_pb2_grpc.ReducerStub(channel) for channel in reducer_channels]
    
#     # Step 1: Map phase and Partition phase
#     map_response_count = 0
#     for mapper_id, mapper_stub in enumerate(mapper_stubs):
#         response_centroids = [master_mapper_reducer_pb2.Point(x = a, y = b) for a,b in centroids] # convert centroids to protobuf format
#         mapper_request = master_mapper_reducer_pb2.MapRequest(start_index=[x[0] for x in input_splits], end_index = [x[1] for x in input_splits], num_reducers = num_reducers, centroids = response_centroids) # create request
#         response = mapper_stub.Map(mapper_request)
#         if response.success:
#             map_response_count += 1

#     # for fault tolarence, first check if all mappers have completed their task
#     if map_response_count != num_mappers:
#         print("Error in map phase")

#     reduce_response_count = 0
#     for reducer_id, reducer_stub in enumerate(reducer_stubs):
#         reducer_request = master_mapper_reducer_pb2.StartReduceRequest(mapper_ids = list(range(num_mappers)))
#         response = reducer_stub.StartReduce(reducer_request)
#         if response.success:
#             reduce_response_count += 1

#     if reduce_response_count != num_reducers:
#         print("Error in reducer phase")


#     '''
#     parse output files generated by reducers

#     Input: 
#     centroid_id x y

#     Output:
#     [{0: [1.0, 2.0], 1: [3.0, 4.0]}, {2: [5.0, 6.0], 3: [7.0, 8.0]}]
#     '''
#     compiled_reducers_output_ = []
#     for i in range(num_reducers):
#         with open(f'Data/Reducer/R{i}/reducer_output.txt', 'r') as f:
#             reducer_output = {}
#             for line in f:
#                 parts = line.strip().split()
#                 centroid_id = int(parts[0])
#                 x = float(parts[1])
#                 y = float(parts[2])
#                 reducer_output[centroid_id] = [x, y]
#             compiled_reducers_output_.append(reducer_output)

#     return compile_centroids(compiled_reducers_output_)



if __name__ == '__main__':

    input_data_points_filepath = "Data/Input/points.txt"
    centroids_file_path = "Data/centroids.txt"
    centroids = []

    # Input data points
    with open(input_data_points_filepath, 'r') as f:
        input_data_points = [list(map(float, line.strip().split(','))) for line in f]
    
    # Inputs
    inputs = [int(x) for x in sys.argv[1:5]]
    num_mappers, num_reducers, num_iterations, num_clusters = inputs

    dump_file = 'test_outputs/dump.txt'
    dump_file = open(dump_file, 'w')
    dump_file.write(f'Input data points: {input_data_points}\n')
    dump_file.write(f'Number of mappers: {num_mappers}\n')
    dump_file.write(f'Number of reducers: {num_reducers}\n')
    dump_file.write(f'Number of iterations: {num_iterations}\n')
    dump_file.write(f'Number of clusters: {num_clusters}\n')

    # Make Directories
    for i in range(num_mappers):
        os.makedirs(f'Data/Mapper/M{i}', exist_ok=True)
    for i in range(num_reducers):
        os.makedirs(f'Data/Reducer/R{i}', exist_ok=True)

    # Input splits
    input_splits = utils.split_input_data(len(input_data_points), num_mappers)
    dump_file.write(f'Input splits generated: {input_splits}\n')

    # Initial Centroids
    with open(centroids_file_path, 'w') as f:
        for _ in range(num_clusters):
            centroid = random.choice(input_data_points)
            centroids.append(centroid)
            f.write(f'Initial centroid point{_}: ' + ','.join(map(str, centroid)) + '\n')
    dump_file.write(f'Initial centroids: {centroids}\n')

    # Iterations and convergence
    for i in range(num_iterations):
        dump_file.write(f'\nIteration {i + 1}: \n')
        print(f'Iteration {i + 1}')
        updated_centroids = run_iteration(input_splits, num_mappers, num_reducers, centroids)
        dump_file.write(f'Updated centroids for iteration_{i}: {updated_centroids}\n')
        if updated_centroids == centroids:
            break
        centroids = updated_centroids
    dump_file.write(f'\nFinal centroids after {i + 1} iterations: {centroids}\n')
    dump_file.close()

    print(f'Final centroids after {i + 1} iterations: {centroids}')