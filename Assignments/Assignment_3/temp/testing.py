import numpy as np
import random
import grpc
import raft_pb2
import raft_pb2_grpc
import threading
from concurrent import futures
from mapper import mapper
from reducer import reducer

class MasterNode(raft_pb2_grpc.RaftServicer):
    def __init__(self, m, r, k, num_iterations):
        self.m = m
        self.r = r
        self.k = k
        self.num_iterations = num_iterations
        self.data = []  # List to store the dataset
        
        # Initialize gRPC server
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        raft_pb2_grpc.add_RaftServicer_to_server(self, self.server)
        self.server.add_insecure_port('[::]:50051')
        self.server.start()

        # Load data from file or generate random data
        # Example: self.load_data_from_file('data.txt')
        self.load_random_data(100, 2)  # Example: Generate 100 data points with 2 dimensions
        
        # Start K-Means algorithm
        self.run_k_means()

    def load_data_from_file(self, filename):
        # Implement loading data from file
        pass

    def load_random_data(self, num_points, num_dimensions):
        # Generate random data
        self.data = [np.random.rand(num_dimensions) for _ in range(num_points)]

    def run_k_means(self):
        # Step 1: Choose k initial means uniformly at random
        centroids = random.sample(self.data, self.k)
        
        # Iterate over number of iterations
        for _ in range(self.num_iterations):
            # Step 2: Map phase
            intermediate_results = self.mapper(centroids)
            
            # Step 3: Reduce phase
            updated_centroids = self.reducer(intermediate_results)
            
            # Step 5: Update centroids
            centroids = [total_x / total_count for total_x, total_count in updated_centroids]
        
        final_centroids = np.array(centroids)
        print("Final centroids:", final_centroids)

    def mapper(self, centroids):
        # Split data into chunks for each mapper
        chunk_size = len(self.data) // self.m
        data_chunks = [self.data[i:i + chunk_size] for i in range(0, len(self.data), chunk_size)]

        # Initialize mapper threads
        mapper_threads = []
        intermediate_results = []
        for chunk in data_chunks:
            mapper_thread = threading.Thread(target=self.mapper_thread, args=(chunk, centroids, intermediate_results))
            mapper_thread.start()
            mapper_threads.append(mapper_thread)
        
        # Wait for all mapper threads to finish
        for mapper_thread in mapper_threads:
            mapper_thread.join()
        
        # Combine intermediate results from all mappers
        combined_results = {}
        for result in intermediate_results:
            for key, value in result.items():
                if key not in combined_results:
                    combined_results[key] = value
                else:
                    combined_results[key].extend(value)
        
        return combined_results

    def mapper_thread(self, data_chunk, centroids, intermediate_results):
        result = mapper(data_chunk, centroids)
        intermediate_results.append(result)

    def reducer(self, intermediate_results):
        # Initialize reducer threads
        reducer_threads = []
        updated_centroids = []
        for key, value in intermediate_results.items():
            reducer_thread = threading.Thread(target=self.reducer_thread, args=(key, value, updated_centroids))
            reducer_thread.start()
            reducer_threads.append(reducer_thread)
        
        # Wait for all reducer threads to finish
        for reducer_thread in reducer_threads:
            reducer_thread.join()
        
        return updated_centroids

    def reducer_thread(self, key, values, updated_centroids):
        result = reducer(key, values)
        updated_centroids.append(result)

    def Run(self):
        try:
            while True:
                pass
        except KeyboardInterrupt:
            self.server.stop(0)
            print("Server stopped")

if __name__ == '__main__':
    master_node = MasterNode(4, 2, 3, 10)  # Example: 4 mappers, 2 reducers, 3 centroids, 10 iterations
    master_node.Run()



# <-------------------------------------------------------------------------------------->

