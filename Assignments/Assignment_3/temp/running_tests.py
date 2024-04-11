import numpy as np

def read_data(file_path):
    data_points = []
    with open(file_path, 'r') as file:
        for line in file:
            parts = line.strip().split(',')
            x = float(parts[0])
            y = float(parts[1])
            data_points.append([x, y])
    print(f'Input data: {data_points}')
    return np.array(data_points)

def calculate_distance(point1, point2):
    return np.sqrt(np.sum((point1 - point2)**2))

def assign_clusters(data_points, centroids):
    clusters = {}
    for point in data_points:
        min_dist = float('inf')
        closest_centroid = None
        for centroid_id, centroid in enumerate(centroids):
            dist = calculate_distance(point, centroid)
            if dist < min_dist:
                min_dist = dist
                closest_centroid = centroid_id
        if closest_centroid not in clusters:
            clusters[closest_centroid] = []
        clusters[closest_centroid].append(point)
    return clusters

def update_centroids(clusters):
    new_centroids = []
    for cluster_points in clusters.values():
        new_centroid = np.mean(cluster_points, axis=0)
        new_centroids.append(new_centroid)
    return np.array(new_centroids)

def kmeans(data_points, k, max_iterations=2, tol=1e-4):
    # Initialize centroids randomly
    centroids = data_points[np.random.choice(len(data_points), k, replace=False)]
    # take the first two points as initial centroids
    # centroids = data_points[:k]
    
    for _ in range(max_iterations):
        # Assign clusters
        clusters = assign_clusters(data_points, centroids)
        
        # Update centroids
        new_centroids = update_centroids(clusters)
        
        # Check convergence
        if np.sum(np.abs(new_centroids - centroids)) < tol:
            break
        
        centroids = new_centroids
    
    return centroids

if __name__ == "__main__":
    file_path = "../Data/Input/points.txt"
    data_points = read_data(file_path)
    
    # Set number of clusters (K)
    K = 2
    
    # Perform K-means clustering
    centroids = kmeans(data_points, K)
    
    print("Centroids:")
    for i, centroid in enumerate(centroids):
        print(f"Cluster {i+1}: {centroid}")
