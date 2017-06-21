from cassandra.query import SimpleStatement
from cassandra.cluster import Cluster
import random
import numpy as np
import pprint

cluster = Cluster()
session = cluster.connect("e35_taxi")
session.default_timeout = 30

def init_params(num_k, statement):
    rands = list()
    centroids = dict()
    clusters = dict()
    clusters["sum"] = dict()
    clusters["size"] = dict()
    for n in range(num_k):
        rands.append(random.randint(1, 612201)) # Max statement rows: 612201
        centroids[n] = list()
        centroids[n].append(None)
        clusters["sum"][n] = 0
        clusters["size"][n] = 1

    for i, row in enumerate(session.execute(statement)):
        for n in range(num_k):
            if i == rands[n]:
                print(i)
                centroids[n] = np.array(
                                    [row.start_loc_long,
                                    row.start_loc_lat,
                                    row.end_loc_long,
                                    row.end_loc_lat])
                print(centroids[n])
    print(i)
    return centroids, clusters

def get_centroid(coord, centroids):
    distances = dict()
    for i in range(1, len(centroids)):
        distances[i] = np.linalg.norm(coord - centroids[i])
    return min(distances, key=distances.get)

def is_finished(c, old_c):
    res = True
    for i in range(0, len(c)):
        if(not (c[i] == old_c[i]).all()):
            res = False
    return res

def kmeans(num_k):
    query = "SELECT start_loc_long, start_loc_lat, end_loc_long, end_loc_lat FROM loc GROUP BY start_loc_long, start_loc_lat, end_loc_long, end_loc_lat;"
    statement = SimpleStatement(query, fetch_size=5000)
    print("Init parameters...")
    centroids, clusters = init_params(3, statement)
    print("Centroids:")
    pprint.pprint(centroids)
    print("Clusters:")
    pprint.pprint(clusters)
    old_centroids = centroids
    old_centroids[0] += 1 # change for initial while check
    print("Computing kmeans...")
    n = 0
    while (not is_finished(old_centroids, centroids)):
        n+=1
        print("Iteration n%d...", n)
        for row in session.execute(statement):
            coord = np.array(
                        [row.start_loc_long,
                        row.start_loc_lat,
                        row.end_loc_long,
                        row.end_loc_lat])
            old_centroids = centroids
            j = get_centroid(coord, centroids)
            clusters["sum"][j] += coord
            clusters["size"][j] += 1
            centroids[j] = clusters["sum"][j]/clusters["size"][j]
    return centroids, clusters

if __name__ == "__main__":
    centroids, clusters = kmeans(2)
    print("Results:")
    print("Centroids:")
    pprint.pprint(centroids)

# for i, row in enumerate(session.execute(statement)):
    
#     j le num du centroid (x, c)
#     S[j] += x
#     n[j] += 1
# c = S/n

# Init parameters...
# 172061
# [ -8.618  41.156  -8.621  41.156]
# 194079
# [ -8.607  41.15   -8.618  41.179]
# 612201
# Centroids:
# {0: array([ -8.607,  41.15 ,  -8.618,  41.179]),
#  1: array([ -8.68 ,  41.157,  -8.631,  41.146])}
# Clusters:
# {'size': {0: 1, 1: 1, 2: 1}, 'sum': {0: 0, 1: 0, 2: 0}}
# Computing kmeans...

# Results:
# Centroids:
# {0: array([ -8.68 ,  41.157,  -8.631,  41.146]),
#  1: array([ -8.618,  41.156,  -8.621,  41.156])}
