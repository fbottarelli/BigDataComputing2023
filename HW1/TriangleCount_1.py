from pyspark import SparkContext, SparkConf
import sys
import os
import time
import statistics
import random as rand
from collections import defaultdict

def CountTriangles(edges):
    # Create a defaultdict to store the neighbors of each vertex
    neighbors = defaultdict(set)
    for edge in edges:
        u, v = edge
        neighbors[u].add(v)
        neighbors[v].add(u)

    # Initialize the triangle count to zero
    triangle_count = 0

    # Iterate over each vertex in the graph.
    # To avoid duplicates, we count a triangle <u, v, w> only if u<v<w
    for u in neighbors:
        # Iterate over each pair of neighbors of u
        for v in neighbors[u]:
            if v > u:
                for w in neighbors[v]:
                    # If w is also a neighbor of u, then we have a triangle
                    if w > v and w in neighbors[u]:
                        triangle_count += 1
    # Return the total number of triangles in the graph
    return triangle_count

def hashFunction(u,C):
    p = 8192
    a = rand.randint(1,p-1)
    b = rand.randint(0,p-1)
    return (((a*u+b) % p) % C)


def colorPartitioning(edge, C):
    # print(edge)
    u = edge[0]
    u_color = hashFunction(u, C)
    v = edge[1]
    v_color = hashFunction(v, C)
    if u_color != v_color:
        return []
    else:
        color = u_color
        return [(color, (u, v))]

# Algorithm 1
def MR_ApproxTCwithNodeColors(E, C):
    edgesColors = (E.flatMap(lambda edge: colorPartitioning(edge, C))
                    .groupByKey()
                    .mapValues(CountTriangles) # mapValues because it must applied only to values of key-value pairs
                    .values() # extract the values from the pairs
                    .collect()) # collect to return only the list from the RDD)
    t_final = ((C**2)*sum(edgesColors))
    return t_final

# Algorithm 2
def MR_ApproxTCwithSparkPartitions(E, C):
    # Map each edge to a random partition
    edges_partitions = E.map(lambda edge: (rand.randint(0, C - 1), edge))

    # Group the edges by partition and count the triangles in each partition
    partition_triangle_counts = (edges_partitions
                                 .groupByKey()
                                 .map(lambda x: (x[0], CountTriangles(x[1])))
                                 .collect())

    # Sum the triangle counts for each partition and multiply by C^2
    num_triangles = (C ** 2) * sum(count for _, count in partition_triangle_counts)

    return num_triangles


def main():
    # assert len(sys.argv) == 3, "Usage: python TriangleCount <C> <file-name>" # !!!!!!!!!!!1
    # SPARK SETUP
    conf = SparkConf().setAppName('TriangleCount')
    sc = SparkContext(conf=conf)

    ## Sys for debug
    C = 1
    R = 5
    data_path = "/home/fd/repo/BigDataComputing2023/data/facebook_small.txt"

    # INPUT READING
    C = int(C)

	# 2. Read input file and subdivide it into C random partitions
    assert os.path.isfile(data_path), "File or folder not found"
    rawData = sc.textFile(data_path,minPartitions=C).cache()
    rawData.repartition(numPartitions=C)
    edges = rawData.map(lambda x: tuple(map(int, x.split(","))))
	# SETTING GLOBAL VARIABLES
    
    # Loop to calculate R times MR_ApproxTCwithNodeColors
    numTriangles_counts = []
    runtimes = []
    for i in range(R):
        start_time = time.time()
        numTriangles_counts.append(MR_ApproxTCwithNodeColors(edges, C))
        end_time = time.time()
        runtimes.append(end_time - start_time)

    ## Output SECTION
    # Dataset = facebook_small.txt
    print("Dataset = ",os.path.basename(data_path).split('/')[-1])
    # Number of Edges = 88234
    numEdges = edges.count()
    print("Number of Edges = ", numEdges)
    # Number of Colors = 2
    print("Number of Colors = ", C)
    # Number of Repetitions = 5
    print("Number of Repetitions = ", R)
    # - Number of triangles (median over 5 runs) = 1595308
    print("Approximation through node coloring")
    print("- Number of triangles  =", statistics.median(numTriangles_counts))
    # - Running time (average over 5 runs) = 481 ms
    avg_runtime = sum(runtimes) / len(runtimes)
    print("- Running time (average over 5 runs) = {} ms".format(avg_runtime*1000))

    print("Approximation through Spark partitions")
    # - Number of triangles = 1587400
    
    # - Running time = 140 ms

if __name__ == "__main__":
    main()