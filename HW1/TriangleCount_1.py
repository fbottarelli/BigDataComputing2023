from pyspark import SparkContext, SparkConf
import sys
import os
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


def MR_ApproxTCwithNodeColors(V, C):
    edgesColors = (V.flatMap(lambda edge: colorPartitioning(edge, C))
                    .groupByKey()
                    .mapValues(CountTriangles) # mapValues because it must applied only to values of key-value pairs
                    .values() # extract the values from the pairs
                    .collect()) # collect to return only the list from the RDD)
    return edgesColors

def main():
    # assert len(sys.argv) == 3, "Usage: python TriangleCount <C> <file-name>" # !!!!!!!!!!!1
    # SPARK SETUP
    conf = SparkConf().setAppName('TriangleCount')
    sc = SparkContext(conf=conf)
    ## Sys for debug
    C = 1
    data_path = "/home/fd/repo/BigDataComputing2023/data/facebook_small.txt"
    # INPUT READING
    C = int(C)

	# 2. Read input file and subdivide it into C random partitions
    assert os.path.isfile(data_path), "File or folder not found"
    graph = sc.textFile(data_path,minPartitions=C).cache()
    graph.repartition(numPartitions=C)
    edges = graph.map(lambda x: tuple(map(int, x.split(","))))
	# SETTING GLOBAL VARIABLES

    
    # Algorithm 1 -- MR_ApproxTCwithNodeColors
    numTriangles_counts = []
    for i in range(5):
        numTriangles_counts.append(sum(MR_ApproxTCwithNodeColors(edges, C)))

    print("Sum of the triangle in the graph:", (C**2)*sum(numTriangles_counts))

    
    ## Output SECTION
    # Dataset = facebook_small.txt
    print("Dataset = ",os.path.basename(data_path).split('/')[-1])
    # Number of Edges = 88234
    numEdges = edges.count()
    print("Number of Edges = ", numEdges)
    # Number of Colors = 2
    numColors = C
    print("Number of Colors = ", numColors)
    # Number of Repetitions = 5
    numRepetitions = 
    print("Number of Repetitions = ", numRepetitions)
    # Approximation through node coloring
    # - Number of triangles (median over 5 runs) = 1595308
    numTriangles_spark = (C**2)*sum(numTriangles3) # Example value, replace with actual number of triangles
    print("Approximation through Spark partitions")
    print("- Number of triangles = {}".format(numTriangles_spark))
    # - Running time (average over 5 runs) = 481 ms
    # Approximation through Spark partitions
    # - Number of triangles = 1587400
    # - Running time = 140 ms

if __name__ == "__main__":
    main()