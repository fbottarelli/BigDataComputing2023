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
        # i=0
        u, v = edge
        # if i <=10:
        #     print(u)      
        #     print(v)    
        neighbors[u].add(v)
        neighbors[v].add(u)
        # i = i+1

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
                    .mapValues(CountTriangles) # mapValues because it must applied only on values of key-value pairs
                    .values() # extract the values from the pairs
                    .collect()) # collect to return only the list from the RDD) !!!!! Warning: this action needs enough memory on the driver to store all data in X, hence it must be used on when the RDD is sufficiently small, i think is ok because the RDD are already the partinioned.
    t_final = ((C**2)*sum(edgesColors))
    return t_final

# Algorithm 2
# def MR_ApproxTCwithSparkPartitions(E, C):
#     edgesColors = (E.mapPartitions(CountTriangles)
#                 #    .groupByKey()
#                 #    .mapPartitions(CountTriangles)
#                 #    .mapValues(lambda vals: sum(vals))
#                    .values()
#                 #    .collect()
#                    )
#     # t_final = edgesColors
#     t_final = ((C**2)*sum(edgesColors))
#     return t_final



def main():
    assert len(sys.argv) == 4, "Usage: python TriangleCount <C> <R> <file-name>" 
    # SPARK SETUP
    conf = SparkConf().setAppName('TriangleCount')
    sc = SparkContext(conf=conf)

    ## Sys for debug
    # C = 1
    # R = 1
    # data_path = "/home/fd/repo/BigDataComputing2023/data/facebook_large.txt"

    # INPUT READING
    # 1. Read number of partitions
    C = sys.argv[1]
    assert C.isdigit(), "K must be an integer"
    C = int(C)
    # 2. number of iteration of the loop
    R = sys.argv[2]
    assert R.isdigit(), "K must be an integer"
    R = int(R)
	# 3. Read input file and subdivide it into C random partitions
    data_path = sys.argv[3]
    assert os.path.isfile(data_path), "File or folder not found"
    rawData = sc.textFile(data_path,minPartitions=C).cache()
    edges = rawData.map(lambda x: tuple(map(int, x.split(","))))
    
    # Loop to calculate R times MR_ApproxTCwithNodeColors
    t_final_colors = []
    runtimes = []
    for i in range(R):
        start_time = time.time()
        t_final_colors.append(MR_ApproxTCwithNodeColors(edges, C))
        end_time = time.time()
        runtimes.append(end_time - start_time)
    
    # edges = rawData.map(lambda x: tuple(map(int, x.split(","))))
    # edges.repartition(numPartitions=C)
    # print(edges.take(3))
    # print(edges.getNumPartitions())

    # Algorithm 2
    # start_time = time.time()
    # t_final_part = MR_ApproxTCwithSparkPartitions(edges, C)
    # end_time = time.time()
    # runtime_part = (end_time - start_time)

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
    print("- Number of triangles  =", statistics.median(t_final_colors))
    # - Running time (average over 5 runs) = 481 ms
    avg_runtime = sum(runtimes) / len(runtimes)
    print("- Running time (average over 5 runs) = {} ms".format(avg_runtime*1000))
    # Algorithm 2 -- Output
    # print("Approximation through Spark partitions")
    # # - Number of triangles = 1587400
    # print("Number of triangles = ", t_final_part)
    # # - Running time = 140 ms
    # print("Running time = {} ms".format(runtime_part*1000))


if __name__ == "__main__":
    main()