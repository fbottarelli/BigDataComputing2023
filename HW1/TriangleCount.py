from pyspark import SparkContext, SparkConf
import sys
import os
import random as rand
from collections import defaultdict

# def colorPartitioning(V_i,C):
#     edges = []
#     # for edge in V:
#     u = int(V_i[0])
#     print(u)
#     u_color = hashFunction(u,C)
#     v = int(V_i[1])
#     v_color = hashFunction(v,C)
#     if u_color != v_color:
#         return
#     else:
#         color = u_color
#         edges.append((color,(u,v)))
#     return edges

# def colorPartioning2(V,C):
#     edges_color = {}
    

#     # MR_ApproxTCwithNodeColors to implement algh. 1
# def MR_ApproxTCwithNodeColors(V,C):
#     # is necessary to partition here the data or are already partitioned?
#     edgesColors = (V.flatMap(lambda x: colorPartitioning(x,C))
#                     .groupByKey()
#                     .flatMap(CountTriangles)
#                     .mapValues(lambda vals: sum(vals)))
#     # At the end must return the triangle's count
#     return edgesColors

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
        .flatMap(CountTriangles)
        .reduceByKey(lambda x, y: x + y) )
        # .values() 
        # .sum())
    return edgesColors.sum()


def main():
    # assert len(sys.argv) == 3, "Usage: python TriangleCount <C> <file-name>" # !!!!!!!!!!!1
    # SPARK SETUP
    conf = SparkConf().setAppName('TriangleCount')
    sc = SparkContext(conf=conf)

    ## Sys for debug
    C = 4
    data_path = "/home/fd/repo/BigDataComputing2023/data/facebook_small.txt"

    # INPUT READING
	# 1. Read number of partitions
    # C = sys.argv[1] # !!!!!!!!!!!!!!!!!
    # assert C.isdigit(), "C must be an integer"
    C = int(C)

	# 2. Read input file and subdivide it into C random partitions
    # data_path = sys.argv[2] # !!!!!!!!!!!!!!!!
    assert os.path.isfile(data_path), "File or folder not found"
    graph = sc.textFile(data_path,minPartitions=C).cache()
    graph.repartition(numPartitions=C)
    edges = graph.map(lambda x: tuple(map(int, x.split(","))))
    print(edges.take(3)[0][0])
	# SETTING GLOBAL VARIABLES
    numEdges = edges.count()
    print("Number of Edges in the graph = ", numEdges)
    
    # Algorithm 1 -- MR_ApproxTCwithNodeColors
    numTriangles = MR_ApproxTCwithNodeColors(edges, C)
    print("Number of unique triangles in the graph:", numTriangles)

if __name__ == "__main__":
    main()