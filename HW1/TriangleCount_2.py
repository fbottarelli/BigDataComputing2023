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


def MR_ApproxTCwithNodeColors1(V, C):
    edgesColors = (V.flatMap(lambda edge: colorPartitioning(edge, C)))
    print("flatMap Done")
        # .values() 
        # .sum())
    return edgesColors

def MR_ApproxTCwithNodeColors2(V, C):
    edgesColors = (V.groupByKey())
    edgesColorsList=(edgesColors.take(4))
    print(len(edgesColorsList[0][1]) +len(edgesColorsList[1][1]) + len(edgesColorsList[2][1]) + len(edgesColorsList[3][1])) 
        # .values() 
        # .sum())
    return edgesColors

def MR_ApproxTCwithNodeColors3(V, C):
    edgesColors = (V.mapValues(CountTriangles))
    # edgeColorsList = list(edgesColors)
        # .values() 
        # .sum())
    return edgesColors

# def MR_ApproxTCwithNodeColors4(V, C):
#     edgesColors = (V.mapV(lambda x, y: x + y))
#     edgesColors = (V.reduceByKey(lambda x, y: x + y))
#         # .values() 
#         # .sum())
#     return edgesColors


def main():
    # assert len(sys.argv) == 3, "Usage: python TriangleCount <C> <file-name>" # !!!!!!!!!!!1
    # SPARK SETUP
    conf = SparkConf().setAppName('TriangleCount')
    sc = SparkContext(conf=conf)
    ## Sys for debug
    C = 4
    data_path = "/home/fd/repo/BigDataComputing2023/data/facebook_small.txt"
    # INPUT READING
    C = int(C)

	# 2. Read input file and subdivide it into C random partitions
    assert os.path.isfile(data_path), "File or folder not found"
    graph = sc.textFile(data_path,minPartitions=C).cache()
    graph.repartition(numPartitions=C)
    edges = graph.map(lambda x: tuple(map(int, x.split(","))))
    print(edges.take(3)[0][0])
	# SETTING GLOBAL VARIABLES
    numEdges = edges.count()
    print("Number of Edges in the graph = ", numEdges)
    
    # Algorithm 1 -- MR_ApproxTCwithNodeColors
    numTriangles1 = MR_ApproxTCwithNodeColors1(edges, C)
    print("result after flatMap:", numTriangles1.take(18))
    numTriangles2 = MR_ApproxTCwithNodeColors2(numTriangles1,C)
    print("result after groubykey:", numTriangles2.take(10))
    numTriangles3 = MR_ApproxTCwithNodeColors3(numTriangles2,C)
    
    numTriangles_values = numTriangles3.values()
    numTriangles4 = numTriangles_values.collect()
    print("result after flatMap:", sum(numTriangles4))
    # numTriangles4 = MR_ApproxTCwithNodeColors4(numTriangles3,C)
    # print("result after reducedByKey:", numTriangles4.take(5))

if __name__ == "__main__":
    main()