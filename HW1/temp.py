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


1. I have this edges data, of couse much longer: 
```
0,1
0,2
0,3
0,4
0,5
```

3. My code is:
```
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
```

3. It used to resolve this problem:
The purpose of this first homework is to get acquainted with Spark and with its use to implement MapReduce algorithms. In preparation for the homework, you must set up your environment following the instructions given in Moodle Exam, in the same section as this page. After the set up is complete, test it using the WordCountExample program (Java or Python), and familiarize with the Spark methods it uses. The Introduction to Programming in Spark may turn out useful to this purpose.

Undirected graph with trianglesTRIANGLE COUNTING. In the homework you must implement and test in Spark two MapReduce algorithms to count the number of distinct triangles in an undirected graph G=(V,E)
, where a triangle is defined by 3 vertices u,v,w
 in V
, such that (u,v),(v,w),(w,u)
 are in E
. In the right image, you find an example of a 5-node graph with 3 triangles. Triangle counting is a popular primitive in social network analysis, where it is used to detect communities and measure the cohesiveness of those communities. It has also been used is different other scenarios, for instance: detecting web spam (the distributions of local triangle frequency of spam hosts significantly differ from those of the non-spam hosts), uncovering the hidden thematic structure in the World Wide Web (connected regions of the web which are dense in triangles represents a common topic), query plan optimization in databases (triangle counting can be used for estimating the size of some joins).

Both algorithms use an integer parameter C≥1
, which is used to partition the data.

ALGORITHM 1: Define a hash function hC
 which maps each vertex u
 in V
 into a color hC(u)
 in [0,C−1]
. To this purpose, we advise you to use the hash function

hC(u)=((a⋅u+b)modp)modC


where p=8191
 (which is prime), a
 is a random integer in [1,p−1]
, and b
 is a random integer in [0,p−1]
.

Round 1:
Create C
 subsets of edges, where, for 0≤i<C
, the i-th subset, E(i)
 consist of all edges (u,v)
 of E
 such that hC(u)=hC(v)=i
. Note that if the two endpoints of an edge have different colors, the edge does not belong to any E(i)
 and will be ignored by the algorithm.
Compute the number t(i)
 triangles formed by edges of E(i)
, separately for each 0≤i<C
.
Round 2: Compute and return tfinal=C2∑0≤i<Ct(i)
 as final estimate of the number of triangles in G
.

In the homework you must develop an implementation of this algorithm as a method/function MR_ApproxTCwithNodeColors. (More details below.)
SEQUENTIAL CODE for TRIANGLE COUNTING. Both algorithms above require (in Round 1) to compute the number of triangles formed by edges in the subsets E(i)
's. To this purpose you can use the methods/functions provided in the following files: CountTriangles.java (Java) and CountTriangles.py (Python).

DATA FORMAT. To implement the algorithms assume that the vertices (set V
) are represented as 32-bit integers (i.e., type Integer in Java), and that the graph G
 is given in input as the set of edges E
 stored in a file. Each row of the file contains one edge stored as two integers (the edge's endpoints) separated by comma (','). Each edge of E
 appears exactly once in the file and E
 does not contain multiple copies of the same edge.

TASK for HW1:

1) Write the method/function MR_ApproxTCwithNodeColors which implements ALGORITHM 1. Specifically, MR_ApproxTCwithNodeColors must take as input an RDD of edges and a number of colors C
 and must return an estimate tfinal
 of the number of triangles formed by the input edges computed through transformations of the input RDD, as specified by the algorithm. It is important that the local space required by the algorithm be proportional to the size of the largest subset E(i)
 (hence, you cannot download the whole graph into a local data structure). Hint: define the hash function hC
 inside MR_ApproxTCwithNodeColors, but before starting processing the RDD, so that all transformations of the RDD will use the same hash function, but different runs of MR_ApproxTCwithNodeColors will use different hash functions (i.e., defined by different values of a and b).

 4. it gives this error:   File "/home/fd/.local/lib/python3.10/site-packages/pyspark/rdd.py", line 2554, in combineLocally
    merger.mergeValues(iterator)
  File "/home/fd/.local/lib/python3.10/site-packages/pyspark/python/lib/pyspark.zip/pyspark/shuffle.py", line 253, in mergeValues
    for k, v in iterator:
  File "/home/fd/.local/lib/python3.10/site-packages/pyspark/python/lib/pyspark.zip/pyspark/util.py", line 81, in wrapper
    return f(*args, **kwargs)
  File "/home/fd/repo/BigDataComputing2023/HW1/TriangleCount.py", line 40, in CountTriangles
    u, v = edge
TypeError: cannot unpack non-iterable int object