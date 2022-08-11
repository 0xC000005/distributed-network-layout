# import packages
from __future__ import print_function
import pyspark
import math
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import ArrayType
from pyspark.sql.types import StringType
import os
import shutil
import sys
import timeit
import networkx as nx
import matplotlib as mpl

mpl.use('Agg')
import matplotlib.pyplot as plt

from graphframes import GraphFrame
from graphframes.lib import AggregateMessages as AM


# Function to calculate the displacement on source`s x axis due to dest attractive force
# @para it take 4 parameter. x,y attributes from Src node and x,y attribute from Dst node
# @return the displacement on the source`s x axis due to destinations attractive force: DoubleType()
def aDispSrc(node1, node2):
    # Constant to calculate attractive and repulsive force
    K = math.sqrt(1 / nNodes)
    dx = (node2[0] - node1[0])
    dy = (node2[1] - node1[1])

    distance = math.sqrt(dx ** 2 + dy ** 2)

    if distance == 0:
        return [0, 0, 0]

    if distance < 0.01:
        distance = 0.01

    aForce = distance / K
    dispX = dx / distance
    dispY = dy / distance
    aDispX = dispX * aForce
    aDispY = dispY * aForce
    xy = [aDispX, aDispY, 1.0]
    return xy


aDispSrc = F.udf(aDispSrc, ArrayType(DoubleType()))


# Function to calculate the displacement on dst`s x axis due to dest attractive force
# @para it take 4 parameter. x,y attributes from Src node and x,y attribute from Dst node
# @return the displacement on the dst`s x axis due to destinations attractive force: DoubleType()
def aDispDst(node1, node2):
    K = math.sqrt(1 / nNodes)
    dx = (node2[0] - node1[0])
    dy = (node2[1] - node1[1])

    distance = math.sqrt(dx ** 2 + dy ** 2)

    if distance == 0:
        return [0, 0, 0]

    if distance < 0.01:
        distance = 0.01

    aForce = distance / K
    dispX = dx / distance
    dispY = dy / distance
    aDispX = dispX * aForce
    aDispY = dispY * aForce
    xy = [-aDispX, -aDispY, 1.0]
    return xy


aDispDst = F.udf(aDispDst, ArrayType(DoubleType()))

# Function to calculate the associated centroid and it distance to the vertex 
# @para it takes source vertex x,y co-ordiates as input parameter
# @return the associated centroid and its distances from the vertex
centroidVertexAssociationUdf = F.udf(lambda z: centroidVertexAssociation(z), ArrayType(DoubleType()))


def centroidVertexAssociation(vertexCord):
    centroidsValue = centroidBroadcast.value
    centroidLength = len(centroidsValue)
    centroidDistance = 0.0
    centroidAssociated = 0.0
    for i in range(centroidLength):
        dx = (vertexCord[0] - centroidsValue[i][1][0])
        dy = (vertexCord[1] - centroidsValue[i][1][1])
        distance = math.sqrt(dx ** 2 + dy ** 2)
        if i == 0:
            centroidDistance = distance
            centroidAssociated = centroidsValue[i][0]
        if distance < centroidDistance:
            centroidAssociated = centroidsValue[i][0]
            centroidDistance = distance
    return [centroidAssociated, centroidDistance]


# Function to calculate the dispalcement on vertices due to centroids repulsive force
# @para it takes vertex x,y co-ordiates as input parameter
# @return the dsisplacement of vertex position due to centroids repulsive force
def rForceCentroid(vertexCord):
    K = math.sqrt(1 / nNodes)
    centroidLength = len(centroid_list)
    weight = 1
    dx = 0.0
    dy = 0.0
    distance = 0.0
    rDispX = 0.0
    rDispY = 0.0
    if centroidLength > 0:
        for i in range(centroidLength):
            dx = (vertexCord[0] - centroid_list[i][0])
            dy = (vertexCord[1] - centroid_list[i][1])
            distance = math.sqrt(dx ** 2 + dy ** 2)
            if distance != 0:
                if distance < 0.01:
                    distance = 0.01
                rForce = -(K / distance ** 2) * weight
                dispX = dx / distance
                dispY = dy / distance
                rDispX = rDispX + dispX * rForce
                rDispY = rDispY + dispY * rForce
    return [rDispX, rDispY]


rForceCentroid = F.udf(rForceCentroid, ArrayType(DoubleType()))


# Function to calculate the dispalcement on vertices due to center repulsive force
# @para it takes vertex x,y co-ordiates as input parameter
# @return the dsisplacement of vertex position due to center repulsive force
def rForceCenter(vertexCord):
    K = math.sqrt(1 / nNodes)
    weight = 1
    centerValue = centerBroadcast.value
    dx = (vertexCord[0] - centerValue[0])
    dy = (vertexCord[1] - centerValue[1])
    distance = math.sqrt(dx ** 2 + dy ** 2)
    if distance == 0:
        return [0, 0]
    if distance < 0.01:
        distance = 0.01
    rForce = -(K / distance ** 2) * weight
    dispX = dx / distance
    dispY = dy / distance
    rDispX = dispX * rForce
    rDispY = dispY * rForce
    return [rDispX, rDispY]


rForceCenter = F.udf(rForceCenter, ArrayType(DoubleType()))


# Funtion to scale the vertice degree centrality
# @Param: degree of vertex and maxDegree in the graph
# @return: the scaled degree between 0 to 5
def scale_degree(degree, maxDegree, minDegree=1, mi=0, ma=5, log=False, power=1):
    r"""Convert property map values to be more useful as a vertex size, or edge
    width. The new values are taken to be

    .. math::

        y_i = mi + (ma - mi) \left(\frac{x_i - \min(x)} {\max(x) - \min(x)}\right)^\text{power}

    If ``log=True``, :math:`x_i` is replaced with :math:`\ln(x_i)`.

    If :math:`\max(x) - \min(x)` is zero, :math:`y_i = mi`.

    """
    delta = (maxDegree) - minDegree
    if delta == 0:
        delta = 1
    prop = mi + (ma - mi) * ((degree - minDegree) / delta) ** power
    return prop


scale_degree = F.udf(scale_degree, DoubleType())

if __name__ == "__main__":
    startTime = timeit.default_timer()
    # save file arguments to variables
    inputPath = sys.argv[1]
    outputPath = sys.argv[2]
    numIteration = int(sys.argv[3])
    name = os.path.basename(inputPath).split(".")[0]

    # create spark context
    spark = pyspark.sql.SparkSession.builder \
        .appName(name) \
        .getOrCreate()

    sc = pyspark.SparkContext.getOrCreate()
    sqlContext = pyspark.SQLContext.getOrCreate(sc)
    sc.setLogLevel("ERROR")

    checkpintDir = outputPath + "checkpoint"

    try:
        if os.path.exists(checkpintDir):
            print("Checkpoint directory already exist")
        else:
            os.mkdir(checkpintDir)
            print("Successfully created the directory %s " % checkpintDir)
    except OSError:
        print("Creation of the directory %s failed" % checkpintDir)

    sc.setCheckpointDir(checkpintDir)

    #     startTime = timeit.default_timer()
    InitialNumPartitions = sc.defaultParallelism

    # load input edge file 
    # edges = spark.read.csv(inputPath,sep = "\t",comment='#',header=None)
    print("Start reading the file")
    edges = spark.read.parquet(inputPath)
    print("File reading complete")

    edges = edges.withColumnRenamed("_c0", "src") \
        .withColumnRenamed("_c1", "dst")
    edgesCheckpoint = edges.checkpoint()
    edgesCheckpoint.count()

    print("the number of partitions in edges df are")
    numPartitions = edges.rdd.getNumPartitions()
    print(numPartitions)

    # Extract nodes from the edge list dataframe
    vA = edgesCheckpoint.select(F.col('src')).drop_duplicates() \
        .withColumnRenamed('src', 'id')
    print(vA.rdd.getNumPartitions())
    print("number of unique verticex in src column: {}".format(vA.count()))

    vB = edgesCheckpoint.select(F.col('dst')).drop_duplicates() \
        .withColumnRenamed('dst', 'id')
    print(vB.rdd.getNumPartitions())
    print("number of unique verticex in dst column: {}".format(vB.count()))
    vF1 = vA.union(vB).distinct()

    nodesCheckpoint = vF1.persist(pyspark.StorageLevel.MEMORY_AND_DISK_2)
    nodesCheckpoint.count()
    print("the number of partitions in vF df are")
    print(nodesCheckpoint.rdd.getNumPartitions())
    print("Num of nodes: {}".format(nodesCheckpoint.count()))
    print("Num of edges: {}".format(edgesCheckpoint.count()))

    independentSet = []
    graphs = dict()
    metaGraph = dict()
    metaGraphlayout = dict()
    metaEdgeCord = dict()
    completeGraphLayout = dict()

    # initialize index and graphs dictionary
    i = 0
    # create GraphFrame object using nodes and edges dataframe
    graphs[i] = GraphFrame(nodesCheckpoint, edgesCheckpoint)
    currentNodes = graphs[i].vertices

    # number of nodes in the first filtration level 
    currentNodesCounts = currentNodes.count()

    currentEdges = graphs[i].edges
    currentEdgesCounts = currentEdges.count()

    # variable to stop the while loop of the selected nodes of next level of filtration is less than 3
    currentSelectedNodes = currentNodes.count()

    numOfVertices = graphs[i].vertices.count()
    i = i + 1
    k = 1
    t = 0.1

    vertices = nodesCheckpoint
    edges = edgesCheckpoint
    nNodes = vertices.count()

    numberOfCentroids = round(nNodes / 2)

    # Initialize the vertices with x,y with random values and dispx,dispy with 0
    verticeWithCord = vertices.withColumn("xy", F.array(F.rand(seed=1) * F.lit(3), F.rand(seed=0) * F.lit(3))) \
        .checkpoint()

    # cool-down amount
    dt = t / (numIteration + 1)

    # calculate the center repulsive force for given iteration 
    for p in range(numIteration):
        # calculate centroids
        centroids = verticeWithCord.sample(withReplacement=False, fraction=(numberOfCentroids / nNodes), seed=1)
        centroid_list = centroids.select("xy").rdd.flatMap(lambda x: x).collect()

        # calculate centroids repulsive force
        vCentroid = verticeWithCord.withColumn("dispCentroidXY", rForceCentroid("xy")).cache()

        vCentroid.count()

        # find the center of the network
        if nNodes > 0:
            x = centroids.agg(F.avg(F.col("xy")[0]).alias("centerX")).collect()
            y = centroids.agg(F.avg(F.col("xy")[0]).alias("centerX")).collect()
            center = [x[0][0], y[0][0]]
        else:
            center = [0, 0]

        centerBroadcast = sc.broadcast(center)

        # calculate center repulsive force
        vCenter = verticeWithCord.withColumn("dispCenterXY", rForceCenter("xy")).select("id", "xy",
                                                                                        "dispCenterXY").cache()
        vCenter.count()

        centerBroadcast.unpersist()

        # calculate total repulsive forece displacement
        newVertices = vCentroid.join(vCenter, on="id") \
            .drop(vCentroid.xy) \
            .withColumn("dispX", (F.col("dispCentroidXY")[0] + F.col("dispCenterXY")[0])) \
            .withColumn("dispY", (F.col("dispCentroidXY")[1] + F.col("dispCenterXY")[1])) \
            .cache()

        vCentroid.unpersist()
        vCenter.unpersist()
        #         print("rForce is calculated")
        gfA = GraphFrame(verticeWithCord, edges)  # .cache()

        # messages send to source and destination vertices to calculate displacement on node due to attractive force
        msgToSrc = aDispSrc(AM.src['xy'], AM.dst['xy'])
        msgToDst = aDispDst(AM.src['xy'], AM.dst['xy'])

        # AM fucntion to calculate displacement on node due to attractive force
        # @para Sum all the messages on the nodes,sendToSrc adn SendToDst messages
        # @return Dataframe with attribute Id, and displacementX
        aAgg = gfA.aggregateMessages(
            F.array((F.sum(AM.msg.getItem(0))).alias("x"), (F.sum(AM.msg.getItem(1))).alias("y")).alias("aDispXY"),
            sendToSrc=msgToSrc,  # )
            sendToDst=msgToDst)

        cachedAAgg = AM.getCachedDataFrame(aAgg)
        #         print("aForce is calculated")

        # Calculate total displacement from all forces
        newVertices2 = newVertices.join((cachedAAgg), on=(newVertices['id'] == cachedAAgg['id']), how='left_outer') \
            .drop(cachedAAgg['id']) \
            .withColumn('newDispColX', F.when(cachedAAgg['adispXY'][0].isNotNull(),
                                              (cachedAAgg['adispXY'][0] + newVertices['dispX'])).otherwise(
            newVertices['dispX'])) \
            .withColumn('newDispColY', F.when(cachedAAgg['adispXY'][1].isNotNull(),
                                              (cachedAAgg['adispXY'][1] + newVertices['dispY'])).otherwise(
            newVertices['dispY'])) \
            .cache()

        newVertices.unpersist()
        # Update the vertices position
        updatedVertices = newVertices2.withColumn("length",
                                                  F.sqrt(F.col('newDispColX') ** 2 + F.col('newDispColY') ** 2)) \
            .withColumn('newDispX',
                        (F.col('newDispColX') / F.col('length')) * F.least(F.abs(F.col('length')), F.lit(t))) \
            .withColumn('newDispY',
                        (F.col('newDispColY') / F.col('length')) * F.least(F.abs(F.col('length')), F.lit(t))) \
            .withColumn('newXY', F.array((F.col('xy')[0] + F.col('newDispX')), (F.col('xy')[1] + F.col('newDispY')))) \
            .drop("xy", "dispCentroidXY", "dispCenterXY", "dispX", "dispY", "aDispXY", "newDispColX", "newDispColY",
                  "length", "newDispX", "newDispY") \
            .withColumnRenamed("newXY", "xy").checkpoint(True)

        newVertices2.unpersist()

        verticeWithCord = updatedVertices
        cachedAAgg.unpersist()

        print("{} Iterations are completed".format(p, k))
        t -= dt
    updatedV = verticeWithCord.select("id", "xy")

    graph = GraphFrame(updatedV, edges)
    VerticesInDegrees = graph.inDegrees
    VerticesOutDegrees = graph.outDegrees
    veticesFinal = updatedV.join(VerticesInDegrees, on="id", how="left").na.fill(value=1).join(VerticesOutDegrees,
                                                                                               on="id",
                                                                                               how="left").na.fill(
        value=1)
    maxInDegree = veticesFinal.orderBy(F.col("inDegree").desc()).take(1)[0][2]
    maxOutDegree = veticesFinal.orderBy(F.col("outDegree").desc()).take(1)[0][2]
    vertices_scaled_degree = veticesFinal.withColumn("scaled_inDegree",
                                                     scale_degree("inDegree", F.lit(maxInDegree))).withColumn(
        "scaled_outDegree", scale_degree("outDegree", F.lit(maxOutDegree)))
    time5 = timeit.default_timer() - startTime
    print("time taken for layout of combined levels = {}".format(time5))

    vList = vertices_scaled_degree.select("id").rdd.flatMap(lambda x: x).collect()
    print("list of nodes are collected")

    vListPos = vertices_scaled_degree.select("xy").rdd.flatMap(lambda x: x).collect()
    print("list of nodes positions are collected")

    vlistDegree = vertices_scaled_degree.select("scaled_inDegree").rdd.flatMap(lambda x: x).collect()
    print("list of nodes degree are collected")

    degree = dict(zip(vList, vlistDegree))
    pos = dict(zip(vList, vListPos))
    print("dict of nodes and their positions are created")

    nxGraphLayout = nx.from_pandas_edgelist(edges.toPandas(), source="src", target="dst")
    print("networkx graph object is created")

    # plot the nextworkX graph and save it to output path
    a = nx.draw(nxGraphLayout, pos, node_size=vlistDegree, width=0.1)
    print("networkx graph using distribute layout is created")

    plt.title("{name}_{numIteration}_Iterations layout".format(name=name, numIteration=numIteration))
    plt.savefig("{outputPath}{name}_{numIteration}_Iterations.png".format(outputPath=outputPath, name=name,
                                                                          numIteration=numIteration), dpi=1000,
                bbox_inches='tight')
    print("graph is saved to the disk")
    print("Num of nodes: {}".format(updatedV.count()))
    print("Num of edges: {}".format(edges.count()))

    # Save the calculated position of the graph to output path
    updatedV.coalesce(10).write.mode("overwrite").parquet(
        "{outputPath}{name}_{numIteration}_Iterations_updatedVertices.parquet".format(outputPath=outputPath, name=name,
                                                                                      numIteration=numIteration))
    edges.coalesce(10).write.mode("overwrite").parquet(
        "{outputPath}{name}_{numIteration}_Iterations_edges.parquet".format(outputPath=outputPath, name=name,
                                                                            numIteration=numIteration))
    print("Nodes and Edges dataframes saved to disk")

    # remove the checkpoint dir
    try:
        if os.path.exists(checkpintDir):
            shutil.rmtree(checkpintDir)
            print("Successfully deleted the directory %s " % checkpintDir)
        else:
            print("Directory does not exist: %s " % checkpintDir)
    except OSError:
        print("Directory does not exist: %s " % checkpintDir)
