# import packages
import pyspark
import math
from pyspark.sql import functions as Func
from pyspark.sql.types import DoubleType
from pyspark.sql.types import ArrayType
import os
import shutil
import sys
import timeit
import networkx as nx
import matplotlib as mpl
import matplotlib.pyplot as plt
from graphframes import GraphFrame
from graphframes.lib import AggregateMessages as AM
# from numba import jit

mpl.use('Agg')


# Function to calculate the associated centroid, and it distances to the vertex
# @para it takes source vertex x,y co-ordinates as input parameter and the centroidBroadcast
# @return the associated centroid and its distances from the vertex
# @jit(cache=True)
def centroidVertexAssociation(vertexCord, centroidBroadcast):
    # TODO: fix centroidBroadcast not pre-defined issue (ERR LEVEL ISSUE!!!)
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


centroidVertexAssociationUdf = Func.udf(lambda z: centroidVertexAssociation(z, centerBroadcast),
                                        ArrayType(DoubleType()))


# Function to calculate the displacement on sources' x,y-axis due to dest attractive force
# @para it takes 4 parameter. x,y attributes from Src node and x,y attribute from Dst node
# @return the displacement on the source`s x-axis due to destinations attractive force: DoubleType()
# @jit(cache=True)
def aDispSrc(src, dst):
    # Constant to calculate attractive and repulsive force
    K = math.sqrt(1 / nVertices)
    dx = (dst[0] - src[0])
    dy = (dst[1] - src[1])

    distance = math.sqrt(dx ** 2 + dy ** 2)

    if distance == 0:
        return [0, 0, 0]

    # Minimum distance is set to be 0.01 to prevent close nodes from overlapping
    # TODO: add an arg for users to customize minimum distance
    if distance < 0.01:
        distance = 0.01

    attractiveForce = distance / K
    dispX = dx / distance
    dispY = dy / distance
    aDispX = dispX * attractiveForce
    aDispY = dispY * attractiveForce

    # TODO: where the 1.0 comes from?
    dispXY = [aDispX, aDispY, 1.0]
    return dispXY


aDispSrc = Func.udf(aDispSrc, ArrayType(DoubleType()))


# Function to calculate the displacement on dst`s x-axis due to dest attractive force
# @para it takes 4 parameter. x,y attributes from Src node and x,y attribute from Dst node
# @return the displacement on the dst`s x-axis due to destinations attractive force: DoubleType()
# @jit(cache=True)
def aDispDst(node1, node2):
    K = math.sqrt(1 / nVertices)
    dx = (node2[0] - node1[0])
    dy = (node2[1] - node1[1])

    distance = math.sqrt(dx ** 2 + dy ** 2)

    if distance == 0:
        return [0, 0, 0]

    if distance < 0.01:
        distance = 0.01

    attractiveForce = distance / K
    dispX = dx / distance
    dispY = dy / distance
    aDispX = dispX * attractiveForce
    aDispY = dispY * attractiveForce
    xy = [-aDispX, -aDispY, 1.0]
    return xy


aDispDst = Func.udf(aDispDst, ArrayType(DoubleType()))


# Function to calculate the displacement on vertices due to centroids repulsive force
# @para it takes vertex x,y co-ordinates as input parameter
# @return the displacement of vertex position due to centroids repulsive force
# @jit(cache=True)
def rForceCentroid(vertexCord):
    K = math.sqrt(1 / nVertices)
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


rForceCentroid = Func.udf(rForceCentroid, ArrayType(DoubleType()))


# Function to calculate the displacement on vertices due to center repulsive force
# @para it takes vertex x,y co-ordinates as input parameter
# @return the displacement of vertex position due to center repulsive force
# @jit(cache=True)
def rForceCenter(vertexCord):
    K = math.sqrt(1 / nVertices)
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


rForceCenter = Func.udf(rForceCenter, ArrayType(DoubleType()))


# Function to scale the vertices degree centrality
# @Param: degree of vertex and maxDegree in the graph
# @return: the scaled degree between 0 and 5
# @jit(cache=True)
def scale_degree(degree, maxDegree, minDegree=1, mi=0, ma=5, log=False, power=1):
    r"""Convert property map values to be more useful as a vertex size, or edge
    width. The new values are taken to be

    .. math::

        y_i = mi + (ma - mi) \left(\frac{x_i - \min(x)} {\max(x) - \min(x)}\right)^\text{power}

    If ``log=True``, :math:`x_i` is replaced with :math:`\ln(x_i)`.

    If :math:`\max(x) - \min(x)` is zero, :math:`y_i = mi`.

    """
    delta = maxDegree - minDegree
    if delta == 0:
        delta = 1
    prop = mi + (ma - mi) * ((degree - minDegree) / delta) ** power
    return prop


scale_degree = Func.udf(scale_degree, DoubleType())

if __name__ == "__main__":
    startTime = timeit.default_timer()

    # save file arguments to variables
    inputPath = sys.argv[1]
    outputPath = sys.argv[2]
    numIteration = int(sys.argv[3])
    name = os.path.basename(inputPath).split(".")[0]

    # create spark context
    spark = pyspark.sql.SparkSession.builder.appName(name).getOrCreate()
    sc = pyspark.SparkContext.getOrCreate()
    sqlContext = pyspark.SQLContext.getOrCreate(sc)
    sc.setLogLevel("ERROR")

    # Mkdir place for storing the checkpoints of the nodes and edges' dataframe
    # TODO: how is this works distributively?? hoping on every machine the following path is available? or shuffling data to one location on one machine?
    checkpointDir = outputPath + "checkpoint"

    # try:
    #     if os.path.exists(checkpointDir):
    #         print("Checkpoint directory already exist")
    #     else:
    #         os.mkdir(checkpointDir)
    #         print("Successfully created the directory %s " % checkpointDir)
    # except OSError:
    #     print("Creation of the directory %s failed" % checkpointDir)

    if os.path.exists(checkpointDir):
        print("Checkpoint directory already exist")
    else:
        os.mkdir(checkpointDir)
        print("Successfully created the directory %s " % checkpointDir)

    sc.setCheckpointDir(checkpointDir)

    # startTime = timeit.default_timer()
    InitialNumPartitions = sc.defaultParallelism

    # load input edge file, the file consists of 2 rows, the source node ID (whoever send the message) and the
    # destination node ID (whoever received the message)
    edgesCheckpoint = spark.read.csv(inputPath, sep="\t", comment='#', header=None)
    edgesCheckpoint = edgesCheckpoint.withColumnRenamed("_c0", "src").withColumnRenamed("_c1", "dst")

    # Save a checkpoint of the edge dataframe
    edgesCheckpoint = edgesCheckpoint.checkpoint()
    # TODO: since the checkpoint is eager, why followed by the spark action to make it effective?

    # Check https://stackoverflow.com/questions/45751113/what-is-lineage-in-spark for more on why some code here is
    # followed by a count
    # TODO: every count is O(n) bruh this is expensive
    edgesCheckpoint.count()

    # print("the number of partitions in edges df are")
    # numPartitions = edges.rdd.getNumPartitions()
    # print(numPartitions)

    # Extract nodes from the edge list dataframe, drop repeated nodes. Here nodes are represented as IDs
    sources = edgesCheckpoint.select(Func.col('src')).drop_duplicates() \
        .withColumnRenamed('src', 'id')
    # print(vA.rdd.getNumPartitions())
    # print("number of unique vertices in src column: {}".format(vA.count()))

    destinations = edgesCheckpoint.select(Func.col('dst')).drop_duplicates() \
        .withColumnRenamed('dst', 'id')
    # print(vB.rdd.getNumPartitions())
    # print("number of unique vertices in dst column: {}".format(vB.count()))
    allNodes = sources.union(destinations).distinct()

    # Save nodes rdd to checkpoint
    verticesCheckpoint = allNodes.persist(pyspark.StorageLevel.MEMORY_AND_DISK_2)
    # TODO: why don't cache it? too big?
    # TODO: why only the nodes are persisting and edge is not?
    verticesCheckpoint.count()
    # print("the number of partitions in vF df are")
    # print(nodesCheckpoint.rdd.getNumPartitions())
    # print("Num of nodes: {}".format(nodesCheckpoint.count()))
    # print("Num of edges: {}".format(edgesCheckpoint.count()))

    # initialize index and graphs dictionary
    graphs = dict()
    i = 0
    # create GraphFrame object using nodes and edges dataframe
    graphs[i] = GraphFrame(verticesCheckpoint, edgesCheckpoint)

    # # number of nodes in the first filtration level
    # currentNodes = graphs[i].vertices
    # currentNodesCounts = currentNodes.count()
    #
    # currentEdges = graphs[i].edges
    # currentEdgesCounts = currentEdges.count()
    #
    # # variable to stop the while loop of the selected nodes of next level of filtration is less than 3
    # currentSelectedNodes = currentNodes.count()

    numOfVertices = graphs[i].vertices.count()
    i = i + 1
    # TODO: k is only used once in a print format emmm considering removing it
    k = 1
    t = 0.1

    nVertices = verticesCheckpoint.count()

    numberOfCentroids = round(nVertices / 2)

    # Initialize the nodes with random x,y coordinates and dispX,dispY with 0
    verticesWithCord = verticesCheckpoint.withColumn("xy", Func.array(Func.rand(seed=1) * Func.lit(3),
                                                                      Func.rand(seed=0) * Func.lit(3))).checkpoint()

    # cool-down amount
    dt = t / (numIteration + 1)

    # calculate the center repulsive force for given iteration 
    for p in range(numIteration):

        # calculate centroids
        centroids = verticesWithCord.sample(withReplacement=False, fraction=(numberOfCentroids / nVertices), seed=1)
        centroid_list = centroids.select("xy").rdd.flatMap(lambda x: x).collect()

        # calculate centroids repulsive force
        vCentroid = verticesWithCord.withColumn("dispCentroidXY", rForceCentroid("xy")).cache()
        vCentroid.count()

        # find the center of the network
        if nVertices > 0:
            x = centroids.agg(Func.avg(Func.col("xy")[0]).alias("centerX")).collect()
            y = centroids.agg(Func.avg(Func.col("xy")[0]).alias("centerX")).collect()
            center = [x[0][0], y[0][0]]
        else:
            center = [0, 0]

        centerBroadcast = sc.broadcast(center)

        # calculate center repulsive force
        vCenter = verticesWithCord.withColumn("dispCenterXY", rForceCenter("xy")).select("id", "xy",
                                                                                         "dispCenterXY").cache()
        vCenter.count()

        centerBroadcast.unpersist()

        # calculate total repulsive force displacement
        newVertices = vCentroid.join(vCenter, on="id") \
            .drop(vCentroid.xy) \
            .withColumn("dispX", (Func.col("dispCentroidXY")[0] + Func.col("dispCenterXY")[0])) \
            .withColumn("dispY", (Func.col("dispCentroidXY")[1] + Func.col("dispCenterXY")[1])) \
            .cache()

        vCentroid.unpersist()
        vCenter.unpersist()
        # print("rForce is calculated")
        gfA = GraphFrame(verticesWithCord, edgesCheckpoint)  # .cache()
        # TODO: well not using cache is because OOM, then just keep using persist I guess?

        # messages send to source and destination vertices to calculate displacement on node due to attractive force
        msgToSrc = aDispSrc(AM.src['xy'], AM.dst['xy'])
        msgToDst = aDispDst(AM.src['xy'], AM.dst['xy'])

        # AM function to calculate displacement on node due to attractive force
        # @para Sum all the messages on the nodes,sendToSrc adn SendToDst messages
        # @return Dataframe with attribute ID, and displacementX
        aAgg = gfA.aggregateMessages(
            Func.array((Func.sum(AM.msg.getItem(0))).alias("x"), (Func.sum(AM.msg.getItem(1))).alias("y")).alias(
                "aDispXY"),
            sendToSrc=msgToSrc,  # )
            sendToDst=msgToDst)

        cachedAAgg = AM.getCachedDataFrame(aAgg)
        # print("aForce is calculated")

        # Calculate total displacement from all forces
        newVertices2 = newVertices.join(cachedAAgg, on=(newVertices['id'] == cachedAAgg['id']), how='left_outer') \
            .drop(cachedAAgg['id']) \
            .withColumn('newDispColX', Func.when(cachedAAgg['adispXY'][0].isNotNull(),
                                                 (cachedAAgg['adispXY'][0] + newVertices['dispX'])).otherwise(
            newVertices['dispX'])) \
            .withColumn('newDispColY', Func.when(cachedAAgg['adispXY'][1].isNotNull(),
                                                 (cachedAAgg['adispXY'][1] + newVertices['dispY'])).otherwise(
            newVertices['dispY'])) \
            .cache()

        newVertices.unpersist()

        # Update the vertices position
        updatedVertices = newVertices2.withColumn("length",
                                                  Func.sqrt(
                                                      Func.col('newDispColX') ** 2 + Func.col('newDispColY') ** 2)) \
            .withColumn('newDispX',
                        (Func.col('newDispColX') / Func.col('length')) * Func.least(Func.abs(Func.col('length')),
                                                                                    Func.lit(t))) \
            .withColumn('newDispY',
                        (Func.col('newDispColY') / Func.col('length')) * Func.least(Func.abs(Func.col('length')),
                                                                                    Func.lit(t))) \
            .withColumn('newXY', Func.array((Func.col('xy')[0] + Func.col('newDispX')),
                                            (Func.col('xy')[1] + Func.col('newDispY')))) \
            .drop("xy", "dispCentroidXY", "dispCenterXY", "dispX", "dispY", "aDispXY", "newDispColX", "newDispColY",
                  "length", "newDispX", "newDispY") \
            .withColumnRenamed("newXY", "xy").checkpoint(True)

        newVertices2.unpersist()

        verticesWithCord = updatedVertices
        cachedAAgg.unpersist()

        print("{} Iterations are completed".format(p, k))
        t -= dt
    updatedV = verticesWithCord.select("id", "xy")

    # Plot the force-directed graph based on the final vertices and edges' positions
    graph = GraphFrame(updatedV, edgesCheckpoint)
    VerticesInDegrees = graph.inDegrees
    VerticesOutDegrees = graph.outDegrees
    verticesFinal = updatedV.join(VerticesInDegrees, on="id", how="left").na.fill(value=1).join(VerticesOutDegrees,
                                                                                                on="id",
                                                                                                how="left").na.fill(
        value=1)
    maxInDegree = verticesFinal.orderBy(Func.col("inDegree").desc()).take(1)[0][2]
    maxOutDegree = verticesFinal.orderBy(Func.col("outDegree").desc()).take(1)[0][2]
    vertices_scaled_degree = verticesFinal.withColumn("scaled_inDegree",
                                                      scale_degree("inDegree", Func.lit(maxInDegree))).withColumn(
        "scaled_outDegree", scale_degree("outDegree", Func.lit(maxOutDegree)))
    time5 = timeit.default_timer() - startTime
    print("time taken for layout of combined levels = {}".format(time5))

    vList = vertices_scaled_degree.select("id").rdd.flatMap(lambda x: x).collect()
    print("list of nodes are collected")

    vListPos = vertices_scaled_degree.select("xy").rdd.flatMap(lambda x: x).collect()
    print("list of nodes positions are collected")

    vListDegree = vertices_scaled_degree.select("scaled_inDegree").rdd.flatMap(lambda x: x).collect()
    print("list of nodes degree are collected")

    degree = dict(zip(vList, vListDegree))
    pos = dict(zip(vList, vListPos))
    print("dict of nodes and their positions are created")

    nxGraphLayout = nx.from_pandas_edgelist(edgesCheckpoint.toPandas(), source="src", target="dst")
    print("networkx graph object is created")

    # plot the networkX graph and save it to output path
    a = nx.draw(nxGraphLayout, pos, node_size=vListDegree, width=0.1)
    print("networkx graph using distribute layout is created")

    plt.title("{name}_{numIteration}_Iterations layout".format(name=name, numIteration=numIteration))
    plt.savefig("{outputPath}{name}_{numIteration}_Iterations.png".format(outputPath=outputPath, name=name,
                                                                          numIteration=numIteration), dpi=1000,
                bbox_inches='tight')
    print("graph is saved to the disk")
    print("Num of nodes: {}".format(updatedV.count()))
    print("Num of edges: {}".format(edgesCheckpoint.count()))

    # Save the calculated position of the graph to output path
    updatedV.coalesce(10).write.mode("overwrite").parquet(
        "{outputPath}{name}_{numIteration}_Iterations_updatedVertices.parquet".format(outputPath=outputPath, name=name,
                                                                                      numIteration=numIteration))
    edgesCheckpoint.coalesce(10).write.mode("overwrite").parquet(
        "{outputPath}{name}_{numIteration}_Iterations_edges.parquet".format(outputPath=outputPath, name=name,
                                                                            numIteration=numIteration))
    print("Nodes and Edges dataframes saved to disk")

    # remove the checkpoint dir
    try:
        if os.path.exists(checkpointDir):
            shutil.rmtree(checkpointDir)
            print("Successfully deleted the directory %s " % checkpointDir)
        else:
            print("Directory does not exist: %s " % checkpointDir)
    except OSError:
        print("Directory does not exist: %s " % checkpointDir)

    # TODO: potential packaging the code?
