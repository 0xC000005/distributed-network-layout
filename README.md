
# Distributed Network Layout Algorithm

The Distributed Network Layout script is designed to calculate a network layout for a large graph using a distributed environment; that is, to identify X and Y coordinates for each vertice while minimizing the number of overlapping nodes/edges.

To support the distributed processing of a graph, the Distributed Network Layout (DNL) script utilizes the [GraphFrames](https://graphframes.github.io/graphframes/docs/_site/index.html) package for Apache Spark. DNL is designed to be run on a large compute cluster such [ComputeCanada](https://www.computecanada.ca/).

At its core, the DNL script is a Spark/GraphFrames-based implementation of the MuGDAD approach proposed by [Hinge, Richer & Auber (2017)](https://hal.archives-ouvertes.fr/hal-01516889/document) and the Force-Directed algorithm by [Fruchterman & Reingold (1991)](https://doi.org/10.1002/spe.4380211102).

Please cite as:
    
    Gruzd, A. & Attarwala, S. (2021). Distributed Network Layout [Open Souce]. Social Media Lab, Toronto Metropolitan University, Toronto, Canada.


## How to start the batch file

To run the DNL script on a compute cluster such as [ComputeCanada](https://www.computecanada.ca/), modify the sample batch file ([BatchScript.sh](https://github.com/SocialMediaLab/distributed-network-layout/blob/main/BatchScript.s)) and then submit it using the sbatch command as shown below:

    sbatch BatchScript.sh

The Batch Script requires 4 arguments that need to be passed with the spark-submit command. They are listed below:
| Argument | Description|
|--|--|
|1. Location of the DNL script | Spark will look for this location to import the DNL script |
|2. Location of the input edge-list file | DNL will load the input file from this location |
|3. Location of the output folder (Note: the output folder has to exist) | DNL will save the output in this location |
|4. The number of iterations | DNL will iterate the layout algorithm for the given number of iterations.  |

 
After starting the script using the batch file, you can monitor its progression with the following command:

    sqc -j [job_id]


## How to configure the batch file
Prior to starting the script, you will need to update the parameters in the **srun** line for the spark-submit command inside the BatchScript.sh file. 

    srun -n 1 -N 1 spark-submit --master [MASTER_URL] --driver-memory 100g --executor-memory 16g --packages graphframes:graphframes:0.8.0-spark2.4-s_2.11 /distributedLayputScript.py /amazon.txt /outputFolder/ 15

> In the above sample spark-submit command the 1st argument is
> “**/distributedLayputScript.py**” location of script, 2nd argument is
> “**/amazon.txt**” input file location, 3rd argument is
> “**/outputFolder**” the location of output folder, and 4th is “**15**”
> the number of Iteration of layout algorithm.

The arguments are separated by a single space.

Note:  You will also need to confirm the version of GraphFrames and Apache Spark installed in your environment and modify this parameter in the "srun" line accordingly.  By default, we specify the version of GraphFrames package as 0.8.0 and the version of Spark as 2.4.

    --packages graphframes:graphframes:0.8.0-spark2.4-s_2.11

The number of nodes and tasks-per-node parameters of the job can be changed depending on the available resources. The following commands need to be modified:

    #!/bin/bash
    #SBATCH --nodes=20
    #SBATCH --ntasks-per-node=8
    #SBATCH --cpus-per-task=5


NOTE: If you decide to change the number of nodes, make sure to also adjust the number of workers  (“-n”) in the "start worker nodes" line, after "SPARK_NO_DAEMONIZE=1 srun -n ...". Specifically, it has to be < [ nodes X ntasks-per-node ].

In the example above, since the total number of nodes is set to 20 and the number of tasks per node is 8, we assign 152 to -n parameter, just to make sure we leave some room for the system.

Similarly, if you change the number of tasks per node (ntask-per-node), make sure that your node has enough RAM for each task.

In our case, since the node had 180Gb, we assign 16GB to -m parameter and --executor-memory per node just to make sure we leave some room for the system.

    #start worker nodes
    
    SPARK_NO_DAEMONIZE=1 srun -n 152 -N ${NWORKERS} -r 1 --label --output=$SPARK_LOG_DIR/spark-%j-workers.out start-slave.sh -m 16g -c 5 ${MASTER_URL} & slaves_pid=$!
    
    srun -n 1 -N 1 spark-submit --master ${MASTER_URL} --driver-memory 100g --conf spark.driver.memoryOvehead=50g --executor-memory 16g

## Input Format

The default input format is a tab-separated values (TSV) edge list. The input file can have comments starting with # character. Here's a sample of the acceptable input format:

    # Undirected graph: ../../data/output/amazon.ungraph.txt
    # Amazon
    # Nodes: 334863 Edges: 925872
    # FromNodeId  ToNodeId
    1  88160 
    1  118052
    1  161555
    1  244916
    1  346495
    1  444232
    1  447165
    1  500600
    2  27133
    2  62291
    2  170507

The script will load the file into a dataframe using following code:

## How to load the input edge-list file

    edges = spark.read.csv(inputPath,sep = "\t",comment='#',header=None)

Users can modify the input file format supported by script by changing the separator value in the above code. For example if the input file is a comma separated values then the separator argument will be “,” instead of “\t”

## Output format

The DNL script will produce the following output files:

 1. An image of the network visualization in the PNG format
 2. A parquet file containing the list of vertices with the calculated X and Y coordinates to be used for the network visualization. 
 3. A parquet file containing the list of edges for the network visualization 
