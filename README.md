
# Distributed Layout Algorithm
The algorithm is designed to calculate the layout of large graph dataset in a distributed environment. It utilize PySpark's distributed framework with graphframe package for calculating distributed layout. 

The algorithm is based on the following paper
> [Antoine Hinge, Gaëlle Richer, David Auber. MuGDAD: Multilevel graph drawing algorithm in a distributed architecture . Conference on Computer Graphics, Visualization and Computer Vision, IADIS, Jul 2017, Lisbon, Portugal. pp.189. ffhal-01516889](https://hal.archives-ouvertes.fr/hal-01516889/document)

To calculate the position of the vertices of large graph we used force-directed algorithm as a basis, that is, the modified algorithm of [Fruchterman, T. M., & Reingold, E. M. (1991). Graph drawing by force‐directed placement. _Software: Practice and experience_, _21_(11), 1129-1164](https://onlinelibrary.wiley.com/doi/abs/10.1002/spe.4380211102). Instead of calculating repulsive force for each pair of vertex we used centroids and center repulsive force and attractive forces is the force between vertices connected via an edge.


# Batch script file

The algorithm can be deployed on cluster such as ComputeCanada using the batch script file, sample batch sript is provided in the repository - **BatchScript.sh**

## Job Arguments

The scripts requires 4 arguments that need to be passed in BatchScript.sh file. Following is the example of argument given with the spark-submit command inside the BatchScript.sh file
> spark-submit --master [MASTER_URL] --driver-memory 100g  --executor-memory 16g --packages graphframes:graphframes:0.8.0-spark2.4-s_2.11 [Path of distributedLayoutAlgorithm.py file] [Input path of graph edgelist tab seperated file] [output path] [Integer value for number of Iterations]

**Note**: The **graphframe** library is passed as an argument to the **--package** in the spark-submit command. This will include the package in the script. 


## Input Format
The input file should be a TSV edge list file

## Output
The algorithm wil produce couple of outputs that will be saved in the output path 
- The matplotlib plot of the graph
- calculated position of vertex and edges of the graph

# Run the algorithm on ComputeCanada cluster

To run the algorithm on cluster we will have to submit the batch script to the cluster. We can submit the job with following command

>sbatch BatchScript.sh