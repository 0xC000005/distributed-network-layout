WARNING: the following only works on Graham cluster 

Spark can't create folder on it's own, thus we need to create folders beforehand.

Creates following folders
```
mkdir ~/.spark/2.4.4/eventlog
mkdir ~/.spark/2.4.4/conf
mkdir ~/.spark/2.4.4/log
mkdir ~/distributed-network-layout/jobOutput
mkdir ~/distributed-nedwork-layout/jobOutput/checkpoint
```

In the `conf` folder, creates `spark-defaults.conf` and set it as following:
```
spark.eventLog.enabled true
spark.eventLog.dir /home/<userid>/.spark/<spark version>/eventlog
spark.history.fs.logDirectory  /home/<userid>/.spark/<spark version>/eventlog
```



Remember, it is important to execute `sbatch BashScript.sh` inside the Git repo since the path you put on the script depent on the point of execution.

To monitor the spark, first creates a SSH tunnel between your computer and the login node, then type the following and access the url on your local brower:
```
 module load spark
 [name@server ~]$ SPARK_NO_DAEMONIZE=1 start-history-server.sh 
starting org.apache.spark.deploy.history.HistoryServer, logging to /home/<userid>/.spark/<spark version>/log/spark-<userid>-org.apache.spark.deploy.history.HistoryServer-1-<server>.computecanada.ca.out
Spark Command: /cvmfs/soft.computecanada.ca/easybuild/software/2017/Core/java/1.8.0_121/bin/java -cp /home/<userid>/.spark/<spark version>/conf/:/cvmfs/soft.computecanada.ca/easybuild/software/2017/Core/spark/2.2.0/jars/* -Xmx1g org.apache.spark.deploy.history.HistoryServer
========================================
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
17/10/13 04:28:56 INFO HistoryServer: Started daemon with process name: 71616@<server>.computecanada.ca
17/10/13 04:28:56 INFO SignalUtils: Registered signal handler for TERM
17/10/13 04:28:56 INFO SignalUtils: Registered signal handler for HUP
17/10/13 04:28:56 INFO SignalUtils: Registered signal handler for INT
17/10/13 04:28:56 INFO SecurityManager: Changing view acls to: <userid>
17/10/13 04:28:56 INFO SecurityManager: Changing modify acls to: <userid>
17/10/13 04:28:56 INFO SecurityManager: Changing view acls groups to:
17/10/13 04:28:56 INFO SecurityManager: Changing modify acls groups to:
17/10/13 04:28:56 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(<userid>); groups with view permissions: Set(); users  with modify permissions: Set(<userid>); groups with modify permissions: Set()
17/10/13 04:28:56 INFO FsHistoryProvider: History server ui acls disabled; users with admin permissions: ; groups with admin permissions
17/10/13 04:29:01 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
17/10/13 04:29:02 INFO FsHistoryProvider: Replaying log path: file:/home/<userid>/.spark/<spark version>/eventlog/app-20171013040359-0000
17/10/13 04:29:02 INFO Utils: Successfully started service on port 18080.
17/10/13 04:29:02 INFO HistoryServer: Bound HistoryServer to 0.0.0.0, and started at http://<server ip address>:18080
 
```



`BashScript.sh`:
```
#!/bin/bash
#SBATCH --nodes=20
#SBATCH --ntasks-per-node=8
#SBATCH --cpus-per-task=5
#SBATCH --time=23:59:59
#SBATCH --account=def-primath
#SBATCH --job-name=DistributedLayoutAlgorithm
#SBATCH --output=jobOutput/DistributedLayoutAlgorithm_output_%j.txt
#SBATCH --mail-user=maxjingwei.zhang@ryerson.ca
#SBATCH --mail-type=begin #email when job starts
#SBATCH --mail-type=end #email when job ends
#SBATCH --mail-type=FAIL


cd $SLURM_SUBMIT_DIR
#import required modules
module load nixpkgs/16.09
module load spark/2.4.4
module load scipy-stack
module load python/3.6.3
module load networkx/1.1

# Recommended settings for calling Intel MKL routines from multi-threaded applications
# https://software.intel.com/en-us/articles/recommended-settings-for-calling-intel-mkl-routines-from-multi-threaded-applications 
export MKL_NUM_THREADS=1
export SPARK_IDENT_STRING=$SLURM_JOBID
export SPARK_WORKER_DIR=$SLURM_TMPDIR
export PYTHONPATH=$PYTHONPATH:/cvmfs/soft.computecanada.ca/easybuild/software/2017/Core/spark/2.4.4/python/lib/py4j-0.10.7-src.zip


start-master.sh
sleep 5
#get URL of master node
MASTER_URL=spark://$( scontrol show hostname $SLURM_NODELIST | head -n 1 ):7077
NWORKERS=$((SLURM_NNODES - 1))
echo "Master URL = "$MASTER_URL
echo "Number of Workers = "$NWORKERS

#start worker nodes
SPARK_NO_DAEMONIZE=1 srun -n 152 -N ${NWORKERS} -r 1 --label --output=$SPARK_LOG_DIR/spark-%j-workers.out start-slave.sh -m 16g -c 5 ${MASTER_URL} & slaves_pid=$!  

srun -n 1 -N 1 spark-submit --master ${MASTER_URL} --driver-memory 100g --conf spark.driver.memoryOvehead=50g --executor-memory 16g --conf spark.executor.memoryOverhead=4g --conf spark.memory.fraction=0.6 --conf spark.driver.maxResultSize=0 --conf spark.memory.storageFraction=0.5 --conf spark.default.parallelism=500 --conf spark.sql.shuffle.partitions=500 --conf spark.shuffle.io.maxRetries=10 --conf spark.blacklist.enabled=False  --conf spark.shuffle.io.retryWait=60s --conf spark.reducer.maxReqsInFlight=1 --conf spark.shuffle.io.backLog=4096 --conf spark.network.timeout=1200 --conf "spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xloggc:$SCRATCH/log/ -XX:+UseCompressedOops" --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.scheduler.listenerbus.eventqueue.capacity=20000 --packages graphframes:graphframes:0.8.0-spark2.4-s_2.11  --repositories https://repos.spark-packages.org DistributedLayoutAlgorithm.py /sampleInput1/graph_edgelist.tsv /testOuput/ 50
kill $slaves_pid
stop-master.sh
```

