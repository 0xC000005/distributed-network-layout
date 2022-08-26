#!/bin/bash
#SBATCH --nodes=20
#SBATCH --ntasks-per-node=10
#SBATCH --cpus-per-task=8
#SBATCH --time=23:59:59
#SBATCH --account=rrg-primath
#SBATCH --job-name=DistributedLayoutAlgorithm
#SBATCH --output=jobOutput/DistributedLayoutAlgorithm_output_%j.txt
#SBATCH --mail-user=maxjingwei.zhang@ryerson.ca
#SBATCH --mail-type=begin #email when job starts
#SBATCH --mail-type=end #email when job ends
#SBATCH --mail-type=FAIL

cd $SLURM_SUBMIT_DIR
export SPARK_LOG_DIR=$SCRATCH
#import required modules
module load CCEnv
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
export SLURM_SPARK_MEM=$(printf "%.0f" $((${SLURM_MEM_PER_NODE} *95/100)))
export PYTHONPATH=$PYTHONPATH:/cvmfs/soft.computecanada.ca/easybuild/software/2017/Core/spark/2.4.4/python/lib/py4j-0.10.7-src.zip

start-master.sh
sleep 5
#get URL of master node
MASTER_URL=spark://$(scontrol show hostname $SLURM_NODELIST | head -n 1):7077
NWORKERS=$((SLURM_NNODES - 1))
echo "Master URL = "$MASTER_URL
echo "Number of Workers = "$NWORKERS
echo "SPARK_IDENT_STRING = "$SPARK_IDENT_STRING
echo "SPARK_WORKER_DIR = "$SPARK_WORKER_DIR
echo "SLURM_MEM_PER_NODE = "$SLURM_MEM_PER_NODE
echo "SLURM_SPARK_MEM= "$SLURM_SPARK_MEM
echo "SLURM_CPUS_PER_TASK = "$SLURM_CPUS_PER_TASK


#start worker nodes
SPARK_NO_DAEMONIZE=1 srun -n 190 -N ${NWORKERS} -r 1 --label --output=$SPARK_LOG_DIR/spark-%j-workers.out start-slave.sh -m 20g -c ${SLURM_CPUS_PER_TASK} ${MASTER_URL} & slaves_pid=$!
srun -n 1 -N 1 spark-submit --master ${MASTER_URL} --conf spark.default.parallelism=3040 --conf spark.sql.shuffle.partitions=3040 --conf spark.driver.maxResultSize=2g --conf spark.memory.fraction=0.9 --driver-memory 18g --executor-memory 18g --conf spark.executor.memoryOverhead=2g --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.dynamicAllocation.enabled=False --conf "spark.executor.extraJavaOptions= -XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p' -Xloggc:$SCRATCH/log/ -XX:+UseCompressedOops" --conf "spark.driver.extraJavaOptions=-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p' -Xloggc:$SCRATCH/log/ -XX:+UseCompressedOops" --packages graphframes:graphframes:0.8.0-spark2.4-s_2.11  --repositories https://repos.spark-packages.org DistributedLayoutAlgorithm.py sampleInput1/covid19vaccine_twitter_replyto_net_feb2021.tsv output/ 100

kill $slaves_pid
stop-master.sh
