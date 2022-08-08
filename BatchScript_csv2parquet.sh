#!/bin/bash
#SBATCH --account=rrg-primath
#SBATCH --ntasks=1
#SBATCH --nodes=1
#SBATCH --cpus-per-task=80
#SBATCH --time=8:0:0
#SBATCH --job-name=csv2parquet
#SBATCH --output=csv2parquet_output_%j.txt
#SBATCH --mail-user=maxjingwei.zhang@ryerson.ca
#SBATCH --mail-type=begin #email when job starts
#SBATCH --mail-type=end #email when job ends
#SBATCH --mail-type=FAIL

module load CCEnv
module load StdEnv/2020  gcc/9.3.0
module load arrow/0.17.1
source $SCRATCH/virEnv/bin/activate
csv2parquet SNAP/com-friendster.ungraph.txt