#!/bin/bash
#SBATCH --partition cpu-dedicated
#SBATCH --account dedicated-smp@cirad   ##@cirad-normal ##-smp@cirad
#SBATCH --job-name=AgriScale
#SBATCH --qos=dedicated
#SBATCH --output="out/AgriScale_%a.out"
#SBATCH --error="err/AgriScale_%a.err"
#SBATCH --time=23:58:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8 #2
#SBATCH --mem=64G #8
###SBATCH --exclude=io-cpu-02


export SIMUL_DIR=$SLURM_SUBMIT_DIR

echo "SIMUL_DIR =  ${SIMUL_DIR} "

echo "$USER"

cd ${SIMUL_DIR}


export ncpus=$SLURM_CPUS_PER_TASK
export nchunks=1                      ####### Number of array tasks
export INDEXES=$SLURM_ARRAY_TASK_ID

echo "number of chunks =  ${nchunks} "
echo "number of tasks =  $nchunks "
echo "number of cpus =  ${ncpus} "
echo "INDEXES : $INDEXES"

date

echo "==== JOB ID: $SLURM_JOB_ID ===="
echo "Node: $(hostname)"
echo "Temporary working directory: $TMPDIR"


export TMPDIR=/tmp/AgriScale_$SLURM_JOB_ID
mkdir -p $TMPDIR
echo "Temporary directory created at: $TMPDIR"
df -h "$TMPDIR"


export JOBLIB_TEMP_FOLDER=$TMPDIR
#/scratch/users/$USER/AgriScale_Ind/results:/inter

singularity exec --no-home -B /scratch/users/$USER/AgriScale_Ind:/package \
                           -B /scratch/users/$USER/tmp:$TMPDIR \
		           -B /scratch/users/$USER/AgriScale_Ind/results:/inter \
                           -B /scratch/users/$USER/tmp:/tempDir \
                           -B /storage/replicated/cirad/projects/AIDA/LIMA/AgriScale/data_zarr/data_zarr:/inputData \
                           -B /scratch/users/$USER/AgriScale_Ind/results:/outputData \
                            datamill.sif /package/scripts/main.sh $INDEXES $ncpus $nchunks


rm -rf $TMPDIR


echo "==== JOB ID: $SLURM_JOB_ID ===="
echo "Node: $(hostname)"
echo "Temporary working directory: $TMPDIR"

date

# sbatch --array=0-14 datamill.slurm

