#CLS begin job script static section
#SBATCH --time=08:00:00                         # Job run time (hh:mm:ss)
#SBATCH --nodes=1                               # Number of nodes
#SBATCH --ntasks-per-node=16                     # Number of task (cores/ppn) per node
#SBATCH --job-name=FPpf                      # Name of batch job
#SBATCH --output=parfu_test_%j.out                  # stdout from job is written to this file
#SBATCH --error=parfu_test_%j.err                   # stderr from job is written to this file

#CLS end job script static section
====================
#CLS begin job script active section (this should be the last section)

ORIGINAL_DIR=`pwd`
echo "original dir: $ORIGINAL_DIR"

TIMING_DATA_FILE="${TIMING_DATA_DIR}/FPD_test_${FP_SYSTEM}_${SLURM_JOBID}.dat"
echo "timing data file: ${TIMING_DATA_FILE}"

DATASET="GW"
DATASET_INDEX=0 # 0 is GW; 1 is Ar; 2 is VC

# system-specific index into list of volumes we might use
FSIND=0 # File System INDex

FP_CODE="parfu" # this is not used on command line; this is a human label

NODES=1
RANKS=16
ITER=0
#ELAP=17
MY_TARGET_DIR=${TGT_DIR[${FSIND}]}
MY_TARFILE=${TAR_DIR}/${FP_TARFILE[${DATASET_INDEX}]}

#echo "TAR_DIR=${TAR_DIR}"
#echo "iniital target: ${MY_TARGET_DIR}"



START=`date +%s`
echo "*** start execution"
# cd to the target directory and then reference the data directory relative makes
# things nice for both parfu and tar, in that the archive will reference from directly
# above the target directory.  
echo "cd ${MY_TARGET_DIR}"
cd ${MY_TARGET_DIR}

MY_OUTFILE=$ORIGINAL_DIR/${FP_CODE}_out_data_${DATASET}_${FP_SYSTEM}_${SLURM_JOBID}_${ITER}.out
#echo "About to fire off untar with output to file: ${MY_OUTFILE}"
echo "About to fire off ${FP_CODE} with output to file: ${MY_OUTFILE}"

MY_STATUSFILE="parfu_run_${SLURM_JOBID}_${ITER}"
MY_ARCFILE="${ARC_DIR[${FSIND}]}/${DATASET}_${SLURM_JOBID}_${ITER}.pfu"
# MY_TARGET="${TGT_DIR[FSIND]}/${DATASET}_data"
MY_TARGET="${DATASET}_data" # as explained above, referenced locally to make the archive nice.  Coupled to "cd ${MY_TARGET_DIR}" above.  
MY_OUTFILE="${ORIGINAL_DIR}/parfu_${DATASET}_${SLURM_JOBID}_${ITER}.out"
echo "code=${FP_CODE} archive=${MY_ARCFILE} target=${MY_TARGET} out=${MY_OUTFILE}" >> ${MY_STATUSFILE}
echo "code=${FP_CODE} archive=${MY_ARCFILE} target=${MY_TARGET} out=${MY_OUTFILE}" 
srun ${FP_SRUN_FLAGS} --nodes=${NODES} --ntasks-per-node=${RANKS} --cpus-per-task=1 parfu archivefile=${MY_ARCFILE} ${MY_TARGET} >& ${MY_OUTFILE}
#srun --nodes=${NODES} --ntasks-per-node=${RANKS} --cpus-per-task=1 parfu archivefile=${ARC_DIR[FSIND]}/${DATASET}_${SLURM_JOBID}_${ITER}.pfu ${TGT_DIR[FSIND]}/${DATASET}_data >& ${ORIGINAL_DIR}/parfu_${DATASET}_${SLURM_JOBID}_${ITER}.out
echo "***stop execution"
END=`date +%s`
ELAP=$(expr $END - $START)

echo '#${FP_CODE} ${FP_SYSTEM} ${DATASET} ${TAR_FS} ${TAR_STRIPE} ${TGT_FS[FSIND]} ${TGT_STRIPE[FSIND]} ${NODES} ${RANKS} ${ITER} ${ELAP}' >> ${TIMING_DATA_FILE}
echo '#${FP_CODE} ${FP_SYSTEM} ${DATASET} ${TAR_FS} ${TAR_STRIPE} ${TGT_FS[FSIND]} ${TGT_STRIPE[FSIND]} ${NODES} ${RANKS} ${ITER} ${ELAP}' 
echo "${FP_CODE} ${FP_SYSTEM} ${DATASET} ${TAR_FS} ${TAR_STRIPE} ${TGT_FS[FSIND]} ${TGT_STRIPE[FSIND]} ${NODES} ${RANKS} ${ITER} ${ELAP}" >> ${TIMING_DATA_FILE}
echo "${FP_CODE} ${FP_SYSTEM} ${DATASET} ${TAR_FS} ${TAR_STRIPE} ${TGT_FS[FSIND]} ${TGT_STRIPE[FSIND]} ${NODES} ${RANKS} ${ITER} ${ELAP}" 

