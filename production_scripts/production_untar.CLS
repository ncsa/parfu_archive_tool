#CLS begin job script static section
#SBATCH --time=08:00:00                         # Job run time (hh:mm:ss)
#SBATCH --nodes=1                               # Number of nodes
#SBATCH --ntasks-per-node=1                     # Number of task (cores/ppn) per node
#SBATCH --job-name=FPuntar                      # Name of batch job
#SBATCH --output=untar_test_%j.out                  # stdout from job is written to this file
#SBATCH --error=untar_test_%j.err                   # stderr from job is written to this file

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

FP_CODE="tar"

NODES=1
RANKS=1
ITER=0
#ELAP=17
MY_TARGET_DIR=${TGT_DIR[${FSIND}]}
MY_TARFILE=${TAR_DIR}/${FP_TARFILE[${DATASET_INDEX}]}

echo "TAR_DIR=${TAR_DIR}"
echo "iniital target: ${MY_TARGET_DIR}"
echo "my tarfile: ${MY_TARFILE}"


START=`date +%s`
echo "*** start execution"
echo "cd ${MY_TARGET_DIR}"
cd ${MY_TARGET_DIR}
MY_OUTFILE=$ORIGINAL_DIR/tar_out_${FP_SYSTEM}_${SLURM_JOBID}_${ITER}.out
echo "About to fire off untar with output to file: ${MY_OUTFILE}"
echo "tar xf ${MY_TARFILE} >& ${MY_OUTFILE}"
tar xf ${MY_TARFILE} >& ${MY_OUTFILE}
echo "***stop execution"
END=`date +%s`
ELAP=$(expr $END - $START)

echo '#${FP_CODE} ${FP_SYSTEM} ${DATASET} ${TAR_FS} ${TAR_STRIPE} ${TGT_FS[FSIND]} ${TGT_STRIPE[FSIND]} ${NODES} ${RANKS} ${ITER} ${ELAP}' >> ${TIMING_DATA_FILE}
echo '#${FP_CODE} ${FP_SYSTEM} ${DATASET} ${TAR_FS} ${TAR_STRIPE} ${TGT_FS[FSIND]} ${TGT_STRIPE[FSIND]} ${NODES} ${RANKS} ${ITER} ${ELAP}' 
echo "${FP_CODE} ${FP_SYSTEM} ${DATASET} ${TAR_FS} ${TAR_STRIPE} ${TGT_FS[FSIND]} ${TGT_STRIPE[FSIND]} ${NODES} ${RANKS} ${ITER} ${ELAP}" >> ${TIMING_DATA_FILE}
echo "${FP_CODE} ${FP_SYSTEM} ${DATASET} ${TAR_FS} ${TAR_STRIPE} ${TGT_FS[FSIND]} ${TGT_STRIPE[FSIND]} ${NODES} ${RANKS} ${ITER} ${ELAP}" 

