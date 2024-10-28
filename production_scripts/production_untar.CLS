#CLS begin job script static section
#SBATCH --time=08:00:00                         # Job run time (hh:mm:ss)
#SBATCH --nodes=1                               # Number of nodes
#SBATCH --ntasks-per-node=16                    # Number of task (cores/ppn) per node
#SBATCH --job-name=RupCLS                     # Name of batch job
#SBATCH --output=untar_test_%j.out                  # stdout from job is written to this file
#SBATCH --error=untar_test_%j.err                   # stderr from job is written to this file
#SBATCH --mail-user=craigsteffen@gmail.com     # put YOUR email address for notifications
#SBATCH --mail-type=BEGIN,END                  # Type of email notifications to send

#CLS end job script static section
====================
#CLS begin job script active section (this should be the last section)

ORIGINAL_DIR=`pwd`
echo "original dir: $ORIGINAL_DIR"

TIMING_DATA_FILE="${TIMING_DATA_DIR}/FPD_test_${FP_SYSTEM}_${SLURM_JOBID}.dat"
echo "timing data file: ${TIMING_DATA_FILE}"

DATASET="GW"
FSIND=0 # File System INDex

FP_CODE="tar"

NODES=1
RANKS=1
ITER=0
ELAP=17

sleep 30

echo '#${FP_CODE} ${FP_SYSTEM} ${DATASET} ${TAR_FS} ${TAR_STRIPE} ${TGT_FS[FSIND]} ${TGT_STRIPE[FSIND]} ${NODES} ${RANKS} ${ITER} ${ELAP}' >> ${TIMING_DATA_FILE}
echo '#${FP_CODE} ${FP_SYSTEM} ${DATASET} ${TAR_FS} ${TAR_STRIPE} ${TGT_FS[FSIND]} ${TGT_STRIPE[FSIND]} ${NODES} ${RANKS} ${ITER} ${ELAP}' 
echo "#${FP_CODE} ${FP_SYSTEM} ${DATASET} ${TAR_FS} ${TAR_STRIPE} ${TGT_FS[FSIND]} ${TGT_STRIPE[FSIND]} ${NODES} ${RANKS} ${ITER} ${ELAP}" >> ${TIMING_DATA_FILE}
echo "#${FP_CODE} ${FP_SYSTEM} ${DATASET} ${TAR_FS} ${TAR_STRIPE} ${TGT_FS[FSIND]} ${TGT_STRIPE[FSIND]} ${NODES} ${RANKS} ${ITER} ${ELAP}" 

