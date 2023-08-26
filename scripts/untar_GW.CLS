#SBATCH --time=08:00:00                         # Job run time (hh:mm:ss)
#SBATCH --nodes=1                               # Number of nodes
#SBATCH --ntasks-per-node=16                    # Number of task (cores/ppn) per node
#SBATCH --job-name=RFunT                     # Name of batch job
#SBATCH --output=serial_%j.out                  # stdout from job is written to this file
#SBATCH --error=serial_%j.err                   # stderr from job is written to this file

============

CODE="untar"

#TARGETFS="TGFS_Lustre_0008"
TARGETFS="TGFS_Lustre"
ARCFS="ARCfs_Lustre"
STRIPE="0008"
DATASET="GW"
TARFILE=${GW_TARFILE}
NODES=1
RANKS=1

mkdir -p ${TIMING_DATA_DIR}
#TIMING_DATA_FILE="FPD_test_comet_${SLURM_JOBID}.dat"
#TIMING_DATA_FILE="${HOME}/FPD_timing_data/FPD_${FPD_SYSTEM}_${SLURM_JOBID}.dat"

DIR_INDEX=0

cd ${TGT_DIR[$DIR_INDEX]}
echo "START:`date +%s`"
START=`date +%s`

ITER="0"

#tar xf ../transfer/
tar xf "${TAR_DIR}/${TARFILE}"
END=`date +%s`
ELAP=$(expr $END - $START)
echo "END:`date +%s`"

echo "${CODE} ${FPD_SYSTEM} ${DATASET} ${TGT_FS[$DIR_INDEX]} ${ARCFS[$DIR_INDEX]} ${STRIPE} ${NODES} ${RANKS}    ${ITER} ${ELAP}" >> ${TIMING_DATA_FILE}


exit

fi
ARCHIVE_DIR="$BASE_ARC_DIR/arc_${STRIPE}"
if [ ! -d "${ARCHIVE_DIR}" ]; then
    echo "archive dir ${ARCHIVE_DIR} does not exist!"
    exit
fi

ITER=0
NUM_ITERATIONS=1

while [ $ITER -lt $NUM_ITERATIONS ]; do 
    START=`date +%s`

    cd $DATA/data/
    tar cf $ARCHIVE_DIR/prod

    END=`date +%s`
    ELAP=$(expr $END - $START)
    echo ${CODE} ${SYSTEM} ${DATASET} ${TARGETFS} ${ARCFS} ${STRIPE} ${NODES} ${RANKS}    ${ITER} ${ELAP}" >> ${TIMING_DATA_FILE}
#    echo "${CODE} ${BLOCK}    ${MACH_FS}  ${DATASET}    ${STRIPE}    ${NODES} ${RANKS}    ${ITER} ${ELAP}" >> ${TIMING_DATA_FILE}
    let ITER=ITER+1
done
