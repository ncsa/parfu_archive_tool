CODE="CODEparfu"
SYSTEM="rockfish"
#TARGETFS="TGFS_Lustre_0008"
TARGETFS="TGFS_Lustre"
ARCFS="ARCfs_Lustre"
STRIPE="0008"
DATASET="GW"
NODES=1
RANKS=1

BASE_ARC_DIR=${DATA}
BASE_TGT_DIR=${DATA}

#TIMING_DATA_FILE="FPD_test_comet_${SLURM_JOBID}.dat"
TIMING_DATA_FILE="FPD_test_${SYSTEM}_${SLURM_JOBID}.dat"

cd $DATA/data/
echo "START:`date +%s`"
tar xf ../transfer/csteffen_FPfiles_Ar_2022jan07a.tar
echo "END:`date +%s`"

exit

mkdir -p output_files
TARGET_DIR="$BASE_TGT_DIR/${DATASET}_data/"
if [ ! -d "${TARGET_DIR}" ]; then
    echo "data target dir ${TARGET_DIR} does not exist!"
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
