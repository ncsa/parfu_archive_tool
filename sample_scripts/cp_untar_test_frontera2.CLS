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

#SYSTEM="rockfish"
#ARCFS="ARCfs_Lustre"
#STRIPE="0008"
#DATASET="GW"
NODES=1

#BASE_ARC_DIR=${DATA}
#BASE_TGT_DIR=${DATA}

TIMING_DATA_FILE="${TIMING_DATA_DIR}/FPD_test_${SYSTEM}_${SLURM_JOBID}.dat"
echo "timing data file: ${TIMING_DATA_FILE}"

echo '#${FP_CODE} ${FP_SYSTEM} ${DATASET} ${TAR_FS} ${TAR_STRIPE} ${TGT_FS[FSIND]} ${TGT_STRIPE[FSIND]} ${NODES} ${RANKS} ${ITER} ${ELAP}' >> ${TIMING_DATA_FILE}
echo '#${FP_CODE} ${FP_SYSTEM} ${DATASET} ${TAR_FS} ${TAR_STRIPE} ${TGT_FS[FSIND]} ${TGT_STRIPE[FSIND]} ${NODES} ${RANKS} ${ITER} ${ELAP}' 

DATASET="GW"
FSIND=0 # File System INDex


#ITER="0"
#FP_CODE="untar"
#TARFL="${GW_TARFILE}"
#RANKS=1
#mkdir -p ${TGT_DIR[FSIND]}
cd ${TGT_DIR[FSIND]}
#echo "untar START:`date +%s`"
#START=`date +%s`
##tar xf ../transfer/csteffen_FPfiles_GW_2022jan07a.tar
#tar xf ${TAR_DIR}/${TARFL}
#echo "untar END:`date +%s`"
#END=`date +%s`
#ELAP=$(expr $END - $START)
#echo "${FP_CODE} ${FP_SYSTEM} ${DATASET} ${TAR_FS} ${TAR_STRIPE} ${TGT_FS[FSIND]} ${TGT_STRIPE[FSIND]} ${NODES} ${RANKS} ${ITER} ${ELAP}" >> ${TIMING_DATA#_FILE}
#echo "timing:"
#echo "${FP_CODE} ${FP_SYSTEM} ${DATASET} ${TAR_FS} ${TAR_STRIPE} ${TGT_FS[FSIND]} ${TGT_STRIPE[FSIND]} ${NODES} ${RANKS} ${ITER} ${ELAP}"
echo
echo "check directory sizes:"
du -s -h *
echo "check directory counts:"
find GW_data | wc -l
echo "done checking"

ITER="1"
FP_CODE="parfu"
RANKS=2
mkdir -p ${ARC_DIR[FSIND]}
echo "parfu START:`date +%s`"
START=`date +%s`
#tar xf ../transfer/csteffen_FPfiles_GW_2022jan07a.tar
#tar xf ${TAR_DIR}/${TARFL}
echo "about to run parfu at `pwd`"
touch parfu_run_${SLURM_JOBID}_${ITER}
srun --nodes=${NODES} --ntasks-per-node=${RANKS} --cpus-per-task=1 parfu archivefile=${ARC_DIR[FSIND]}/${DATASET}_${SLURM_JOBID}_${ITER}.pfu ${TGT_DIR[FSIND]}/${DATASET}_data >& ${ORIGINAL_DIR}/parfu_${DATASET}_${SLURM_JOBID}_${ITER}.out
echo "untar END:`date +%s`"
END=`date +%s`
ELAP=$(expr $END - $START)
echo "${FP_CODE} ${FP_SYSTEM} ${DATASET} ${TGT_FS[FSIND]} ${TGT_STRIPE[FSIND]} ${ARC_FS[FSIND]} ${ARC_STRIPE[FSIND]} ${NODES} ${RANKS} ${ITER} ${ELAP}" >> ${TIMING_DATA_FILE}
echo "timing:"
echo "${FP_CODE} ${FP_SYSTEM} ${DATASET} ${TGT_FS[FSIND]} ${TGT_STRIPE[FSIND]} ${ARC_FS[FSIND]} ${ARC_STRIPE[FSIND]} ${NODES} ${RANKS} ${ITER} ${ELAP}" 
echo




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
