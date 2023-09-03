#!/usr/bin/bash
# CLS static localization
#SBATCH --partition=defq                         # Partition (queue)

# end CLS static localization
# CLS csteffen's static personal localization
#SBATCH --mail-user=craigsteffen@gmail.com
#SBATCH --mail-type=ALL

# end CLS csteffen static personal localization
#CLS begin job script static section
#SBATCH --time=08:00:00                         # Job run time (hh:mm:ss)
#SBATCH --nodes=1                               # Number of nodes
#SBATCH --ntasks-per-node=4                    # Number of task (cores/ppn) per node
#SBATCH --job-name=RupCLS                     # Name of batch job
#SBATCH --output=untar_test_%j.out                  # stdout from job is written to this file
#SBATCH --error=untar_test_%j.err                   # stderr from job is written to this file
#SBATCH --mail-user=craigsteffen@gmail.com     # put YOUR email address for notifications
#SBATCH --mail-type=BEGIN,END                  # Type of email notifications to send

#CLS end job script static section
# Begin CLS system active localization fragment on rockfish

# This section will contain things like arrays for file systems with
# pathnames, so that the main parts of the code can refer to
# TARGET_FS[3] or whatever and have that be a valid path

# this is largely empty initially on rockfish because rockfish has
# statically defined ``$DATA'' which points to the scratch space

# system variable definitions go here.  

FP_SYSTEM="rockfish"

# system-specific file system array definitions go here

TAR_DIR="${DATA}/transfer"
TAR_FS="G"
TAR_STRIPE="0"

ARC_DIR[0]="${DATA}/arc"
TGT_DIR[0]="${DATA}/data"
ARC_FS[0]="G"
TGT_FS[0]="G"
ARC_STRIPE[0]="0"
TGT_STRIPE[0]="0"

ARC_DIR[1]="/tmp/arc"
TGT_DIR[1]="${DATA}/data"
ARC_FS[1]="S"
TGT_FS[1]="G"
ARC_STRIPE[1]="0"
TGT_STRIPE[1]="0"

ARC_DIR[2]="${DATA}/arc"
TGT_DIR[2]="/tmp/data/"
ARC_FS[2]="G"
TGT_FS[2]="S"
ARC_STRIPE[2]="0"
TGT_STRIPE[2]="0"

ARC_DIR[3]="/tmp/arc"
TGT_DIR[3]="/tmp/data"
ARC_FS[3]="S"
TGT_FS[3]="S"
ARC_STRIPE[3]="0"
TGT_STRIPE[3]="0"





TIMING_DATA_DIR="${HOME}/FPD_timing_data"
#TIMING_DATA_FILE="${HOME}/FPD_timing_data/FPD_${FPD_SYSTEM}_${SLURM_JOBID}.dat"

# end CLS system active job fragment
# CLS this is the active personal localization fragment

GW_TARFILE="csteffen_FPfiles_GW_2022jan07a.tar"
AR_TARFILE="csteffen_FPfiles_Ar_2022jan07a.tar"
VC_TARFILE="csteffen_FPfiles_VC_2022dec28a.tar"

# CLS end active personalization fragment

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
#cd ${TGT_DIR[FSIND]}
#echo "untar START:`date +%s`"
#START=`date +%s`
##tar xf ../transfer/csteffen_FPfiles_GW_2022jan07a.tar
#tar xf ${TAR_DIR}/${TARFL}
#echo "untar END:`date +%s`"
#END=`date +%s`
#ELAP=$(expr $END - $START)
#echo "${FP_CODE} ${FP_SYSTEM} ${DATASET} ${TAR_FS} ${TAR_STRIPE} ${TGT_FS[FSIND]} ${TGT_STRIPE[FSIND]} ${NODES} ${RANKS} ${ITER} ${ELAP}" >> ${TIMING_DATA_FILE}
#echo "timing:"
#echo "${FP_CODE} ${FP_SYSTEM} ${DATASET} ${TAR_FS} ${TAR_STRIPE} ${TGT_FS[FSIND]} ${TGT_STRIPE[FSIND]} ${NODES} ${RANKS} ${ITER} ${ELAP}"
#echo
#echo "check directory sizes:"
#du -s -h *
#echo "check directory counts:"
#find GW_data | wc -l
#echo "done checking"

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
srun --nodes=${NODES} --ntasks-per-node=${RANKS} parfu archivefile=${ARC_DIR[FSIND]}/${DATASET}_${SLURM_JOBID}_${ITER}.pfu ${TGT_DIR[FSIND]}/${DATASET}_data >& ${ORIGINAL_DIR}/parfu_${DATASET}_${SLURM_JOBID}_${ITER}.out
echo "untar END:`date +%s`"
END=`date +%s`
ELAP=$(expr $END - $START)
echo "${FP_CODE} ${FP_SYSTEM} ${DATASET} ${TGT_FS[FSIND]} ${TGT_STRIPE[FSIND]} ${ARC_FS[FSIND]} ${ARC_STRIPE[FSIND]} ${NODES} ${RANKS} ${ITER} ${ELAP}" >> ${TIMING_DATA_FILE}
echo "timing:"
echo "${FP_CODE} ${FP_SYSTEM} ${DATASET} ${TGT_FS[FSIND]} ${TGT_STRIPE[FSIND]} ${ARC_FS[FSIND]} ${ARC_STRIPE[FSIND]} ${NODES} ${RANKS} ${ITER} ${ELAP}" 
echo



ITER="2"
FP_CODE="tar"
RANKS=1
mkdir -p ${ARC_DIR[FSIND]}
echo "parfu START:`date +%s`"
START=`date +%s`
#tar xf ../transfer/csteffen_FPfiles_GW_2022jan07a.tar
#tar xf ${TAR_DIR}/${TARFL}
echo "about to run parfu at `pwd`"
touch parfu_run_${SLURM_JOBID}_${ITER}
#srun --nodes=${NODES} --ntasks-per-node=${RANKS} parfu archivefile=${ARC_DIR[FSIND]}/${DATASET}_${SLURM_JOBID}_${ITER}.pfu ${TGT_DIR[FSIND]}/${DATASET}_data >& ${ORIGINAL_DIR}/parfu_${DATASET}_${SLURM_JOBID}_${ITER}.out
tar cf ${ARC_DIR[FSIND]}/${DATASET}_${SLURM_JOBID}_${ITER}.tar ${TGT_DIR[FSIND]}/${DATASET}_data >& ${ORIGINAL_DIR}/tar_${DATASET}_${SLURM_JOBID}_${ITER}.out
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
