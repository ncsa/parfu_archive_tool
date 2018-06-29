#!/bin/bash

#####
# user configuration options

# user sets up variables here (unless they're parsed via command line)
NODES=2
ITERATIONS=3
STRIPE=8
RANK_DIVISOR=2      # =1 to fill all CPU threads with ranks, =2 for only half of them
BLOCK=4             # parfu block size in MB (if relevant)
RUNTIME=2:00:00
MYEMAIL="craigsteffen@gmail.com"

# pick the data set we're testing against
# this will be change to iterate through the data sets
DATASET="GW"
#DATASET="Ar"
#DATASET="VC"

# select the code we're testing here
# this will be changed to iterate through the test codes
#CODE="tar"
#CODE="tar_gz"
#CODE="tar_pigz"
#CODE="mpitar"
#CODE="ptar"
CODE="parfu"
#CODE="ptgz"

# select the system we're on.  
# typically this is set once per system
#SYSTEM="wrangler_lustre"
#SYSTEM="wrangler_gpfs"
#SYSTEM="comet"
#SYSTEM="stampede2"
#SYSTEM="jyc_slurm"
#SYSTEM="jyc_moab"
SYSTEM="bw_moab"
#SYSTEM="bridges"

# end of user configuration options.  
#######

# find next non-existent run script name
COUNTER=0
SCRIPT_FILE_NAME=$(printf 'FPD_test_%s_%06d.bash' "$SYSTEM" "$COUNTER")
while [[ -e $SCRIPT_FILE_NAME ]]; do
    let COUNTER=COUNTER+1
    SCRIPT_FILE_NAME=$(printf 'FPD_test_%s_%06d.bash' "$SYSTEM" "$COUNTER")
done
echo "script file name=>${SCRIPT_FILE_NAME}<"
touch $SCRIPT_FILE_NAME

# populate intermediate variables according to what system we're on
case "$SYSTEM" in
    "wrangler_lustre")
	JOB_NAME="FPD_wr_LS"
	FS="lustre"
	MANAGER="slurm"
	RANKS_PER_NODE=24
	DATADIR=${DATA}
	;;
    "wrangler_hpfs")
	JOB_NAME="FPD_wr_HP"
	FS="hpfs"
	MANAGER="slurm"
	RANKS_PER_NODE=24
	;;
    "comet")
	JOB_NAME="FPD_comet"
	MANAGER="slurm"
	RANKS_PER_NODE=24
	FS="lustre"
	DATADIR=${SCRATCH}
	;;
    "stampede2")
	JOB_NAME="FPD_st2"
	RANKS_PER_NODE=64
	MANAGER="slurm"
	FS="lustre"
	QUEUE_NAME="normal"
	DATADIR='${SCRATCH}'
	MYMPIRUN_1="ibrun -n "
	MYMPIRUN_2=" -o 0"
	;;
    "jyc_slurm")
	JOB_NAME="FPD_jyc_SL"
	MANAGER="slurm"
	RANKS_PER_NODE=32
	FS="lustre"
	DATADIR="/scratch/staff/csteffen/FPD"
	MYMPIRUN_1="~jphillip/openmpi/bin/mpirun -n "
	MYMPIRUN_2=""
#	QUEUE_NAME="normal"
	;;
    "bw_moab")
	DATADIR="/scratch/staff/csteffen/FPD_2018"
	JOBNAME="FPD_BW_Mo"
	MANAGER="Moab"
	RANKS_PER_NODE=32
	FS="lustre"
	MYMPIRUN_1="aprun -n "
	MYMPIRUN_2=" -N $(( ${RANKS_PER_NODE}/${RANK_DIVISOR} )) -d $RANK_DIVISOR "	
	;;
    "jyc_moab")
	JOB_NAME="FPD_jyc_Moab"
	RANKS_PER_NODE=32
	MANAGER="moab"
	FS="lustre"
	;;
    "bridges")
	JOB_NAME="FPD_Br"
	RANKS_PER_NODE=28 
	MANAGER="slurm"
	FS="lustre"
	;;	
esac

#####
# preliminary work is done; start writing to the target script

# start populating the target script 
# first the boilerplate bash definition
echo "#!/bin/bash" >> ${SCRIPT_FILE_NAME}
echo "" >> ${SCRIPT_FILE_NAME}

# write the job description lines in the target script
case "$MANAGER" in
    "slurm")
	echo "#SBATCH -J ${JOB_NAME}      # Job name" >> ${SCRIPT_FILE_NAME}
	echo "#SBATCH -o bnch.o%j         # Name of stdout output file" >> ${SCRIPT_FILE_NAME}
	echo "#SBATCH -e bnch.e%j         # Name of stderr error file" >> ${SCRIPT_FILE_NAME}
	if [ $QUEUE_NAME ]; then
	    echo "#SBATCH -p ${QUEUE_NAME}    # Queue (partition) name" >> ${SCRIPT_FILE_NAME}
	fi
	echo "#SBATCH -N ${NODES}         # number of nodes" >> ${SCRIPT_FILE_NAME}
	echo "#SBATCH --tasks-per-node=${RANKS_PER_NODE}     #rank slots per node" >> ${SCRIPT_FILE_NAME}
	echo "#SBATCH -t ${RUNTIME}           # Run time (hh:mm:ss)" >> ${SCRIPT_FILE_NAME}
	echo "#SBATCH --mail-user=$MYEMAIL" >> ${SCRIPT_FILE_NAME}
	echo "#SBATCH --mail-type=all      # Send email at begin and end of job" >> ${SCRIPT_FILE_NAME}
	JOB_ID_NAME='${SLURM_JOBID}'
	;;
    "Moab")
	echo "#PBS -N ${JOBNAME}" >> $SCRIPT_FILE_NAME
	echo "#PBS -l nodes=${NODES}:ppn=32:xe" >> $SCRIPT_FILE_NAME
	echo "#PBS -l walltime=${RUNTIME}" >> $SCRIPT_FILE_NAME
	echo '#PBS -e $PBS_JOBID.err' >> $SCRIPT_FILE_NAME
	echo '#PBS -o $PBS_JOBID.out' >> $SCRIPT_FILE_NAME
	echo '#PBS -m bea' >> $SCRIPT_FILE_NAME
	echo "#PBS -M $MYEMAIL" >> $SCRIPT_FILE_NAME
	echo "" >> $SCRIPT_FILE_NAME
	echo 'cd $PBS_O_WORKDIR' >> $SCRIPT_FILE_NAME
	echo "" >> $SCRIPT_FILE_NAME
	JOB_ID_NAME='${PBS_JOBID}'
	;; 
esac
echo "" >> ${SCRIPT_FILE_NAME}

# variables used for configuration
# RANKS
# BASEDIR
# STRIPE
# JOB_ID_VARIABLE
RANKS=$(( (NODES*RANKS_PER_NODE)/RANK_DIVISOR ))
#EXPANDED_RANKS=$(printf '%04d' "$RANKS")
echo 'RANKS='$RANKS >> ${SCRIPT_FILE_NAME}
echo "RANKS=${RANKS}" 
echo 'NODES='$NODES >> ${SCRIPT_FILE_NAME}
echo "BASEDIR="$DATADIR >> ${SCRIPT_FILE_NAME}
EXPANDED_STRIPE=$(printf '%04d' "$STRIPE")
echo 'STRIPE="'$EXPANDED_STRIPE'"' >> ${SCRIPT_FILE_NAME}
echo 'DATASET="'${DATASET}'"' >> ${SCRIPT_FILE_NAME}
echo 'CODE="'${CODE}'"' >> ${SCRIPT_FILE_NAME}
EXPANDED_BLOCK=$(printf '%04d' "$BLOCK")
echo 'BLOCK="'$EXPANDED_BLOCK'"' >> ${SCRIPT_FILE_NAME}
echo 'MACH_FS="'$FS'"' >> ${SCRIPT_FILE_NAME}
echo "" >> ${SCRIPT_FILE_NAME}

# set up the data file lines in the target script
DATA_FILE_NAME_PREFIX=$(printf 'FPD_test_%s_' "$SYSTEM" )
DATA_FILE_NAME=$DATA_FILE_NAME_PREFIX"${JOB_ID_NAME}.dat"

echo "TIMING_DATA_FILE=\"${DATA_FILE_NAME}\"" >> ${SCRIPT_FILE_NAME}
echo "" >> ${SCRIPT_FILE_NAME}
echo $'echo \"starting production running\"' >> ${SCRIPT_FILE_NAME}
echo $'echo \'${CODE} ${BLOCK}    ${MACH_FS}  ${DATASET}    ${STRIPE}    ${NODES} ${RANKS}    ${ITER} ${ELAP}\' >> ${TIMING_DATA_FILE}' >> ${SCRIPT_FILE_NAME} # 

#' (this line is to get the emacs bash parser to play ball.  It does nothing)

echo "" >> ${SCRIPT_FILE_NAME}

echo "ITER=0" >> ${SCRIPT_FILE_NAME}
echo "NUM_ITERATIONS="$ITERATIONS >> ${SCRIPT_FILE_NAME}
#echo 'echo "comparison: >$CODE< >tar<"' >> ${SCRIPT_FILE_NAME}
echo 'if [ "${CODE}" == "tar" ]; then' >> ${SCRIPT_FILE_NAME}
echo '   let RANKS=1' >> ${SCRIPT_FILE_NAME}
echo 'fi' >> ${SCRIPT_FILE_NAME}
echo "" >> ${SCRIPT_FILE_NAME}

# check for directories
echo 'mkdir -p output_files' >> ${SCRIPT_FILE_NAME}
echo 'TARGET_DIR="$BASEDIR/${DATASET}_data/"' >> ${SCRIPT_FILE_NAME}
echo 'if [ ! -d "${TARGET_DIR}" ]; then' >> ${SCRIPT_FILE_NAME}
echo '    echo "data target dir ${TARGET_DIR} does not exist!"' >> ${SCRIPT_FILE_NAME}
echo '    exit'  >> ${SCRIPT_FILE_NAME}
echo 'fi' >> ${SCRIPT_FILE_NAME}
echo 'ARCHIVE_DIR="$BASEDIR/arc_${STRIPE}"' >> ${SCRIPT_FILE_NAME}
echo 'if [ ! -d "${ARCHIVE_DIR}" ]; then' >> ${SCRIPT_FILE_NAME}
echo '    echo "archive dir ${ARCHIVE_DIR} does not exist!"' >> ${SCRIPT_FILE_NAME}
echo '    exit'  >> ${SCRIPT_FILE_NAME}
echo 'fi' >> ${SCRIPT_FILE_NAME}
echo "" >> ${SCRIPT_FILE_NAME}

# now the data-taking while loop
echo 'while [ $ITER -lt $NUM_ITERATIONS ]; do ' >> ${SCRIPT_FILE_NAME}
echo '    echo "starting iteration $ITER ranks $RANKS"' >> ${SCRIPT_FILE_NAME}
echo '    START=`date +%s`' >> ${SCRIPT_FILE_NAME}
case ${CODE} in
    "parfu")
	echo '    '$MYMPIRUN_1'${RANKS}'$MYMPIRUN_2' parfu C $ARCHIVE_DIR/prod_'${JOB_ID_NAME}'_${ITER}.pfu $TARGET_DIR &> output_files/out_'${JOB_ID_NAME}'_${ITER}.out 2>&1' >> ${SCRIPT_FILE_NAME}
	;;
    "tar")
	echo '    '$MYMPIRUN_1' 1 tar cf $ARCHIVE_DIR/prod_'${JOB_ID_NAME}'_${ITER}.tar $TARGET_DIR > output_files/out_'${JOB_ID_NAME}'_${ITER}.out 2>&1' >> ${SCRIPT_FILE_NAME}
esac
echo '    END=`date +%s`' >> ${SCRIPT_FILE_NAME}
echo '    ELAP=$(expr $END - $START)' >> ${SCRIPT_FILE_NAME}
echo '    echo "${CODE} ${BLOCK}    ${MACH_FS}  ${DATASET}    ${STRIPE}    ${NODES} ${RANKS}    ${ITER} ${ELAP}" >> ${TIMING_DATA_FILE}' >> ${SCRIPT_FILE_NAME}
echo '    let ITER=ITER+1' >> ${SCRIPT_FILE_NAME}
echo 'done' >> ${SCRIPT_FILE_NAME}
echo "" >> ${SCRIPT_FILE_NAME}


# now we set up the loop computation in the target script
