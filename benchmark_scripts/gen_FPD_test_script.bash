#!/bin/bash

#####
# user configuration options

# user sets up variables here (unless they're parsed via command line)
NODES=1
ITERATIONS=3
STRIPE=8
RANK_DIVISOR=1      # =1 to fill all CPU threads with ranks, =2 for only half of them
BLOCK=4             # parfu block size in MB (if relevant)
RUNTIME=2:00:00
CHARGE_ACCOUNT=""


if [ ! "$MYEMAIL" ]; then
    MYEMAIL=""
fi
if [ ! "$ENABLE_EMAIL_NOTIFICATIONS" ]; then
    ENABLE_EMAIL_NOTIFICATIONS="yes"
fi    

# pick the data set we're testing against
# this will be change to iterate through the data sets
#FPD_DATASET="GW"
#FPD_DATASET="Ar"
#FPD_DATASET="VC"

# select the code we're testing here
# this will be changed to iterate through the test codes
#FPD_CODE="tar"
#FPD_CODE="tar_gz"
#FPD_CODE="tar_pigz"
#FPD_CODE="mpitar"
#FPD_CODE="ptar"
#FPD_CODE="parfu"
#FPD_CODE="ptgz"

# select the system we're on.  
# typically this is set once per system
#FPD_SYSTEM="wrangler_LL"
#FPD_SYSTEM="wrangler_LG"
#FPD_SYSTEM="comet"
#FPD_SYSTEM="stampede2"
#FPD_SYSTEM="jyc_slurm"
#FPD_SYSTEM="jyc_moab"
#FPD_SYSTEM="bw_moab"
#FPD_SYSTEM="bridges"
#FPD_SYSTEM="iforge"

if [ ! "$FPD_SYSTEM" ]; then
    echo ; echo "You must set a valid FPD_SYSTEM!  (Edit the script, or set an env var.)" ; echo
    exit
fi    
if [ ! "$FPD_DATASET" ]; then
    echo ; echo "You must set a valid FPD_DATASET!  (Edit the script, or set an env var.)" ; echo
    exit
fi    
if [ ! "$FPD_CODE" ]; then
    echo ; echo "You must set a valid FPD_CODE!  (Edit the script, or set an env var.)" ; echo
    exit
fi    
if [ ! "$MYEMAIL" ]; then
    if [ "$ENABLE_EMAIL_NOTIFICATIONS" == "yes" ]; then
	echo
	echo "Email notifications enabled (set to \"yes\") but email address empty.  Either"
	echo "set ENABLE_EMAIL_NOTIFICATIONS to \"no\" or fill in or export a value for MYEMAIL."
	echo
	exit
    fi
fi

case "$FPD_CODE" in
    "parfu")
	;;
    "ptgz")
	;;
    "tar")
	;;
    *)
	echo 'FPD_CODE='$FPD_CODE', which is not a valid code name! Exiting.'
	exit;
	;;
esac

case "$FPD_DATASET" in
    "GW")
	;;
    "Ar")
	;;
    "VC")
	;;
    *)
	echo 'FPD_DATASET='$FPD_DATASET'!  Dataset must be "GW", "Ar", or "VC".'
	exit;
	;;
esac

# end of user configuration options.  
#######

# find next non-existent run script name
COUNTER=0
SCRIPT_FILE_NAME=$(printf 'FPD_test_%s_%06d.bash' "$FPD_SYSTEM" "$COUNTER")
while [[ -e $SCRIPT_FILE_NAME ]]; do
    let COUNTER=COUNTER+1
    SCRIPT_FILE_NAME=$(printf 'FPD_test_%s_%06d.bash' "$FPD_SYSTEM" "$COUNTER")
done
echo "script file name= >>> ${SCRIPT_FILE_NAME} <<<"
touch $SCRIPT_FILE_NAME

# populate intermediate variables according to what system we're on
case "$FPD_SYSTEM" in
    "wrangler_LL")
	JOB_NAME="FPD_wr_LL"
	BASE_SYSTEM_NAME="wrangler"
	MANAGER="slurm"
	DATA_FS="lstr"
	ARC_FS="lstr"
	JOB_NAME="FPD_wr_LL"
	RANKS_PER_NODE=24
	DATADIR='${DATA}'
	ARCDIR='${DATA}'
	MYMPIRUN_1="ibrun -n "
	MYMPIRUN_2=" -o 0"
	QUEUE_NAME="normal"
	;;
    "wrangler_LG")
	JOB_NAME="FPD_wr_LG"	
	BASE_SYSTEM_NAME="wrangler"
	MANAGER="slurm"
	DATA_FS="lstr"
	ARC_FS="gpfs"
	JOB_NAME="FPD_wr_LG"
	RANKS_PER_NODE=24
	DATADIR='${DATA}'
	ARCDIR='${FLASH}/FP_data'
	MYMPIRUN_1="ibrun -n "
	MYMPIRUN_2=" -o 0"
	QUEUE_NAME="normal"
	;;
    "comet")
	JOB_NAME="FPD_comet"
	BASE_SYSTEM_NAME="comet"
	MANAGER="slurm"
	DATA_FS="lstr"
	ARC_FS="lstr"
	JOB_NAME="FPD_comet"
	RANKS_PER_NODE=24
	DATADIR='${SCRATCH}'
	ARCDIR='${SCRATCH}'       
	MYMPIRUN_1="ibrun -n "
	MYMPIRUN_2=" -o 0"
	;;
    "stampede2")
	JOB_NAME="FPD_st2"	
	BASE_SYSTEM_NAME="stampede2"
	MANAGER="slurm"
	DATA_FS="lstr"
	ARC_FS="lstr"
	RANKS_PER_NODE=64
	MANAGER="slurm"
	QUEUE_NAME="normal"
	DATADIR='${SCRATCH}'
	ARCDIR='${SCRATCH}'
	MYMPIRUN_1="ibrun -n "
	MYMPIRUN_2=" -o 0"
	;;
    "jyc_slurm")
	JOB_NAME="FPD_jyc_SL"
	BASE_SYSTEM_NAME="jyc"
	MANAGER="slurm"
	DATA_FS="lstr"
	ARC_FS="lstr"
	RANKS_PER_NODE=32
	DATADIR='/scratch/staff/csteffen/FPD'
	ARCDIR='/scratch/staff/csteffen/FPD'
	MYMPIRUN_1="~jphillip/openmpi/bin/mpirun -n "
	MYMPIRUN_2=""
#	QUEUE_NAME="normal"
	;;
    "bw_moab")
	JOB_NAME="FPD_BW_Mo"
	BASE_SYSTEM_NAME="bw"
	MANAGER="Moab"
	DATA_FS="lstr"
	ARC_FS="lstr"
	DATADIR="/scratch/staff/csteffen/FPD_2018"
	ARCDIR="/scratch/staff/csteffen/FPD_2018"
	RANKS_PER_NODE=32
	MYMPIRUN_1="aprun -n "
	MYMPIRUN_2=" -N $(( ${RANKS_PER_NODE}/${RANK_DIVISOR} )) -d $RANK_DIVISOR "	
	;;
    "jyc_moab")
	JOB_NAME="FPD_jyc_Moab"
	BASE_SYSTEM_NAME="jyc"
	MANAGER="Moab"
	DATA_FS="lstr"
	ARC_FS="lstr"
	RANKS_PER_NODE=32
	MANAGER="Moab"
	MYMPIRUN_1="aprun -n "
	MYMPIRUN_2=" -N $(( ${RANKS_PER_NODE}/${RANK_DIVISOR} )) -d $RANK_DIVISOR "	
	DATADIR="/scratch/staff/csteffen/FPD"
	ARCDIR="/scratch/staff/csteffen/FPD"
	;;
    "bridges")
	JOB_NAME="FPD_Br"
	BASE_SYSTEM_NAME="bridges"
	MANAGER="slurm"
	DATA_FS="lstr"
	ARC_FS="lstr"
	RANKS_PER_NODE=28 
	MYMPIRUN_1="ibrun -n "
	MYMPIRUN_2=" -o 0"
	DATADIR='${SCRATCH}/FP_data'
	ARCDIR='${SCRATCH}/FP_data'
	;;
    "iforge")
	JOB_NAME="FPD_iF"
	BASE_SYSTEM_NAME="iforge"
	QUEUE="skylake"
	PSN="aaa"
	MANAGER="pbs"
	DATA_FS="gpfs"
	ARC_FS="gpfs"
	RANKS_PER_NODE=40 
	MYMPIRUN_1="mpirun_rsh -ssh -np " 
	MYMPIRUN_2=" -N $(( ${RANKS_PER_NODE}/${RANK_DIVISOR} )) --cpus-per-proc $RANK_DIVISOR"
	DATADIR="/projects/bioinformatics/ParFuTesting/TestData"
	ARCDIR="/projects/bioinformatics/ParFuTesting/Archive"
	;;
    *)
	echo 'The $FPD_SYSTEM variable not set to a valid value.  Exiting without generating a script.'
	exit
	;;
esac

if [ ! "$BASE_SYSTEM_NAME" ];then 
    echo "finished with FPD_SYSTEM case, but BASE_SYSTEM_NAME not set!!!  Aborting."
    exit
fi

case "$MANAGER" in
    "pbs")
	;;
    "Moab")
	;;
    "slurm")
	;;
    *)
	echo "MANAGER="$MANAGER", which is not valid!"
	exit
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
	if [ "$CHARGE_ACCOUNT" ]; then
	    echo "#SBATCH -A $CHARGE_ACCOUNT" >> $SCRIPT_FILE_NAME
	fi
	if [ $QUEUE_NAME ]; then
	    echo "#SBATCH -p ${QUEUE_NAME}    # Queue (partition) name" >> ${SCRIPT_FILE_NAME}
	fi
	echo "#SBATCH -N ${NODES}         # number of nodes" >> ${SCRIPT_FILE_NAME}
	echo "#SBATCH --tasks-per-node=${RANKS_PER_NODE}     #rank slots per node" >> ${SCRIPT_FILE_NAME}
	echo "#SBATCH -t ${RUNTIME}           # Run time (hh:mm:ss)" >> ${SCRIPT_FILE_NAME}
	if [ $ENABLE_EMAIL_NOTIFICATIONS ]; then
	    echo "#SBATCH --mail-user=$MYEMAIL" >> ${SCRIPT_FILE_NAME}
	    echo "#SBATCH --mail-type=all      # Send email at begin and end of job" >> ${SCRIPT_FILE_NAME}
	fi
	JOB_ID_NAME='${SLURM_JOBID}'
	;;
    "Moab")
	echo "#PBS -N ${JOB_NAME}" >> $SCRIPT_FILE_NAME
	echo "#PBS -l nodes=${NODES}:ppn=32:xe" >> $SCRIPT_FILE_NAME
	echo "#PBS -l walltime=${RUNTIME}" >> $SCRIPT_FILE_NAME
	if [ "$CHARGE_ACCOUNT" ]; then
	    echo "#PBS -A $CHARGE_ACCOUNT" >> $SCRIPT_FILE_NAME
	fi
	echo '#PBS -e $PBS_JOBID.err' >> $SCRIPT_FILE_NAME
	echo '#PBS -o $PBS_JOBID.out' >> $SCRIPT_FILE_NAME
	if [ $ENABLE_EMAIL_NOTIFICATIONS ]; then
	    echo '#PBS -m bea' >> $SCRIPT_FILE_NAME
	    echo "#PBS -M $MYEMAIL" >> $SCRIPT_FILE_NAME
	fi
	echo "" >> $SCRIPT_FILE_NAME
	echo 'cd $PBS_O_WORKDIR' >> $SCRIPT_FILE_NAME
	echo "" >> $SCRIPT_FILE_NAME
	JOB_ID_NAME='${PBS_JOBID}'
	;;
    "pbs")
	echo "#PBS -N ${JOB_NAME}" >> $SCRIPT_FILE_NAME
        echo "#PBS -l nodes=${NODES}:ppn=40" >> $SCRIPT_FILE_NAME
        echo "#PBS -l walltime=${RUNTIME}" >> $SCRIPT_FILE_NAME
	echo "#PBS -A ${PSN}" >> $SCRIPT_FILE_NAME
	echo "#PBS -q ${QUEUE}" >> $SCRIPT_FILE_NAME
        echo '#PBS -e $PBS_JOBID.err' >> $SCRIPT_FILE_NAME
        echo '#PBS -o $PBS_JOBID.out' >> $SCRIPT_FILE_NAME
        if [ $ENABLE_EMAIL_NOTIFICATIONS ]; then
            echo '#PBS -m bea' >> $SCRIPT_FILE_NAME
            echo "#PBS -M $MYEMAIL" >> $SCRIPT_FILE_NAME
        fi
        echo "" >> $SCRIPT_FILE_NAME
        echo 'cd $PBS_O_WORKDIR' >> $SCRIPT_FILE_NAME
        echo "" >> $SCRIPT_FILE_NAME
	case ${FPD_CODE} in
             "parfu")
       		 echo 'module load /usr/local/apps/bioapps/modules/parfu/parfu' >> ${SCRIPT_FILE_NAME}
       		 ;;
             "ptgz")
       		 echo 'module load /usr/local/apps/bioapps/modules/ptgz/ptgz' >> ${SCRIPT_FILE_NAME}
       		 ;;
	esac
        JOB_ID_NAME='${PBS_JOBID}'
	;; 
    *)
	echo 'ERROR! $MANAGER='$MANAGER' which is not valid.  Aborting script generation.'
	exit;
	;;
esac
echo "" >> ${SCRIPT_FILE_NAME}

# variables used for configuration
# RANKS
# BASE_ARC_DIR
# STRIPE
# JOB_ID_VARIABLE
RANKS=$(( (NODES*RANKS_PER_NODE)/RANK_DIVISOR ))
#EXPANDED_RANKS=$(printf '%04d' "$RANKS")
echo 'RANKS='$RANKS >> ${SCRIPT_FILE_NAME}
echo "RANKS=${RANKS}" 
echo 'NODES='$NODES >> ${SCRIPT_FILE_NAME}
echo "BASE_ARC_DIR="$ARCDIR >> ${SCRIPT_FILE_NAME}
echo "BASE_TGT_DIR="$DATADIR >> ${SCRIPT_FILE_NAME}
EXPANDED_STRIPE=$(printf '%04d' "$STRIPE")
echo 'STRIPE="'$EXPANDED_STRIPE'"' >> ${SCRIPT_FILE_NAME}
echo 'FPD_DATASET="'${FPD_DATASET}'"' >> ${SCRIPT_FILE_NAME}
echo 'FPD_CODE="'${FPD_CODE}'"' >> ${SCRIPT_FILE_NAME}
EXPANDED_BLOCK=$(printf '%04d' "$BLOCK")
echo 'BLOCK="'$EXPANDED_BLOCK'"' >> ${SCRIPT_FILE_NAME}

# standardized system name 2018 AUG 16 by Craig
#echo 'MACH_FS="'$FPD_SYSTEM'_'$FS'"' >> ${SCRIPT_FILE_NAME}
echo 'MACH_FS="'$BASE_SYSTEM_NAME'_'$MANAGER'_T'$DATA_FS'_A'$ARC_FS'"' >> ${SCRIPT_FILE_NAME}

echo "" >> ${SCRIPT_FILE_NAME}

# set up the data file lines in the target script
DATA_FILE_NAME_PREFIX=$(printf 'FPD_test_%s_' "$FPD_SYSTEM" )
DATA_FILE_NAME=$DATA_FILE_NAME_PREFIX"${JOB_ID_NAME}.dat"

echo "TIMING_DATA_FILE=\"${DATA_FILE_NAME}\"" >> ${SCRIPT_FILE_NAME}
echo "" >> ${SCRIPT_FILE_NAME}
echo $'echo \"starting production running\"' >> ${SCRIPT_FILE_NAME}
echo $'echo \'${FPD_CODE} ${BLOCK}    ${MACH_FS}  ${FPD_DATASET}    ${STRIPE}    ${NODES} ${RANKS}    ${ITER} ${ELAP}\' >> ${TIMING_DATA_FILE}' >> ${SCRIPT_FILE_NAME} # 

#' (this line is to get the emacs bash parser to play ball.  It does nothing)

echo "" >> ${SCRIPT_FILE_NAME}

echo "ITER=0" >> ${SCRIPT_FILE_NAME}
echo "NUM_ITERATIONS="$ITERATIONS >> ${SCRIPT_FILE_NAME}
#echo 'echo "comparison: >$FPD_CODE< >tar<"' >> ${SCRIPT_FILE_NAME}
echo 'if [ "${FPD_CODE}" == "tar" ]; then' >> ${SCRIPT_FILE_NAME}
echo '   let RANKS=1' >> ${SCRIPT_FILE_NAME}
echo 'fi' >> ${SCRIPT_FILE_NAME}
echo "" >> ${SCRIPT_FILE_NAME}

# check for directories
echo 'mkdir -p output_files' >> ${SCRIPT_FILE_NAME}
echo 'TARGET_DIR="$BASE_TGT_DIR/${FPD_DATASET}_data/"' >> ${SCRIPT_FILE_NAME}
echo 'if [ ! -d "${TARGET_DIR}" ]; then' >> ${SCRIPT_FILE_NAME}
echo '    echo "data target dir ${TARGET_DIR} does not exist!"' >> ${SCRIPT_FILE_NAME}
echo '    exit'  >> ${SCRIPT_FILE_NAME}
echo 'fi' >> ${SCRIPT_FILE_NAME}
echo 'ARCHIVE_DIR="$BASE_ARC_DIR/arc_${STRIPE}"' >> ${SCRIPT_FILE_NAME}
echo 'if [ ! -d "${ARCHIVE_DIR}" ]; then' >> ${SCRIPT_FILE_NAME}
echo '    echo "archive dir ${ARCHIVE_DIR} does not exist!"' >> ${SCRIPT_FILE_NAME}
echo '    exit'  >> ${SCRIPT_FILE_NAME}
echo 'fi' >> ${SCRIPT_FILE_NAME}
echo "" >> ${SCRIPT_FILE_NAME}

# now the data-taking while loop
echo 'while [ $ITER -lt $NUM_ITERATIONS ]; do ' >> ${SCRIPT_FILE_NAME}
echo '    echo "starting iteration $ITER ranks $RANKS"' >> ${SCRIPT_FILE_NAME}
echo '    START=`date +%s`' >> ${SCRIPT_FILE_NAME}
case ${FPD_CODE} in
    "parfu")
      	echo '    '$MYMPIRUN_1'${RANKS}'$MYMPIRUN_2' parfu C $ARCHIVE_DIR/prod_'${JOB_ID_NAME}'_${ITER}.pfu $TARGET_DIR &> output_files/out_'${JOB_ID_NAME}'_${ITER}.out 2>&1' >> ${SCRIPT_FILE_NAME}
	;;
    "tar")
 	echo '    '$MYMPIRUN_1' 1 tar cf $ARCHIVE_DIR/prod_'${JOB_ID_NAME}'_${ITER}.tar $TARGET_DIR > output_files/out_'${JOB_ID_NAME}'_${ITER}.out 2>&1' >> ${SCRIPT_FILE_NAME}
	;;
    "ptgz")
	echo '    '$MYMPIRUN_1'$RANKS'$MYMPIRUN_2' ptgz -c -d $TARGET_DIR prod_'${JOB_ID_NAME}'_${ITER} &> output_files/out_'${JOB_ID_NAME}'_${ITER}.out 2>&1' >> ${SCRIPT_FILE_NAME}
	;;
esac
echo '    END=`date +%s`' >> ${SCRIPT_FILE_NAME}
echo '    ELAP=$(expr $END - $START)' >> ${SCRIPT_FILE_NAME}
echo '    echo "${FPD_CODE} ${BLOCK}    ${MACH_FS}  ${FPD_DATASET}    ${STRIPE}    ${NODES} ${RANKS}    ${ITER} ${ELAP}" >> ${TIMING_DATA_FILE}' >> ${SCRIPT_FILE_NAME}
echo '    let ITER=ITER+1' >> ${SCRIPT_FILE_NAME}
echo 'done' >> ${SCRIPT_FILE_NAME}
echo "" >> ${SCRIPT_FILE_NAME}

# now we set up the loop computation in the target script
