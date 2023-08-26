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
