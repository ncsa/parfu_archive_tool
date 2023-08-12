#!/usr/bin/perl
#////////////////////////////////////////////////////////////////////////////////
#// 
#//  University of Illinois/NCSA Open Source License
#//  http://otm.illinois.edu/disclose-protect/illinois-open-source-license
#//  
#//  Parfu is copyright (c) 2017-2022, 
#//  by The Trustees of the University of Illinois. 
#//  All rights reserved.
#//  
#//  Parfu was developed by:
#//  The University of Illinois
#//  The National Center For Supercomputing Applications (NCSA)
#//  Blue Waters Science and Engineering Applications Support Team (SEAS)
#//  Craig P Steffen <csteffen@ncsa.illinois.edu>
#//  Roland Haas <rhaas@illinois.edu>
#//  
#//  https://github.com/ncsa/parfu_archive_tool
#//  http://www.ncsa.illinois.edu/People/csteffen/parfu/
#//  
#//  For full licnse text see the LICENSE file provided with the source
#//  distribution.
#//  
#////////////////////////////////////////////////////////////////////////////////

# grabbing first and only argument
$main_script_fragment=$ARGV[0];

print STDERR "\nCLS consolitator script running.\n";

# If run without proper arguments, tell the user what arguments
# we're expecting.  Might at some point put a real "usage" here, which I suppose could also
# include a list of environment variables, since that's kind of important for this script.

if(!$main_script_fragment){
    print STDERR "\nThe first argument must be the name of your main script fragment!\n";
    print STDERR "You must also define these environment variables: \n";
    print STDERR "     CLS_STATIC_SYS_FILE CLS_STATIC_PERS_FILE\n";
    print STDERR "     CLS_ACTIVE_SYS_FILE CLS_ACTIVE_PERS_FILE\n";
    print STDERR "It is safe to point those at empty files if you don't have anything to put in them\n";
    print STDERR "  Exiting\n\n";
    exit;
}

################
# "static" here refers to file fragments that will go into the top section of the final
# job file, to contain lines preceeded #SBATCH that will be read and interpreted by the
# job submission system at submit time but ignored by the running job script at runtime

# "sys" refers to things that are defined for an allocation on a system that stays the same
# for all jobs by that group on that system.  
$my_sys_static_fragment=$ENV{CLS_STATIC_SYS_FILE};

# "pers" refers to lines that are unique to the user on that system, that contain their
# email address and email notification preferences. 
$my_pers_static_fragment=$ENV{CLS_STATIC_PERS_FILE};

################
# "active" here refers to lines that happen in the final job script after the last #SBATCH
# line.  So these are lines that define BASH variables that are ignored by the batch submission
# but set things up for the running bash job script.

# system-wide (or perhaps group-wide) active variable definitions
$my_sys_active_fragment=$ENV{CLS_ACTIVE_SYS_FILE};

# per-user active variable definitions
$my_pers_active_fragment=$ENV{CLS_ACTIVE_PERS_FILE};

################
# finally, the job fragment file, the thing that the user writes and sets up before each job,
# consists of two sections separated by a line with at least 5 equals signs in a row.  Their
# static job definitions are in the file before the "=====", and their active lines (presumably
# the bulk of what will be in the job script) will be after that.  So a simple job file
# might look like this: (lines start after the "|")
# |#SBATCH nodes=2
# |#SBATCH ntasks-per-node=6
# |===============
# |
# |srun -N12 --tasks-per-node=6 ${MY_BIN_DIR}/my_app.exe -output ${MY_OUTPUT_DIR}/my_app_output_${SLURM_JOBID}
# |

# the actual final job script will be assembled in the following order:

# Static sys fragment
# static personal fragment
# static section of job fragment file (the part before the "======")
# active sys fragment
# active personal fragment
# active job file fragment (the part after the "=====")


print "main script fragment: $main_script_fragment\n";
if(! -e $main_script_fragment){
    print STDERR "\n\nThe fragment file you specified: >$main_script_fragment< does not exist!\n";
    print STDERR "Exiting.\n\n";
    exit;
}

if(! $my_sys_static_fragment){
    print STDERR "\nEnv variable CLS_STATIC_SYS_FILE must point to the CLS static system localization\n";
    print STDERR "file.  For testing or initial deployment, it is safe to point this at an empty file.\n\n";
    exit;
}

if(! $my_pers_static_fragment){
    print STDERR "\nEnv variable CLS_STATIC_PERS_FILE must point to the CLS static personal localization\n";
    print STDERR "file.  For testing or initial deployment, it is safe to point this at an empty file.\n\n";
    exit;
}


if(! $my_sys_active_fragment){
    print STDERR "\nEnv variable CLS_ACTIVE_SYS_FILE must point to the CLS active system localization\n";
    print STDERR "file.  For testing or initial deployment, it is safe to point this at an empty file.\n\n";
    exit;
}

if(! $my_pers_active_fragment){
    print STDERR "\nEnv variable CLS_ACTIVE_PERS_FILE must point to the CLS active personal localization\n";
    print STDERR "file.  For testing or initial deployment, it is safe to point this at an empty file.\n\n";
    exit;
}


$check_equals_lines = `grep "=====" $main_script_fragment`;

if(!$check_equals_lines){
    print STDERR "\nOops!  Your script fragment file doesn't contain the \"=====\" separator!!!\n\n!";
    exit;
}

print "\n";

# insert both system and personal static fragments and output them 
open LOC_SYS_STATIC,"<$my_sys_static_fragment" or die "Could not open file >$my_sys_static_fragment< for reading!\n";
while(<LOC_SYS_STATIC>){
    print $_;
}
close LOC_SYS_STATIC;

open LOC_PERS_STATIC,"<$my_pers_static_fragment" or die "Could not open file >$my_pers_static_fragment< for reading!\n";
while(<LOC_PERS_STATIC>){
    print $_;
}
close LOC_PERS_STATIC;

# now grab the static part of the job file fragment:

# the static part of the fragment starts immediately.  It STOPS at the line of "=====".  
open MAIN_FRAG,"<$main_script_fragment" or die "Could not open main script fragment >$main_script fragment< for reading!\n";
$in_static_part=1;
while(<MAIN_FRAG>){
    if(! m/=====/){
	if($in_static_part){
	    print $_;
	}
    }
    else{
	$in_static_part=0;
    }
}

# insert both system and personal active fragments and output them 
open LOC_SYS_ACTIVE,"<$my_sys_active_fragment" or die "Could not open file >$my_sys_active_fragment< for reading!\n";
while(<LOC_SYS_ACTIVE>){
    print $_;
}
close LOC_SYS_ACTIVE;

open LOC_PERS_ACTIVE,"<$my_pers_active_fragment" or die "Could not open file >$my_pers_active_fragment< for reading!\n";
while(<LOC_PERS_ACTIVE>){
    print $_;
}
close LOC_PERS_ACTIVE;

# now grab the active part of the job file fragment:

# the active part of the fragment starts immediately.  It STOPS at the line of "=====".  
open MAIN_FRAG,"<$main_script_fragment" or die "Could not open main script fragment >$main_script fragment< for reading!\n";
$in_active_part=0;
while(<MAIN_FRAG>){
    if(! m/=====/){
	if($in_active_part){
	    print $_;
	}
    }
    else{
	$in_active_part=1;
    }
}

