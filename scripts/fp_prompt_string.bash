#!/usr/bin/bash

if [ "$FP_FS_INDEX" ] ;
then
    echo -n "$FP_FS_INDEX"
else
    echo -n "X"
fi

if [ "$FP_ARC_TYPE" ] ;
then
    echo -n "$FP_ARC_TYPE"
else
    echo -n "X"
fi

if [ "$FP_TGT_TYPE" ] ;
then
    echo -n "$FP_TGT_TYPE"
else
    echo -n "X"
fi



#if [ ! "$FP_FS_INDEX" ] ;
#then
#    echo -n "XXX"
#else
#    echo -n "${FP_FS_INDEX}"
#    echo -n "$FP_TGT_TYPE[$FP_FS_INDEX]"
#    echo -n "$FP_ARC_TYPE[$FP_FS_INDEX]"
#fi

