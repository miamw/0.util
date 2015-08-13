#!/bin/bash

project=$1
fromPath='/homes/mengwang'
toPath='/homes/mengwang/wmTools'
timestamp="`date +%m%d%H%M`"

if [ -z $project ]
then
	echo "[ERROR] No Project Name!"
	exit -1
fi

mkdir -p $toPath/$project/$timestamp

# cp
cp -r $fromPath/$project/{*.sh,conf,util} $toPath/$project/$timestamp
if [ -d $fromPath/$project/template ]
then
	cp -r $fromPath/$project/template $toPath/$project/$timestamp
fi

# check result
if [ -d $toPath/$project/$timestamp ]
then
	echo "[INFO] Have backed up to $toPath/$project/$timestamp"
else
	echo "[ERROR]"
fi
