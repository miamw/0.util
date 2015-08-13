#!/bin/bash

. /homes/mengwang/.bash_profile
. /homes/mengwang/ana/util/util.sh

queue1=apg_devshort_p1
queue2=apg_devmedium_p2
queue3=apg_qashort_p3
queue4=apg_devlarge_p4
queue5=apg_qa_p5
queue6=apg_int_p6
queue7=apg_p7

proj=mdl_yam_solu
work_dir=`pwd`
data_dir="/tmp/wm/$proj"
alias Rscript="/homes/mengwang/bin/R/bin/Rscript"

rm pig_[0-9]*.log &>/dev/null

if [[ x$1 != "xrun" ]]
then
	echo "Need param run!"
	exit
fi

############################ Func ############################


############################ Main ############################

for dat in 20140707 {20140701..20140706}
do
	f=0_trim;	run1day $dat $f "$data_dir/$f"
done
#	f=11_pair_list;	runPig "util/$f.pig" "$queue4" "-p dat=$dat" &>./log/$f.$dat
