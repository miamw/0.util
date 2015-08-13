#!/bin/bash

## Host
host_from='webhdfs://mithrilblue-nn1.blue.ygrid.yahoo.com'	# M-Blue
host_to='webhdfs://dilithiumblue-nn1.blue.ygrid.yahoo.com'	# D-Blue

## Path
data_path='/tmp/wm/mdl_benz_app/'

################### Main

folder=0_trim.1_ovll

hadoop fs -mkdir $host_to/$data_path
hadoop fs -rm -r -skipTrash $host_to/$data_path/$folder
hadoop distcp -Dmapred.job.queue.name=apg_p7 $host_from/$data_path/$folder $host_to/$data_path
