# ************************** Data Format **************************

function getDateForm()
{
	time=$1

	if [[ $time =~ ^20[0-9][0-9][01][0-9][0-3][0-9]$ ]]
	then
		time1=$time
	elif [[ $time =~ ^20[0-9][0-9][01][0-9][0-3][0-9][0-2][0-9]$ ]]
	then
		time1=`echo $time | awk '{
			dat=int($1/100)
			hr=$1-dat*100
			print dat,hr
		}'`
	elif [[ $time =~ ^20[0-9][0-9][01][0-9][0-3][0-9][0-2][0-9][0-5][05]$ ]]
	then
		time1=`echo $time | awk '{
			dat=int($1/10000)
			hr=int(($1-dat*10000)/100)
			if(hr<10)
				hr="0"hr
			min=int($1-dat*10000-hr*100)
			if(min<10)
				min="0"min
			print dat,hr""min
		}'`
	else
		echo "[ERROR] Wrong Date Format!"
		exit -1 
	fi

	echo `date -d "$time1" +%s`
}

function getTimeList()
{
	timeStart=$1
	timeEnd=$2

	timeStart1=`getDateForm $timeStart` 
	timeEnd1=`getDateForm $timeEnd`     	

	timeString=$timeStart
	if [[ $timeStart =~ ^20[0-9][0-9][01][0-9][0-3][0-9]$ ]]
	then
		for((i=$timeStart1+86400;i<=$timeEnd1;i+=86400))
		do
			timeStamp=`date -d "@$i" +%Y%m%d`
			timeString="$timeString,$timeStamp"
		done
	elif [[ $timeStart =~ ^20[0-9][0-9][01][0-9][0-3][0-9][0-2][0-9]$ ]]
        then
        	for((i=$timeStart1+3600;i<=$timeEnd1;i+=3600))
        	do
        		timeStamp=`date -d "@$i" +%Y%m%d%H`
        		timeString="$timeString,$timeStamp"
        	done
	elif [[ $timeStart =~ ^20[0-9][0-9][01][0-9][0-3][0-9][0-2][0-9][0-5][05]$ ]]
	then
		for((i=$timeStart1+300;i<=$timeEnd1;i+=300))
		do                                           	
			timeStamp=`date -d "@$i" +%Y%m%d%H%M`
			timeString="$timeString,$timeStamp"  	
		done
	fi	
	echo $timeString
}

## getLookbackList	# timeEnd, nLookback, isFromThis, stepLength(1:hour, 2:day)
function getLookbackList() {
	timeEnd=$1
	nLookback=$2
	isFromThis=$3
	stepLength=$4	# 1:hour, 2:day

	timeEnd1=`getDateForm $timeEnd`

	if [[ $isFromThis == 0 ]]
	then
		timeString=""
		length=$nLookback
	elif [[ $isFromThis == 1 ]]
	then
		timeString="$timeEnd"
		length=$((nLookback-1))
	fi

	for((i=1;i<=length;i++))
	do
		if [[ $stepLength == 1 ]]
		then
			j=$((timeEnd1-3600*i))
			timeStamp=`date -d "@$j" +%Y%m%d%H`
		elif [[ $stepLength == 2 ]]
		then
			j=$((timeEnd1-86400*i))
			timeStamp=`date -d "@$j" +%Y%m%d%H`
		else
			echo "[ERROR] Wrong stepLength!!!"
			break
		fi
		timeString="$timeString,$timeStamp"
	done
	echo $timeString | sed 's/^,//g'
}

function handleSlash()
{
	input=$1

	if [[ !($input =~ \\\/) ]]
	then
		echo $input | sed 's/\//\\\//g'
	else
		echo $input
	fi                                 	
}

# transform time from number to : :
# need ts at 1st field
function readTs() {
	l_f=$1
	l_fld=$2
	l_ifTime13=$3

	sort -nk$l_fld $l_f > $l_f.0
	awk -F"\t" '{
      	 	for(i=1;i<'$l_fld'-1;i++)
      	 		printf("%s\t",$i) >"'$l_f'.11"
		if("'$l_fld'"!=1)
      	 		printf("%s\n",$('$l_fld'-1)) >"'$l_f'.11"
		print $'$l_fld'>"'$l_f'.12"
      	 	for(i=('$l_fld'+1);i<NF;i++)
      	 		printf("%s\t",$i) >"'$l_f'.13"
		if("'$l_fld'"!=NF)
      	 		printf("%s\n",$NF) >"'$l_f'.13"
	}' $l_f.0
	while read line
	do
		if [[ $l_ifTime13 == 1 ]]
		then
			date -d @$((line/1000)) +%k':'%M':'%S
		else
			date -d @$line +%k':'%M':'%S
		fi
	done < $l_f.12 > $l_f.22

	if [[ -f $l_f.11 ]] && [[ -f $l_f.13 ]]
	then
		paste $l_f.11 $l_f.22 $l_f.13 > $l_f.nmlTs
	elif [[ -f $l_f.11 ]]
	then
		paste $l_f.11 $l_f.22 > $l_f.nmlTs
	elif [[ -f $l_f.13 ]]
	then
		paste $l_f.22 $l_f.13 > $l_f.nmlTs
	else
		mv $l_f.22 $l_f.nmlTs
	fi

	rm $l_f.[0-9]*
}

# ************************** Run Pig **************************

function runPig() {
	pigFile_rp=$1
	queue_rp=$2
	param_rp=$3

	pig $param_rp \
		-Dmapreduce.job.queuename=$queue_rp \
		-Dmapreduce.job.acl-view-job=* \
		-Dmapred.min.split.size=$((2048*2048*2048)) \
		-Dmapred.job.map.memory.mb=3072 \
		-Dmapred.job.reduce.memory.mb=3072 \
		-Dmapred.task.timeout=1200000 \
	$pigFile_rp
}

function runPigComm() {
	l_dat=$1
	l_f=$2
	l_indir=$3
	l_param=$4
	l_suf=$5

	runPig ./util/$l_f.pig $queue4 "-p in_dir=$l_indir -p out_dir=$data_dir/$l_f$l_suf/$l_dat $l_param" &>log/$l_f$l_suf.$l_dat
}

# checkFinish	# 1:cntFile, 2:path
function checkFinish() {
	l_cntFile=$1
	l_path=$2

	flag=`hadoop fs -ls $l_path/*00000*  | wc -l`
	while [ ! 0$flag -eq 0$l_cntFile ]
	do
		sleep 1m
		flag=`hadoop fs -ls $l_path/*00000*  | wc -l`
	done
}

function run1h() {
	hr_r1=$1
	pigFile_r1=$2
	outPath_r1=$3
	out_check=$4
	para_r1=$5

	queue=$queue1;min=00;ts=$hr_r1$min;runPig ./util/$pigFile_r1.pig $queue "-p ts=$ts -p out_dir=$outPath_r1/$ts $para_r1" &>./log/$pigFile_r1.$ts & 
	queue=$queue3;min=10;ts=$hr_r1$min;runPig ./util/$pigFile_r1.pig $queue "-p ts=$ts -p out_dir=$outPath_r1/$ts $para_r1" &>./log/$pigFile_r1.$ts & 
	queue=$queue3;min=20;ts=$hr_r1$min;runPig ./util/$pigFile_r1.pig $queue "-p ts=$ts -p out_dir=$outPath_r1/$ts $para_r1" &>./log/$pigFile_r1.$ts & 
	queue=$queue5;min=30;ts=$hr_r1$min;runPig ./util/$pigFile_r1.pig $queue "-p ts=$ts -p out_dir=$outPath_r1/$ts $para_r1" &>./log/$pigFile_r1.$ts & 
	queue=$queue6;min=40;ts=$hr_r1$min;runPig ./util/$pigFile_r1.pig $queue "-p ts=$ts -p out_dir=$outPath_r1/$ts $para_r1" &>./log/$pigFile_r1.$ts & 
	queue=$queue7;min=50;ts=$hr_r1$min;runPig ./util/$pigFile_r1.pig $queue "-p ts=$ts -p out_dir=$outPath_r1/$ts $para_r1" &>./log/$pigFile_r1.$ts &

	checkFinish 6 "$outPath_r1/${hr_r1}*/$out_check"
}

function run1dayHive() {
	dat_r1=$1
	file_r1=$2
	para_r1=$3
	suffix_r1=$4

	mkdir -p ./log
	queue=$queue4;for hour in 00 08 16;do hive -d queue=$queue -d dt=$dat_r1 -d hour=$hour $para_r1 -f ./util/$file_r1.sql &>./log/${file_r1}${suffix_r1}.$dat_r1$hour;done & 
	queue=$queue1;for hour in 01 09 17;do hive -d queue=$queue -d dt=$dat_r1 -d hour=$hour $para_r1 -f ./util/$file_r1.sql &>./log/${file_r1}${suffix_r1}.$dat_r1$hour;done & 
	queue=$queue2;for hour in 02 10 18;do hive -d queue=$queue -d dt=$dat_r1 -d hour=$hour $para_r1 -f ./util/$file_r1.sql &>./log/${file_r1}${suffix_r1}.$dat_r1$hour;done & 
	queue=$queue3;for hour in 03 11 19;do hive -d queue=$queue -d dt=$dat_r1 -d hour=$hour $para_r1 -f ./util/$file_r1.sql &>./log/${file_r1}${suffix_r1}.$dat_r1$hour;done & 
	queue=$queue4;for hour in 04 12 20;do hive -d queue=$queue -d dt=$dat_r1 -d hour=$hour $para_r1 -f ./util/$file_r1.sql &>./log/${file_r1}${suffix_r1}.$dat_r1$hour;done & 
	queue=$queue5;for hour in 05 13 21;do hive -d queue=$queue -d dt=$dat_r1 -d hour=$hour $para_r1 -f ./util/$file_r1.sql &>./log/${file_r1}${suffix_r1}.$dat_r1$hour;done & 
	queue=$queue6;for hour in 06 14 22;do hive -d queue=$queue -d dt=$dat_r1 -d hour=$hour $para_r1 -f ./util/$file_r1.sql &>./log/${file_r1}${suffix_r1}.$dat_r1$hour;done & 
	queue=$queue7;for hour in 07 15 23;do hive -d queue=$queue -d dt=$dat_r1 -d hour=$hour $para_r1 -f ./util/$file_r1.sql &>./log/${file_r1}${suffix_r1}.$dat_r1$hour;done 
}

function run1day() {
	dat_r1=$1
	pigFile_r1=$2
	outPath_r1=$3
	out_check=$4
	para_r1=$5
	suf_r1=$6

	mkdir -p ./log
	queue=$queue4;for hr in 00 08 16;do ts=$dat_r1$hr;runPig ./util/$pigFile_r1.pig $queue "-p out_dir=$outPath_r1/$ts -p dt=$dat_r1 -p hour=$hr -p ts_list=${dat_r1}${hr} -p ts_schema=${dat}${hr}00 $para_r1" &>./log/$pigFile_r1.$dat_r1$hr$suf_r1;done & 
	queue=$queue1;for hr in 01 09 17;do ts=$dat_r1$hr;runPig ./util/$pigFile_r1.pig $queue "-p out_dir=$outPath_r1/$ts -p dt=$dat_r1 -p hour=$hr -p ts_list=${dat_r1}${hr} -p ts_schema=${dat}${hr}00 $para_r1" &>./log/$pigFile_r1.$dat_r1$hr$suf_r1;done & 
	queue=$queue2;for hr in 02 10 18;do ts=$dat_r1$hr;runPig ./util/$pigFile_r1.pig $queue "-p out_dir=$outPath_r1/$ts -p dt=$dat_r1 -p hour=$hr -p ts_list=${dat_r1}${hr} -p ts_schema=${dat}${hr}00 $para_r1" &>./log/$pigFile_r1.$dat_r1$hr$suf_r1;done & 
	queue=$queue3;for hr in 03 11 19;do ts=$dat_r1$hr;runPig ./util/$pigFile_r1.pig $queue "-p out_dir=$outPath_r1/$ts -p dt=$dat_r1 -p hour=$hr -p ts_list=${dat_r1}${hr} -p ts_schema=${dat}${hr}00 $para_r1" &>./log/$pigFile_r1.$dat_r1$hr$suf_r1;done & 
	queue=$queue4;for hr in 04 12 20;do ts=$dat_r1$hr;runPig ./util/$pigFile_r1.pig $queue "-p out_dir=$outPath_r1/$ts -p dt=$dat_r1 -p hour=$hr -p ts_list=${dat_r1}${hr} -p ts_schema=${dat}${hr}00 $para_r1" &>./log/$pigFile_r1.$dat_r1$hr$suf_r1;done & 
	queue=$queue5;for hr in 05 13 21;do ts=$dat_r1$hr;runPig ./util/$pigFile_r1.pig $queue "-p out_dir=$outPath_r1/$ts -p dt=$dat_r1 -p hour=$hr -p ts_list=${dat_r1}${hr} -p ts_schema=${dat}${hr}00 $para_r1" &>./log/$pigFile_r1.$dat_r1$hr$suf_r1;done & 
	queue=$queue6;for hr in 06 14 22;do ts=$dat_r1$hr;runPig ./util/$pigFile_r1.pig $queue "-p out_dir=$outPath_r1/$ts -p dt=$dat_r1 -p hour=$hr -p ts_list=${dat_r1}${hr} -p ts_schema=${dat}${hr}00 $para_r1" &>./log/$pigFile_r1.$dat_r1$hr$suf_r1;done & 
	queue=$queue7;for hr in 07 15 23;do ts=$dat_r1$hr;runPig ./util/$pigFile_r1.pig $queue "-p out_dir=$outPath_r1/$ts -p dt=$dat_r1 -p hour=$hr -p ts_list=${dat_r1}${hr} -p ts_schema=${dat}${hr}00 $para_r1" &>./log/$pigFile_r1.$dat_r1$hr$suf_r1;done & 

	sleep 10
	checkFinish 24 "$outPath_r1/${dat_r1}*/$out_check"
}

## run1dayList	# 1:dat, 2:lookback(1:last23+this, 2:last24, 3:last1), 3:filename, 4:pathOut, 5:folderCheck, 6:paramPig
function run1dayList()
{
	l_dat=$1
	l_lookback=$2
	l_filename=$3
	l_pathOut=$4
	l_folderCheck=$5
	l_paramPig=$6
	suf_r1=$7

	mkdir -p ./log
	## getLookbackList	# timeEnd, nLookback, isFromThis, stepLength(1:hour, 2:day)
	if [[ $l_lookback == 1 ]]	# last 23 hour & this hour
	then
		paramLookback="24 1"
	elif [[ $l_lookback == 2 ]]	# last 24 hour
	then
		paramLookback="24 0"
	elif [[ $l_lookback == 3 ]]	# last hour
	then
		paramLookback="1 0"
	fi

	queue=$queue4;for hr in 00 08 16;do ts=$l_dat$hr;ts_list=`getLookbackList $ts $paramLookback 1`;runPig ./util/$l_filename.pig $queue "-p dat=$l_dat -p hr=$hr -p ts_list=$ts_list -p out_dir=$l_pathOut/$ts $l_paramPig" &>./log/$l_filename.$l_dat$hr$suf_r1;done & 
	queue=$queue1;for hr in 01 09 17;do ts=$l_dat$hr;ts_list=`getLookbackList $ts $paramLookback 1`;runPig ./util/$l_filename.pig $queue "-p dat=$l_dat -p hr=$hr -p ts_list=$ts_list -p out_dir=$l_pathOut/$ts $l_paramPig" &>./log/$l_filename.$l_dat$hr$suf_r1;done & 
	queue=$queue2;for hr in 02 10 18;do ts=$l_dat$hr;ts_list=`getLookbackList $ts $paramLookback 1`;runPig ./util/$l_filename.pig $queue "-p dat=$l_dat -p hr=$hr -p ts_list=$ts_list -p out_dir=$l_pathOut/$ts $l_paramPig" &>./log/$l_filename.$l_dat$hr$suf_r1;done & 
	queue=$queue3;for hr in 03 11 19;do ts=$l_dat$hr;ts_list=`getLookbackList $ts $paramLookback 1`;runPig ./util/$l_filename.pig $queue "-p dat=$l_dat -p hr=$hr -p ts_list=$ts_list -p out_dir=$l_pathOut/$ts $l_paramPig" &>./log/$l_filename.$l_dat$hr$suf_r1;done & 
	queue=$queue4;for hr in 04 12 20;do ts=$l_dat$hr;ts_list=`getLookbackList $ts $paramLookback 1`;runPig ./util/$l_filename.pig $queue "-p dat=$l_dat -p hr=$hr -p ts_list=$ts_list -p out_dir=$l_pathOut/$ts $l_paramPig" &>./log/$l_filename.$l_dat$hr$suf_r1;done & 
	queue=$queue5;for hr in 05 13 21;do ts=$l_dat$hr;ts_list=`getLookbackList $ts $paramLookback 1`;runPig ./util/$l_filename.pig $queue "-p dat=$l_dat -p hr=$hr -p ts_list=$ts_list -p out_dir=$l_pathOut/$ts $l_paramPig" &>./log/$l_filename.$l_dat$hr$suf_r1;done & 
	queue=$queue6;for hr in 06 14 22;do ts=$l_dat$hr;ts_list=`getLookbackList $ts $paramLookback 1`;runPig ./util/$l_filename.pig $queue "-p dat=$l_dat -p hr=$hr -p ts_list=$ts_list -p out_dir=$l_pathOut/$ts $l_paramPig" &>./log/$l_filename.$l_dat$hr$suf_r1;done & 
	queue=$queue7;for hr in 07 15 23;do ts=$l_dat$hr;ts_list=`getLookbackList $ts $paramLookback 1`;runPig ./util/$l_filename.pig $queue "-p dat=$l_dat -p hr=$hr -p ts_list=$ts_list -p out_dir=$l_pathOut/$ts $l_paramPig" &>./log/$l_filename.$l_dat$hr$suf_r1;done & 

	sleep 10
	checkFinish 24 "$l_pathOut/${l_dat}*/$l_folderCheck"
}

# ************************** Ana **************************

function getStockChart()
{
	inputFile=$1
	fie=$2

	cat $inputFile | sort -nr | awk -F"\t" 'BEGIN{
		sum=0
		OFS="\t"
	}{
		sum+=$'$fie'
		print $0,sum
	}'
}

function getStockChart2()
{
	inputFile=$1
	key=$2
	trf=$3

	cat $inputFile | sort -nr | awk -F"\t" 'BEGIN{
		sum_key=0
		sum_trf=0
		OFS="\t"
	}{
		sum_key+=$'$key'
		sum_trf+=$'$trf'
		print $0,sum_key,sum_trf
	}'
}

# get sum
function getSum()
{
	inputFile=$1
	sumField=$2

	awk -F"\t" '{
		sum+=$'$sumField'
	}END{
		print sum
	}' $inputFile
}

# input: input, key, step, if(sort -n), trf
# output: group,cnt,cnt_sum,trf,trf_sum
function getDistribution1()
{
	inp=$1
	key=$2
	step=$3
	srt=$4
	trf=$5

	awk -F"\t" 'BEGIN{OFS="\t"}{
		keyBkt=int($'$key'/'$step')*'$step'

		cntTtl++
		cntK[keyBkt]++
		trfTtl+=$'$trf'
		trfK[keyBkt]+=$'$trf'
	}END{
		print cntTtl,trfTtl
		for(k in cntK) {
			print k,cntK[k]/cntTtl,trfK[k]/trfTtl
		}
	}' $inp > $inp.tmp1

	# sort
	if [[ $srt == 1 ]]
	then
		sort -nrk1 $inp.tmp1 > $inp.tmp2
	else
		sort -nk1 $inp.tmp1 > $inp.tmp2

	fi

	awk -F"\t" '{
		if(NF==2){
			print $0
		}else{
			cntSum+=$2
			trfSum+=$3

			printf("%s\t%f\t%f\t%f\t%f\n",$1,$2,cntSum,$3,trfSum)
		}
	}' $inp.tmp2 

	rm $inp.tmp[12]
}

# input: f1(x),f2(y)
function getDistribution2()
{
	stepSize=$1

	awk -F"\t" 'BEGIN{
		OFS="\t"
	}{
		ttl+=$2
		y[int($1/'$stepSize')]+=$2
	}END{
		for(key in y) {
			print key,int(y[key]*100/ttl)/100
		}
	}' | sort -k1 -n
}

# input: ip(16)(if ip10:split(ip,seg,".");ip_subnet=seg[1]"."seg[2]"."seg[3])
# output1: subnet, ip_cnt, 1_ip_sample
# output2: subnet_type, in%
function getSubnet() {
	inFile=$1

	awk -F"\t" 'BEGIN{OFS="\t"}function getSubnetB(ip){
		split(ip,seg,".")
		return seg[1]"."seg[2]
	}function getSubnetC(ip){
		split(ip,seg,".")
		return seg[1]"."seg[2]"."seg[3]
	}{
		ip=$1

		subnetB=getSubnetB(ip)
		subnetBCnt[subnetB]++
		subnetBIP[subnetB]=ip
	
		subnetC=getSubnetC(ip)
		subnetCCnt[subnetC]++
		subnetCIP[subnetC]=ip
	}END{
		for(key in subnetBCnt) {
			print key,subnetBCnt[key],subnetBIP[key] > "'$inFile'.subB"
		}
		for(key in subnetCCnt) {
			print key,subnetCCnt[key],subnetCIP[key] > "'$inFile'.subC"
		}
	}' $inFile

	rm $inFile.stats
	for f in subB subC
	do
		awk -F"\t" 'BEGIN{OFS="\t"}{
			t+=$2
			if($2>1)s+=$2
		}END{print "'$f'",s/t}' $inFile.$f >> $inFile.stats

		sort -nrk2 $inFile.$f > tmp
		mv tmp $inFile.$f
	done 
}

function trans16to10() {
	inFile=$1
	field=$2

	awk -F"\t" 'BEGIN{OFS="\t"}function trans(input){
		return strtonum("0x"input)
	}{
		ip=$'$field'

		for(i=1;i<=7;i+=2) {
			ip16[i]=substr(ip,i,2)
			ip10[i]=trans(ip16[i])
		}

		for(i=1;i<'$field';i++)
			printf("%s\t",$i)
		printf("%s.%s.%s.%s\t",ip10[1],ip10[3],ip10[5],ip10[7])
		for(i=('$field'+1);i<NF;i++)
			printf("%s\t",$i)
		printf("%s\n",$NF)
	}' $inFile
}

function ipConvert() {	
	inFile=$1
	l_fld=$2
	tran_type=$3
	needOri=$4

	awk -F"\t" 'BEGIN{OFS="\t"}
	function getSubnetB(ip){
		split(ip,seg,".")
		return seg[1]"."seg[2]
	}function getSubnetC(ip){
		split(ip,seg,".")
		return seg[1]"."seg[2]"."seg[3]
	}function ip16to10(ip){
		for(i=1;i<=7;i+=2) {
			ip16[i]=substr(ip,i,2)
			ip10[i]=strtonum("0x"ip16[i])
		}
		ret=ip10[1]
		for(i=3;i<=7;i+=2)
			ret=ret"."ip10[i]
		return ret
	}function ip2int(ip) {
	        ret=0
	        n=split(ip,a,".")
	        for(x=1;x<=n;x++)
			ret=or(lshift(ret,8),a[x])
	        return ret
	}function int2ip(ip) {
	        ret=and(ip,255)
	        ip=rshift(ip,8)
	        for(x=0;x<3;x++) {
			ret=and(ip,255)"."ret
			ip=rshift(ip,8)
		}
	        return ret
	}{
		if("'$tran_type'"==1) {
			ip=getSubnetC($'$l_fld')
		} else if("'$tran_type'"==2) {
			ip=int2ip($'$l_fld')
		} else if("'$tran_type'"==3) {
			ip=ip2int($'$l_fld')
		} else if("'$tran_type'"==4) {
			ip=ip16to10($'$l_fld')
      	 	}

		for(i=1;i<'$l_fld';i++)
      	 		printf("%s\t",$i)
		if("'$needOri'"==1)
			printf("%s\t%s",ip,$'$l_fld')
		else
			printf("%s",ip)
      	 	for(i=('$l_fld'+1);i<=NF;i++)
      	 		printf("\t%s",$i)
      	 	printf("\n")
	}' $inFile
}

# Precision & Recall, TP
function getIndicator() {
	inFile=$1

	awk -F"\t" 'BEGIN{OFS="\t"}{
		groundtruth=$1
		rule=$2
		trf=$3

		g+=trf;
		if(groundtruth==1)	g1+=trf;
		if(rule==1 && groundtruth==0)	r1g0+=trf;
		if(groundtruth==0)	g0+=trf;
		if(rule==1 && groundtruth==1)	r1g1+=trf;
		if(rule==1)	r1+=trf;
	}END{
		print "Rule","False Positive","Precision","Recall","Goldenset Size","Ground Truth Positive Rate","Discard Rate"
		print "'$inFile'",r1g0/g0,r1g1/r1,r1g1/g1,g,g1/g,r1/g
	}' $inFile
}

# Analyse pig log
anaPigLog() {
	inFile=$1

	awk -F"\t" 'BEGIN{OFS="\t"}{
		JobId=$1
		Maps=$2
		Reduces=$3
		MaxMapTime=$4
		MinMapTIme=$5
		AvgMapTime=$6
		MedianMapTime=$7
		MaxReduceTime=$8
		MinReduceTim=$9
		AvgReduceTime=$10
		MedianReducetime=$11
		Alias=$12
		Feature=$13
		Outputs=$14

		if($1~"^job_" && NF==14) {
			jobTime[JobId]=(MaxMapTime+MaxReduceTime)/60
			ttlTime+=jobTime[JobId]

			print jobTime[JobId],Alias,Feature
		}
	}END{
		print ttlTime,"Overall"
	}' $inFile | sort -nrk1
}
