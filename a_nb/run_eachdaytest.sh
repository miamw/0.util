##prepare train set
queue=apg_devmedium_p2 #apg_devshort_p1 #apg_qa_p5 #apg_p7 #apg_devlarge_p4 #apg_devmedium_p2

#root directory
hdfsroot=/user/mengwang/case_abf_BRPeak_paper/11_nb
cache_root=$hdfsroot/cache

trainset_max_min_cache=$cache_root/max_min.xls#max_min.xls
trainset_likilihood_cache=$cache_root/trainset_likilihood.xls#trainset_likilihood.xls

##### prepare train and test data #####

###testset:fea1,fea2,...,label,ip
for day in 20140108;do
	for hour in {00..23};do
		testset_path="$hdfsroot/testset_hourly_feature_label_ip/$day$hour"
		hadoop fs -rm -r $testset_path
		pig -Dmapred.job.queue.name=$queue -Dmapreduce.job.acl-view-job=* -param input="/tmp/wm/abf_ip/1_fea_hourly_coo/$day$hour/2_all" -param output=$testset_path pre_test_data.pig
		echo "complete testset"
		test_score_label="$hdfsroot/test_score_hourlylabel/$day$hour"
		hadoop fs -rm -r $test_score_label
		hadoop jar hadoop_bayes.jar max_min_packname.test_set_normalize_score -D mapreduce.job.acl-view-job=wl -D mapreduce.job.queuename=$queue -D mapreduce.job.reduces=1000 $testset_path $test_score_label $queue $trainset_max_min_cache max_min.xls $trainset_likilihood_cache trainset_likilihood.xls
		echo "complete bayes score"
	
		test_score_label_normal="$hdfsroot/test_score_hourlylabel_normal/$day$hour"
		pig -Dmapred.job.queue.name=$queue -Dmapreduce.job.acl-view-job=* -param input_dir=$test_score_label -param output_dir=$test_score_label_normal normalize_score_test.pig
		echo "$day$hour"
		echo "complete score"
	done
done
