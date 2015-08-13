f=3

hdfsroot=/user/mengwang/case_abf_BRPeak_paper/11_nb$f
in=$hdfsroot/../10_sampleip_info/

trf_th=10
c=1
#c=0.5799589
out="$hdfsroot/test"
#input_test="$in/20140109*"
input_test="$in/20140113*"

cache_root=$hdfsroot/cache
trainset_max_min_path="$hdfsroot/trainset_max_min"
trainset_max_min_cache=$cache_root/max_min.xls#max_min.xls
trainset_likilihood_path=$hdfsroot/trainset_likilihood
trainset_likilihood_cache=$cache_root/trainset_likilihood.xls#trainset_likilihood.xls
trainset_path="$hdfsroot/trainset"
testset_path="$out/testset"
test_score_label="$out/test_score_label"
test_score_label_normal="$out/test_score_label_normal"
test_score_label_normal_sample="$out/test_score_label_normal_sample"

queue=apg_devmedium_p2 #apg_devshort_p1 #apg_qa_p5 #apg_p7 #apg_devlarge_p4 #apg_devmedium_p2

###############hadoop_bayes, step 0: data preperation ####################
###### prepare train and test data #####
####trainset: fea1,fea2,..,label
####testset:fea1,fea2,...,label,ip
#pig -Dmapred.job.queue.name=$queue -Dmapreduce.job.acl-view-job=* -param input="$in/{20140106,20140107,20140108,20140109,20140110,20140111,20140112}*" -param output=$trainset_path -p trf_th=$trf_th pre_train_data$f.pig
#
#pig -Dmapred.job.queue.name=$queue -Dmapreduce.job.acl-view-job=* -param input="$input_test" -param output=$testset_path -p trf_th=$trf_th pre_test_data$f.pig
#
#echo "complete trainset and testset"
###############hadoop_bayes, step 1: get max and min of each feature of dataset####################
#hadoop fs -rm -r $trainset_max_min_path
#hadoop jar hadoop_bayes.jar max_min_packname.max_min_feature -D mapreduce.job.queuename=$queue -D mapreduce.job.reduces=1000 $trainset_path $trainset_max_min_path $queue
#
#hadoop fs -rm -r $cache_root
#hadoop fs -mkdir $cache_root
#
#hadoop fs -getmerge $trainset_max_min_path max_min.xls
#hadoop fs -put max_min.xls $cache_root
#echo "complete train part, get max,min of each feature"
#
###############hadoop_bayes, step 2: training, computing conditional probability ###########################
#hadoop fs -rm -r $trainset_likilihood_path
#hadoop jar hadoop_bayes.jar max_min_packname.train_set_normalize_likilihood_mod -D mapreduce.job.queuename=$queue -D mapreduce.job.reduces=1000 $trainset_path $trainset_likilihood_path $queue $trainset_max_min_cache max_min.xls #./max_min/part-r-00000
#
#hadoop fs -getmerge $trainset_likilihood_path trainset_likilihood.xls
#hadoop fs -put trainset_likilihood.xls $cache_root 
#echo "complete train part, get likilhood for each feature"
#
#
###############hadoop_bayes, step 3: predict for test data ####################
#hadoop fs -rm -r $test_score_label
#hadoop jar hadoop_bayes.jar max_min_packname.test_set_normalize_score -D mapreduce.job.acl-view-job=* -D mapreduce.job.queuename=$queue -D mapreduce.job.reduces=1000 $testset_path $test_score_label $queue $trainset_max_min_cache max_min.xls $trainset_likilihood_cache trainset_likilihood.xls
#echo "compute score"


#########normalize score to [0,1] ##################
pig -Dmapred.job.queue.name=$queue -Dmapreduce.job.acl-view-job=* -p c=$c -p input_dir=$test_score_label -p output_dir=$test_score_label_normal normalize_score.pig


######### sample
#sample_size=100000

#pig -Dmapreduce.job.queuename=$queue -param input_dir=$test_score_label_normal -param output_dir=$test_score_label_normal_sample -param sample_size=$sample_size sample.pig

#hadoop fs -cat $test_score_label_normal_sample/* > /tmp/wl/test_score_label_normal

#Rscript eval.r
#hadoop fs -chmod -R 777 $hdfsroot

