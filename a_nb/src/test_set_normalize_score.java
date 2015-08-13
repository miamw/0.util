package max_min_packname;
import java.net.URI;
import java.net.URISyntaxException;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.GenericOptionsParser;
/*bucket test set and compute the score=log(p=good)+log(x|y=good) - (log(p=bad)+log(x|y=bad))*/
public class test_set_normalize_score extends Configured implements Tool{
	
	private static int bin_num=10;
	private static int max_col=200;
	private static double[] eachfeature_max;
	private static double[] eachfeature_min;
	private static double good_cnt=bin_num;
	private static double bad_cnt=bin_num;
	private static double[][] likilihood_under_good;
	private static double[][] likilihood_under_bad;
	
	
	public static void getEachFeatureMaxMin(String filename) throws IOException{
		eachfeature_max = new double[max_col];
		eachfeature_min = new double[max_col];
		System.out.println("file name:"+filename);
		BufferedReader in = null;
		try{
			in = new BufferedReader(new InputStreamReader(new FileInputStream(filename)));
			String line;
			int temp =0;
			while((line = in.readLine())!= null) {
			//	System.out.println("in getEachFeatureMaxMin Read line: " + line);
				String[] tokenizer = line.split("\t");
				temp=Double.valueOf(tokenizer[0]).intValue();
				eachfeature_max[temp] = Double.parseDouble(tokenizer[1]);
				eachfeature_min[temp] = Double.parseDouble(tokenizer[2]);
			}
		}finally{
			IOUtils.closeStream(in);
		}
	}
	
	/*input: (key(feature_number+"\t"+label+"\t"+"bucket"),value), output:likilihood_under_good, likilihood_under_bad, good_cnt, bad_cnt*/
	public static void getlikilihood(String filename) throws IOException{
		likilihood_under_good = new double[max_col][bin_num+1];		
		for(int i=0;i<max_col;i++){
			for(int j=0;j<=bin_num;j++){
			likilihood_under_good[i][j]=1;
			}
		}
		likilihood_under_bad = new double[max_col][bin_num+1];
		for(int i=0;i<max_col;i++){
			for(int j=0;j<=bin_num;j++){
			likilihood_under_bad[i][j]=1;
			}
		}

		BufferedReader in = null;
		try{
			in = new BufferedReader(new InputStreamReader(new FileInputStream(filename)));
			String line;

			while((line = in.readLine())!= null) {
				//System.out.println("in getlikilihood Read line: " + line);
				String[] tokenizer = line.split("\t");
				int field_num = tokenizer.length;
				if(field_num==2) {
					if(tokenizer[0].equals("0")) {bad_cnt=bad_cnt+Double.valueOf(tokenizer[1]).intValue();}
					else {good_cnt=good_cnt+Double.valueOf(tokenizer[1]).intValue();}
				}
				else{
					int feature_num = Double.valueOf(tokenizer[0]).intValue();
					int record_label = Double.valueOf(tokenizer[1]).intValue();
					int bucket_id = Double.valueOf(tokenizer[2]).intValue();
					int fea_lable_bucket_cnt = Double.valueOf(tokenizer[3]).intValue();
					if(record_label==0){
						likilihood_under_bad[feature_num][bucket_id]=likilihood_under_bad[feature_num][bucket_id]+fea_lable_bucket_cnt;
					}
					else {likilihood_under_good[feature_num][bucket_id]=likilihood_under_good[feature_num][bucket_id]+fea_lable_bucket_cnt;}
				}
			}
			for(int i=0;i<max_col;i++){
				for(int j=0;j<=bin_num;j++){
				//System.out.println(likilihood_under_bad[i][j]/bad_cnt+"\t"+likilihood_under_good[i][j]/good_cnt+"\n");
				likilihood_under_good[i][j]=Math.log(likilihood_under_good[i][j]/good_cnt);
				likilihood_under_bad[i][j]=Math.log(likilihood_under_bad[i][j]/bad_cnt);
				}
			}

			System.out.println(good_cnt+"\t"+bad_cnt+"\tpriori"+good_cnt/(good_cnt+bad_cnt)+"\t"+bad_cnt/(good_cnt+bad_cnt)+"\n");
			good_cnt=Math.log(good_cnt/(good_cnt+bad_cnt));
			bad_cnt=Math.log(bad_cnt/(good_cnt+bad_cnt));
			
		}finally{
			IOUtils.closeStream(in);
		}
	}
	
	static class test_set_mapper extends Mapper<LongWritable, Text, DoubleWritable,Text> {
		/*in main file set Configuration*/
		protected void setup(Context context) throws IOException, InterruptedException{
			System.out.println("setup filename:"+context.getConfiguration().get("feature.lable.bucket.file"));
			getlikilihood(context.getConfiguration().get("feature.lable.bucket.file"));
			System.out.println("setup filename:"+context.getConfiguration().get("max.min.file"));
			getEachFeatureMaxMin(context.getConfiguration().get("max.min.file"));
		}
		/*input:[each line format] feature1_value,feature2_value,feature3_value,label,uip; output:score+"\t"+label*/
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			int i = 0;
			double feature = 0;
			int bukid;
			String line=value.toString();
			//System.out.println("line: "+line);
			String[] tokenizer = line.split("\t");
			int col_num = tokenizer.length;
			int fea_num = col_num-2; //<x1,x2,...,label,uip>
			double bad_probability=bad_cnt;
			double good_probability=good_cnt;
			for(i=0;i<fea_num;i++) {
				feature = Double.parseDouble(tokenizer[i]);
				//System.out.println("feature num: " + i+"\t"+ "feature: " + feature);
				if(feature>eachfeature_min[i]&& feature<eachfeature_max[i]){
				feature = (feature - eachfeature_min[i])/(eachfeature_max[i]-eachfeature_min[i]);
				//System.out.println("feature num: " + i+"\t"+ "feature: " + feature+"\t"+"max: "+ eachfeature_max[i] +"\t"+ eachfeature_min[i]);
				feature = Math.floor(feature*bin_num);
				}
				else if(feature<=eachfeature_min[i]) {feature=0;}
				else {feature=bin_num;}
				bukid=(int)feature;
				bad_probability=bad_probability+likilihood_under_bad[i][bukid];
				good_probability=good_probability+likilihood_under_good[i][bukid];
			}
			double test_record_score=good_probability-bad_probability;
			context.write(new DoubleWritable(good_probability), new Text(bad_probability+"\t"+tokenizer[col_num-2]+"\t"+tokenizer[col_num-1]));//output score and label and uid	
		}
	}
	/*hadoop jar jarname.jar classname inputpath outputpath queuename max_min_cache max_min feature.lable.bucket_cache feature.lable.bucket*/	
	public int run(String[] args) throws Exception {
		Configuration conf=new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		conf.set("mapreduce.job.reduces", "1000");
		conf.set("mapreduce.job.acl-view-job","qiuxe");
		conf.set("mapred.job.queue.name", otherArgs[2]);
		conf.set("max.min.file", otherArgs[4]);
		conf.set("feature.lable.bucket.file", otherArgs[6]);
		try{
				DistributedCache.addCacheArchive(new URI(otherArgs[3]), conf);
				DistributedCache.addCacheArchive(new URI(otherArgs[5]), conf);
			}catch (URISyntaxException e) {
				e.printStackTrace();
			}
		Job job = new Job(conf);
		job.setJarByClass(test_set_normalize_score.class);
		job.setMapperClass(test_set_mapper.class);
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		return job.waitForCompletion(true)?0:1;
	}
	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new test_set_normalize_score(), args);
		System.exit(exitCode);
	}
}
