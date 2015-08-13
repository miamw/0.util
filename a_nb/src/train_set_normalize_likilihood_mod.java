package max_min_packname;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.GenericOptionsParser;
/*get likilihood from train set, p(x=bucket|y=bad) and p(x=bucket|y=good)*/
public class train_set_normalize_likilihood_mod extends Configured implements Tool{
	private static double[] eachfeature_max;
	private static double[] eachfeature_min;
	private static int bin_num = 10;
	private static int max_col=200;
	/*read cache file. input:[format for each line is]feature_number\tMax_value\tMin_value, output: eachfeature_max and eachfeature_min*/
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
				System.out.println("Read line, " + temp+": "+line);
				String[] tokenizer = line.split("\t");
				temp=Double.valueOf(tokenizer[0]).intValue();
				eachfeature_max[temp] = Double.parseDouble(tokenizer[1]);
				eachfeature_min[temp] = Double.parseDouble(tokenizer[2]);
				
			}
		}finally{
			IOUtils.closeStream(in);
		}
	}
	
	static class train_set_normalizer_mod_mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		/*in main file set Configuration*/
		protected void setup(Context context) throws IOException, InterruptedException{
			System.out.println("setup filename:"+context.getConfiguration().get("max.min.file"));
			getEachFeatureMaxMin(context.getConfiguration().get("max.min.file"));
		}
		/*bucket the train set*/
		/*input:[each line format] feature1_value,feature2_value,feature3_value,label; output:feature_num+\t+label+\t+normalized_bucket_feature_value,1; for each line can generate 3 (key,value)*/
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			int i = 0;
			double feature = 0;
			String line=value.toString();
			//System.out.println("line: "+line);
			String[] tokenizer = line.split("\t");
			int col_num = tokenizer.length;
			int fea_num = col_num -1;
			for(i=0;i<fea_num;i++) {
				feature = Double.parseDouble(tokenizer[i]);
				//System.out.println("feature num: " + i+"\t"+ "feature: " + feature);
				feature = (feature - eachfeature_min[i])/(eachfeature_max[i]-eachfeature_min[i]);
				//System.out.println("feature num: " + i+"\t"+ "feature: " + feature+"\t"+"max: "+ eachfeature_max[i] +"\t"+ eachfeature_min[i]);
				feature = Math.floor(feature*bin_num);
				//System.out.println("Floor feature: " + feature);
				String rst = Integer.toString(i)+"\t"+tokenizer[fea_num]+"\t"+Integer.toString((int)feature);
				context.write(new Text(rst), new IntWritable(1));
			}
			context.write(new Text(tokenizer[fea_num]), new IntWritable(1));	
		}
	}
	
	static class train_set_likilihood_mod_reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
			/*input:feature_num+\t+label+\t+normalized_bucket_feature_value,1; output: feature_num+\t+label+\t+normalized_bucket_feature_value,count*/
			protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
				int i=0;
				for(IntWritable v : values){
						i=i+v.get();
				}
				context.write(new Text(key), new IntWritable(i));
			}
	}
	/*hadoop jar jarname.jar classname inputpath outputpath queuename max_min_path#max_min max_min*/
	public int run(String[] args) throws Exception {
		Configuration conf=new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		conf.set("mapred.job.queue.name", otherArgs[2]);
		conf.set("max.min.file",otherArgs[4]);
		conf.set("mapreduce.job.reduces", "1000");
                conf.set("mapreduce.job.acl-view-job","*");
                
		try{
		DistributedCache.addCacheArchive(new URI(otherArgs[3]), conf);
		}catch (URISyntaxException e) {
			e.printStackTrace();
		}
		
		Job job = new Job(conf);
		job.setJarByClass(train_set_normalize_likilihood_mod.class);
		job.setMapperClass(train_set_normalizer_mod_mapper.class);
		job.setCombinerClass(train_set_likilihood_mod_reduce.class);
		job.setReducerClass(train_set_likilihood_mod_reduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		return job.waitForCompletion(true)?0:1;
	}
	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new train_set_normalize_likilihood_mod(), args);
		System.exit(exitCode);
	}
}

	
