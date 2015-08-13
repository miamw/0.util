package max_min_packname;
import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.conf.Configuration;
public class max_min_feature extends Configured implements Tool{
/*find each column max and min.*/
/*input: fea1,fea2,fea3,label. ouput: fea1,max,min \n fea2, max, min.*/

/*map: fea1,value; fea2, value; fea3, value; fea1,value;.... */
	public static class max_min_Map extends Mapper<LongWritable, Text, IntWritable,DoubleWritable >  {
		
		public void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException {
			int i=0;
			double feature=0;
			String line=value.toString();
			StringTokenizer tokenizer=new StringTokenizer(line);
			while(tokenizer.hasMoreTokens()){
				feature = Double.parseDouble(tokenizer.nextToken());
				context.write(new IntWritable(i),new DoubleWritable(feature));
				i=i+1;
			}
		}
	}

        public static class max_min_Combiner extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
                public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
                        double maxValue = Double.MIN_VALUE;
                        double minValue = Double.MAX_VALUE;
                        for(DoubleWritable v : values){
                                maxValue = Math.max(maxValue, v.get());
                                minValue = Math.min(minValue,v.get());
                        }
			context.write(key,new DoubleWritable(maxValue));
			context.write(key,new DoubleWritable(minValue));
                        //String rst = Double.toString(maxValue) + "\t" + Double.toString(minValue);
                        //context.write(key, new Text(rst));
                }
        }


	public static class max_min_Reduce extends Reducer<IntWritable, DoubleWritable, IntWritable, Text> {
		public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
			double maxValue = Double.MIN_VALUE;
			double minValue = Double.MAX_VALUE;
			for(DoubleWritable v : values){
				maxValue = Math.max(maxValue, v.get());
				minValue = Math.min(minValue,v.get());
			}
			String rst = Double.toString(maxValue) + "\t" + Double.toString(minValue); 
			context.write(key, new Text(rst));
		}
	}
	/*hadoop jar jarname.jar classname inputpath outputpath queuename*/
        public int run(String[] args) throws Exception {
		Configuration conf=new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		System.out.println("arg num:"+otherArgs.length);
		System.out.println("arg1:"+otherArgs[0]);
		System.out.println("arg2:"+otherArgs[1]);
		System.out.println("arg3:");
		System.out.println(otherArgs[2]);
		conf.set("mapred.job.queue.name", otherArgs[2]);
		conf.set("mapreduce.job.reduces", "1000");
                conf.set("mapreduce.job.acl-view-job","*");
		Job job = new Job(conf);
		job.setJarByClass(max_min_feature.class);
		job.setJobName("max_min");
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		job.setMapperClass(max_min_Map.class);
		job.setCombinerClass(max_min_Combiner.class);
		job.setReducerClass(max_min_Reduce.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		return job.waitForCompletion(true)?0:1;
	}
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new max_min_feature(), args);
		System.exit(exitCode);
	}
}
