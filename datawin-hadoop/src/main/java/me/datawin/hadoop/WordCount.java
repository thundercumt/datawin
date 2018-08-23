package me.datawin.hadoop;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int exit = ToolRunner.run(new WordCount(), args);
		System.exit(exit);
	}

	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
	
		JobConf job = new JobConf(new Configuration(), WordCount.class);
		job.setJarByClass(WordCount.class);
		job.setJobName("WordCounter");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormat(TextOutputFormat.class);
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
	
		RunningJob run = JobClient.runJob(job);
		run.waitForCompletion();
		System.out.println(run.isSuccessful());
		int ret = run.isSuccessful() ? 0 : 1;
		System.out.println("job.isSuccessful " + run.isSuccessful());
		return ret;
	}
	
	static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		 
		private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();
		@Override
		protected void map(LongWritable key, Text value,
				Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer st = new StringTokenizer(line," ");
			while(st.hasMoreTokens()){
				word.set(st.nextToken());
				context.write(word,one);
			}
		}
	}
	
	static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context)
				throws IOException, InterruptedException {
		
			int sum = 0;
			Iterator<IntWritable> valuesIt = values.iterator();
			while(valuesIt.hasNext()){
				sum = sum + valuesIt.next().get();
			}
			context.write(key, new IntWritable(sum));
		}	
	}

}
