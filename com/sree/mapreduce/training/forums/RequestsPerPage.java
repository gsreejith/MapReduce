package com.sree.mapreduce.training.forums;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RequestsPerPage {

	public static class MapperClass extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\"");
			IntWritable one = new IntWritable(1);

			if (tokens.length == 3) {
				String[] resources = tokens[1].split(" ");
				if (resources.length == 3) {
					String page = resources[1];
					context.write(new Text(page), one);
				}
			}
		}
	}

	public static class ReducerClass extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable individual : values) {
				sum += individual.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Path inputPath = new Path("/home/cloudera/datasets/access_log");
		Path outputPath = new Path("/home/cloudera/datasets/output");

		Configuration configuration = new Configuration();
		Job job = new Job(configuration, "Test");
		job.setJarByClass(RequestsPerPage.class);
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);
		job.setCombinerClass(ReducerClass.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
