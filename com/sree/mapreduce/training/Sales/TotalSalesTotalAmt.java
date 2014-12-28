package com.sree.mapreduce.training.Sales;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TotalSalesTotalAmt {

	public static class MapperClass extends
			Mapper<LongWritable, Text, IntWritable, DoubleWritable> {

		@Override
		public void map(LongWritable key, Text value,
				org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] tokens = line.split("\\t");
			if (tokens.length == 6) {
				if (null != value) {
					double amt = Double.valueOf(tokens[4]).doubleValue();
					context.write(new IntWritable(1), new DoubleWritable(amt));
				}
			}
		}
	}

	public static class ReducerClass extends
			Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {

		@Override
		public void reduce(IntWritable key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException {
			int count = 0;
			double amt = 0.0;

			for (DoubleWritable individual : values) {
				count++;
				amt += individual.get();
			}
			context.write(new IntWritable(count), new DoubleWritable(amt));
		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		Job job = new Job(configuration, "Test");
		Path inputPath = new Path("/home/cloudera/datasets/purchases.txt");
		Path outputPath = new Path("/home/cloudera/datasets/output");
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		job.setJarByClass(TotalSalesTotalAmt.class);
		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);
		job.setNumReduceTasks(1);
		// job.setCombinerClass(ReducerClass.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(DoubleWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
