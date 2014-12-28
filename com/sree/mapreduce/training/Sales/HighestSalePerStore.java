package com.sree.mapreduce.training.Sales;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HighestSalePerStore {

	public static class MapperClass extends
			Mapper<LongWritable, Text, Text, DoubleWritable> {

		@Override
		public void map(
				LongWritable key,
				Text value,
				org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] tokens = line.split("\\t");

			if (tokens.length == 6) {
				String store = tokens[2];
				double amt = Double.valueOf(tokens[4]);
				DoubleWritable doubleWritable = new DoubleWritable(amt);
				context.write(new Text(store), doubleWritable);
			}
		}
	}
	
	public static class ReducerClass extends
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		@Override
		public void reduce(
Text key, Iterable<DoubleWritable> values,
				Context context)
				throws IOException, InterruptedException {
			double max = 0.0;

			for (DoubleWritable individual : values) {
				double amt = individual.get();
				if (amt > max) {
					max = amt;
				}
			}
			context.write(key, new DoubleWritable(max));
		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		Job job = new Job(configuration, "Test");
		job.setJarByClass(HighestSalePerStore.class);
		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);
		job.setCombinerClass(ReducerClass.class);
		Path inputPath = new Path("/home/cloudera/datasets/purchases.txt");
		Path outputPath = new Path("/home/cloudera/datasets/ouput");
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
