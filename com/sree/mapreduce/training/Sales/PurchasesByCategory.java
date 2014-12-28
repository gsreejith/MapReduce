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


public class PurchasesByCategory {

	public static class MapperClass extends
			Mapper<LongWritable, Text, Text, DoubleWritable> {

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws NumberFormatException, IOException, InterruptedException {
			String line = value.toString();
			String[] tokens = line.split("\\t");

			if (tokens.length == 6) {
				Text keyText = new Text(tokens[3]);
				float amt = Float.valueOf(tokens[4]);
				DoubleWritable floatValue = new DoubleWritable();
				floatValue.set(amt);
				context.write(keyText, floatValue);
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

			DoubleWritable result = new DoubleWritable();
			double sum = 0.0;
			for (DoubleWritable intermediate : values) {
				sum += intermediate.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		Job job = new Job(configuration, "Test");
		job.setJarByClass(PurchasesByCategory.class);
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
