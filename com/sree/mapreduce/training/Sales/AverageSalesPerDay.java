package com.sree.mapreduce.training.Sales;
import java.io.IOException;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class AverageSalesPerDay {

	public static class MapperClass extends
			Mapper<LongWritable, Text, IntWritable, FloatWritable> {

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\\t");
			if (tokens.length == 6) {
				String[] dateSplits = tokens[0].split("-");
				if (dateSplits.length == 3) {
					Calendar calendar = Calendar.getInstance();
					calendar.set(Integer.valueOf(dateSplits[0]).intValue(),
							Integer.valueOf(dateSplits[1]).intValue(), Integer
									.valueOf(dateSplits[2]).intValue());
					FloatWritable saleAmt = new FloatWritable(Float.valueOf(
							tokens[4]).intValue());
					IntWritable dayOfWeek = new IntWritable(
							calendar.get(Calendar.DAY_OF_WEEK));
					context.write(dayOfWeek, saleAmt);
				}
			}
		}
	}

	public static class ReducerClass extends
			Reducer<IntWritable, FloatWritable, IntWritable, FloatWritable> {

		@Override
		public void reduce(IntWritable key, Iterable<FloatWritable> values,
				Context context) throws IOException, InterruptedException {
			float sum = 0f;
			int count = 0;

			for (FloatWritable value : values) {
				sum += value.get();
				count++;
			}
			float avg = sum / count;
			context.write(key, new FloatWritable(avg));
		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		Job job = new Job(configuration, "avgsales");
		job.setJarByClass(AverageSalesPerDay.class);
		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);
		job.setCombinerClass(ReducerClass.class);

		Path inputPath = new Path("/home/cloudera/datasets/purchases.txt");
		Path outputPath = new Path("/home/cloudera/datasets/output");

		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(FloatWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(FloatWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
