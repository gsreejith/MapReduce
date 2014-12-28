package com.sree.mapreduce.training.forums;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Filter {

	public static class MapperClass extends
			Mapper<LongWritable, Text, LongWritable, Text> {

		@Override
		public void map(LongWritable key, Text value, Mapper.Context context)
				throws IOException, InterruptedException {
			String[] fields = value.toString().split("\\t");
			if (fields.length == 19) {
				String text = fields[4];
				if (text.startsWith(".") || text.startsWith("?")
						|| text.startsWith("!")) {
					text = text.substring(1);
				}
				if (!text.isEmpty()) {
					context.write(key, new Text(text));
				}
			}
		}
	}

	public static class ReducerClass extends
			Reducer<LongWritable, Text, Text, Text> {

		@Override
		public void reduce(LongWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			for (Text value : values) {
				String body = value.toString().trim();
				String[] period = body.split("\\.");
				String[] question = body.split("\\?");
				String[] exclaim = body.split("!");
				int periodCount = period.length - 1;
				int questionCount = question.length - 1;
				int exclaimCount = exclaim.length - 1;
				if (periodCount <= 0 && questionCount <= 0 && exclaimCount <= 0) {
					context.write(null, new Text(body));
				}
			}
		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
		Job job = new Job(configuration, "Test");

		Path inputPath = new Path(
				"/home/cloudera/datasets/forum_data/forum_node.tsv");
		Path outputPath = new Path("/home/cloudera/datasets/forum_data/output");
		org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths(
				job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
