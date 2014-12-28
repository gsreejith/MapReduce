package com.sree.mapreduce.training.forums;
import java.io.IOException;
import java.util.Locale;
import java.util.StringTokenizer;

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

public class InvertedIndexCount {

	public static class MapperClass extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			IntWritable one = new IntWritable(1);

			String[] tabbedTokens = value.toString().split("\\t");
			for (String tabbedToken : tabbedTokens) {
				String[] spacedTokens = tabbedToken.split(" ");
				for (String spacedToken : spacedTokens) {
					StringTokenizer tokenizer = new StringTokenizer(
							spacedToken, ".,!?:;\"()<>[]#$=-/");
					while (tokenizer.hasMoreTokens()) {
						String internalToken = tokenizer.nextToken();
						internalToken = internalToken.trim();
						internalToken = internalToken
								.toLowerCase(Locale.ENGLISH);
						context.write(new Text(internalToken), one);
					}
				}
			}
		}
	}

	public static class ReducerClass extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable value : values) {
				count++;
			}
			context.write(key, new IntWritable(count));
		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		Job job = new Job(configuration, "invindexcounts");
		job.setJarByClass(InvertedIndexCount.class);

		Path inputPath = new Path(
				"/home/cloudera/datasets/forum_data/forum_node.tsv");
		Path outputPath = new Path(
				"/home/cloudera/datasets/forum_data/outputcnt");
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
