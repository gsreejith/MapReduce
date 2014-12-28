package com.sree.mapreduce.training.forums;
import java.io.IOException;
import java.util.Locale;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndexWordsToNodes {

	public static class MapperClass extends
			Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\\t");
			String nodeId = tokens[0];
			String[] spacedTokens = tokens[4].split(" ");
			for (String token : spacedTokens) {
				StringTokenizer tokenizer = new StringTokenizer(token,
						".,!?:;\"()<>[]#$=-/");
				while (tokenizer.hasMoreTokens()) {
					String internalToken = tokenizer.nextToken();
					internalToken = internalToken.trim();
					internalToken = internalToken.toLowerCase(Locale.ENGLISH);
					context.write(new Text(internalToken), new Text(nodeId));
				}
			}
		}
	}

	public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String writable = "";
			for (Text value : values) {
				writable = writable + value.toString() + ",";
			}
			writable = writable.substring(0, (writable.length() - 1));
			context.write(key, new Text(writable));
		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		Job job = new Job(configuration, "InvertedIndex");
		job.setJarByClass(InvertedIndexWordsToNodes.class);

		Path inputPath = new Path(
				"/home/cloudera/datasets/forum_data/forum_node.tsv");
		Path outputPath = new Path("/home/cloudera/datasets/forum_data/output");
		org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths(
				job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
