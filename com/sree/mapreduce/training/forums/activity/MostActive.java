package com.sree.mapreduce.training.forums.activity;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MostActive {

	public static class MapperClass extends
			Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\\t");

			if (tokens.length == 19) {
				String author = tokens[3];
				String[] dateTokens = tokens[8].split(" ");

				if (dateTokens.length == 2) {
					String hour = dateTokens[1].split(":")[0];
					context.write(new Text(author), new Text(hour));
				}
			}
		}
	}

	public static class ReducerClass extends
			org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			HashMap<String, Integer> map = new HashMap<>();

			for (Text value : values) {
				String hour = value.toString();
				if (map.containsKey(hour)) {
					int count = map.remove(hour);
					map.put(hour, Integer.valueOf(count + 1));
				} else {
					map.put(hour, Integer.valueOf(1));
				}
			}

			int maxCount = 0;
			HashSet<String> outputSet = new HashSet<>();
			for (String hour : map.keySet()) {
				int count = map.get(hour);
				if (count > maxCount) {
					outputSet.clear();
					outputSet.add(hour);
				} else if (count == maxCount) {
					outputSet.add(hour);
				}
			}

			Iterator<String> iterator = outputSet.iterator();
			while (iterator.hasNext()) {
				context.write(key, new Text(iterator.next()));
			}
		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		Job job = new Job(configuration, "MostActive");

		Path inputPath = new Path(
				"/home/cloudera/datasets/forum_data/forum_node.tsv");
		Path outputPath = new Path("/home/cloudera/datasets/forum_data/output");

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		job.setJarByClass(MostActive.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
