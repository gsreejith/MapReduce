package com.sree.mapreduce.training.forum.studygroups;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StudyGroups {

	public static class MapperClass extends
			Mapper<LongWritable, Text, Text, Text> {
		String questionTag = "\"question\"";

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\\t");

			if (tokens.length == 19) {
				String author = tokens[3];
				String nodeType = tokens[5];
				String nodeId;

				if (nodeType.equalsIgnoreCase(questionTag)) {
					nodeId = tokens[0];
				} else {
					nodeId = tokens[7];
				}
				context.write(new Text(nodeId), new Text(author));
			}
		}
	}

	public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			StringBuilder builder = new StringBuilder();
			builder.append(key).append("	");

			for (Text value : values) {
				builder.append(value.toString()).append(", ");
			}
			int endIndex = builder.length() - 2;
			String output = builder.substring(0, endIndex);
			context.write(null, new Text(output));
		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		Job job = new Job(configuration, "StudyGroups");
		job.setJarByClass(StudyGroups.class);

		Path inputPath = new Path(
				"/home/cloudera/datasets/forum_data/forum_node.tsv");
		Path outputPath = new Path("/home/cloudera/datasets/forum_data/output");

		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
