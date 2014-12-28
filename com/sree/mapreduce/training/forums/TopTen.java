package com.sree.mapreduce.training.forums;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopTen {

	public static class MapperClass extends
			Mapper<LongWritable, Text, IntWritable, MapWritable> {

		private final TreeMap<Integer, String> bodyLengthMap = new TreeMap<>();
		IntWritable one = new IntWritable(1);

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			if (null != value) {
				String bodyString = value.toString();
				int length = bodyString.length();

				bodyLengthMap.put(length, bodyString);
				if (bodyLengthMap.size() > 10) {
					bodyLengthMap.remove(bodyLengthMap.firstKey());
				}
			}
		}

		@Override
		public void cleanup(Context context) throws IOException,
				InterruptedException {
			MapWritable mapWritable = new MapWritable();
			for (Map.Entry<Integer, String> entry : bodyLengthMap.entrySet()) {
				mapWritable.put(new IntWritable(entry.getKey()),
						new Text(entry.getValue()));
			}
			context.write(one, mapWritable);
		}
	}

	public static class ReducerClass extends
			Reducer<IntWritable, MapWritable, IntWritable, Text> {

		private final TreeMap<Integer, Text> bodyLengthMap = new TreeMap<>();

		@Override
		public void reduce(IntWritable key, Iterable<MapWritable> values,
				Context context) {
			for (MapWritable mapWritable : values) {
				for (Entry<Writable, Writable> entry : mapWritable.entrySet()) {
					Text mwValue = (Text) entry.getValue();
					int reducerKey = ((IntWritable) entry.getKey()).get();
					bodyLengthMap.put(new Integer(reducerKey), mwValue);
					if (bodyLengthMap.size() > 10) {
						bodyLengthMap.remove(bodyLengthMap.firstKey());
					}
				}
			}
		}

		@Override
		public void cleanup(Context context) throws IOException,
				InterruptedException {
			for (Integer mapKey : bodyLengthMap.keySet()) {
				context.write(null, bodyLengthMap.get(mapKey));
			}
		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		Job job = new Job(configuration, "TopEnn");
		job.setJarByClass(TopTen.class);

		Path inputPath = new Path(
				"/home/cloudera/datasets/forum_data/snippets.txt");
		Path outputPath = new Path("/home/cloudera/datasets/forum_data/output");

		org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths(
				job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);
		job.setNumReduceTasks(1);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(MapWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
