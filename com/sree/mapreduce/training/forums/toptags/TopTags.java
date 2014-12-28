package com.sree.mapreduce.training.forums.toptags;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopTags {

	public static class MapperClass extends
			Mapper<LongWritable, Text, Text, MapWritable> {
		String questionNodeType = "\"question\"";

		HashMap<String, Integer> countMap = new HashMap<>();

		@Override
		public void map(LongWritable key, Text value, Context context) {
			String[] tokens = value.toString().split("\\t");
			if (tokens.length == 19) {
				String tag = tokens[2];
				String nodeType = tokens[5];
				if (nodeType.equalsIgnoreCase(questionNodeType)) {
					Integer tagCount = countMap.remove(tag);
					if (null == tagCount) {
						countMap.put(tag, Integer.valueOf(1));
					} else {
						countMap.put(tag,
								Integer.valueOf(tagCount.intValue() + 1));
					}
				}
			}
		}

		@Override
		public void cleanup(Context context) throws IOException,
				InterruptedException {
			TreeMap<Integer, String> topTen = new TreeMap<>();
			for (String value : countMap.keySet()) {
				topTen.put(countMap.get(value), value);
				if (topTen.size() > 10) {
					topTen.remove(topTen.firstKey());
				}
			}

			MapWritable mw = new MapWritable();
			for (Integer count : topTen.keySet()) {
				mw.put(new IntWritable(count.intValue()), new Text(topTen.get(count)));
				context.write(new Text(""), mw);
			}
		}
	}
	
	public static class ReducerClass extends
			Reducer<Text, MapWritable, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<MapWritable> values,
				Context context) throws IOException, InterruptedException {
			TreeMap<Integer, String> topTen = new TreeMap<>();

			for (MapWritable value : values) {
				Set<Writable> keys = value.keySet();
				for (Writable writableKey : keys) {
					IntWritable count = (IntWritable) writableKey;
					Text tag = (Text) value.get(writableKey);
					topTen.put(Integer.valueOf(count.get()), tag.toString());

					if (topTen.size() > 10) {
						topTen.remove(topTen.firstKey());
					}

				}
			}

			for (Integer topKey : topTen.keySet()) {
				context.write(new Text(topTen.get(topKey)), null);
			}
		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		Job job = new Job(configuration, "TopTenTags");
		job.setJarByClass(TopTags.class);

		Path inputPath = new Path(
				"/home/cloudera/datasets/forum_data/forum_node.tsv");
		Path outputPath = new Path("/home/cloudera/datasets/forum_data/output");

		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MapWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
