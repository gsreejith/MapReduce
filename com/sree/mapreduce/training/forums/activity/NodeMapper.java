package com.sree.mapreduce.training.forums.activity;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NodeMapper extends Mapper<LongWritable, Text, Text, Text> {
	String tag = "node";

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] tokens = value.toString().split("\\t");
		if (tokens.length == 19) {
			String author_id = tokens[3];
			StringBuilder builder = new StringBuilder().append(tag).append(",")
					.append(tokens[0]).append(",").append(tokens[1])
					.append(",").append(tokens[2]).append(",")
					.append(tokens[3]).append(",").append(tokens[5])
					.append(",").append(tokens[6]).append(",")
					.append(tokens[7]).append(",").append(tokens[8])
					.append(",").append(tokens[9]);
			context.write(new Text(author_id), new Text(builder.toString()));
		}
	}
}
