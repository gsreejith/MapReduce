package com.sree.mapreduce.training.forums.activity;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AuthorMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String tag = "author";
		String[] tokens = value.toString().split("\\t");

		if (tokens.length == 5) {
			String author = tokens[0];
			StringBuilder builder = new StringBuilder().append(tag).append(",")
					.append(tokens[1]).append(",").append(tokens[2])
					.append(",").append(tokens[3]).append(",")
					.append(tokens[4]);

			context.write(new Text(author), new Text(builder.toString()));
		}
	}
}
