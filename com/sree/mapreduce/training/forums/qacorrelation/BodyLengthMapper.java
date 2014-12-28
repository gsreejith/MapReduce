package com.sree.mapreduce.training.forums.qacorrelation;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BodyLengthMapper extends Mapper<LongWritable, Text, Text, Text> {
	String questionNodeType = "\"question\"";
	String answerNodeType = "\"answer\"";

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] tokens = value.toString().split("\\t");
		if (tokens.length == 19) {
			String nodeId = tokens[0];
			String parentId = tokens[6];
			String nodeType = tokens[5];
			String body = tokens[4];
			StringBuilder builder;

			if (nodeType.equalsIgnoreCase(questionNodeType)
					|| nodeType.equalsIgnoreCase(answerNodeType)) {
				builder = new StringBuilder();
				builder.append(nodeType).append(",").append(body.length());
				String outputKey;
				if (nodeType.equals(questionNodeType)) {
					outputKey = nodeId;
				} else {
					outputKey = parentId;
				}
				context.write(new Text(outputKey), new Text(builder.toString()));
			}
		}
	}
}
