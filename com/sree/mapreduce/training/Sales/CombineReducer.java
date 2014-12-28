package com.sree.mapreduce.training.Sales;
import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class CombineReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		HashSet<String> nodeSet = new HashSet<>();
		HashSet<String> authorSet = new HashSet<>();
		String nodeTag = "node";
		String authorTag = "author";

		for (Text value : values) {
			String[] tokens = value.toString().split(",");
			if (tokens[0].equals(nodeTag)) {
				String str = value.toString().substring(nodeTag.length());
				nodeSet.add(str);
			} else if (tokens[0].equals(authorTag)) {
				String str = value.toString().substring(authorTag.length());
				authorSet.add(str);
			}
		}

		for (String left : nodeSet) {
			for (String right : authorSet) {
				context.write(null, new Text(left + right));
			}
		}
	}
}
