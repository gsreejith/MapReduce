package com.sree.mapreduce.training.forums.qacorrelation;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class QuestionAnswerReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int questionLength = 0;
		int answerLength = 0;
		int answerCount = 0;
		String questionNodeType = "\"question\"";
		String answerNodeType = "\"answer\"";

		for (Text value : values) {
			String[] tokens = value.toString().split(",");
			if (tokens.length == 2) {
				if (tokens[0].equals(questionNodeType)) {
					questionLength = Integer.valueOf(tokens[1]).intValue();
				} else if (tokens[0].equals(answerNodeType)) {
					answerLength += Integer.valueOf(tokens[1]).intValue();
					answerCount++;
				}
			}
		}
		float avgAnswerLength = ((float) answerLength) / answerCount;
		String output = questionLength + "	" + avgAnswerLength;
		context.write(key, new Text(output));
	}
}
