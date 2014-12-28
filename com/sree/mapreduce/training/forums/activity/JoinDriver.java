package com.sree.mapreduce.training.forums.activity;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.sree.mapreduce.training.Sales.CombineReducer;

public class JoinDriver {

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		Job job = new Job(configuration, "ForumJoin");

		Path nodeData = new Path(
				"/home/cloudera/datasets/forum_data/forum_node.tsv");
		Path authorData = new Path(
				"/home/cloudera/datasets/forum_data/forum_users.tsv");
		Path outputPath = new Path("/home/cloudera/datasets/forum_data/output");

		MultipleInputs.addInputPath(job, nodeData, TextInputFormat.class,
				NodeMapper.class);
		MultipleInputs.addInputPath(job, authorData, TextInputFormat.class,
				AuthorMapper.class);

		FileOutputFormat.setOutputPath(job, outputPath);
		job.setReducerClass(CombineReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
