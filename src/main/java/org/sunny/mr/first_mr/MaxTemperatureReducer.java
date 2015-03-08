package org.sunny.mr.first_mr;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTemperatureReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		int maxTemprature = Integer.MIN_VALUE;
		for(IntWritable value : values) {
			maxTemprature = Math.max(value, maxTemprature);			
		}
		context.write(key,new IntWritable(maxTemprature));
	}
}
