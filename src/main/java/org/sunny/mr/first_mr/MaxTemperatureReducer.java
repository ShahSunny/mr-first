package org.sunny.mr.first_mr;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTemperatureReducer extends
		Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
	@Override
	protected void reduce(IntWritable key, Iterable<IntWritable> values,
			Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context)
			throws IOException, InterruptedException {
		int maxTemprature = Integer.MIN_VALUE;
		for(IntWritable value : values) {
			maxTemprature = Math.max(value.get(), maxTemprature);			
		}
		context.write(key,new IntWritable(maxTemprature));
	}
}
