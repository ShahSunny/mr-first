package org.sunny.mr.first_mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;
import org.sunny.mr.first_mr.DI.InjectLogger;

import com.google.inject.Inject;

public class AppRunner extends Configured implements Tool {
	@InjectLogger Logger logger;
	@Inject
	public AppRunner() {
		super();
		
	}
	
	
	@Override
	public int run(String[] args) throws Exception {
		logger.debug("In AppRunner::Run");
		Configuration conf = getConf();
		conf.set("fs.defaultFS", "file:///");
		conf.set("mapreduce.framework.name", "local");
		setConf(conf);
		Job job = Job.getInstance(getConf());
        job.setJobName("Word Count");

        //setting the class names
        job.setJarByClass(AppRunner.class);
        job.setMapperClass(MaxTemperatureMapper.class);
        job.setReducerClass(MaxTemperatureReducer.class);
        job.setCombinerClass(MaxTemperatureReducer.class);
        job.setNumReduceTasks(1);
        //setting the output data type classes
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        Path[] paths = filterOutPaths(args[0]);
        //to accept the hdfs input and outpur dir at run time        
        FileInputFormat.setInputPaths(job, paths);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
    	System.exit(success ? 0 : 1);
		return 0;
	}


	private Path[] filterOutPaths(String path) {
		try {
			final FileSystem fs = FileSystem.get(getConf());
			FileStatus[] filesFound= fs.listStatus(new Path(path), new PathFilter() {
				public boolean accept(Path path) {
					try {
						FileStatus fileStatus = fs.getFileStatus(path);
						if(fileStatus.isDirectory() && path.getName().matches("^190.$")) {
							return true;
						} 
					} catch(IOException e) {
						
					} 
					return false;
				}
			});
			return FileUtil.stat2Paths(filesFound);			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		return null;
	}
}