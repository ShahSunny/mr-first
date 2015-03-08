package org.sunny.mr.first_mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
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
		Job job = Job.getInstance();
        job.setJobName("Word Count");

        //setting the class names
        job.setJarByClass(AppRunner.class);
        job.setMapperClass(MaxTemperatureMapper.class);
        job.setReducerClass(MaxTemperatureReducer.class);

        //setting the output data type classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //to accept the hdfs input and outpur dir at run time
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
    	System.exit(success ? 0 : 1);
		return 0;
	}
}