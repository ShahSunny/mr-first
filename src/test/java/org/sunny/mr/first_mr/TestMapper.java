package org.sunny.mr.first_mr;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestMapper {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void processesValidRecord() throws IOException, InterruptedException {
	    Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
	                                  // Year ^^^^
	        "99999V0203201N00261220001CN9999999N9-00111+99999999999");
	                              // Temperature ^^^^^
	    new MapDriver<LongWritable, Text, Text, IntWritable>()
	      .withMapper(new MaxTemperatureMapper())
	      .withInput(new LongWritable(0), value)
	      .withOutput(new Text("1950"), new IntWritable(-11))
	      .runTest();
	  }
	
}
