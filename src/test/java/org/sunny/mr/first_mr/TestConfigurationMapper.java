package org.sunny.mr.first_mr;


import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

public class TestConfigurationMapper {
	ConfigurationMapper conf;
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		conf = new ConfigurationMapper();
	}

	@After
	public void tearDown() throws Exception {
		conf = null;
	}

	@Test
	public void testInitialState() {
		String value = conf.getProperty("mapreduce.framework.name");
		assertThat(value, is(nullValue()));
	}
	@Test
	public void testStateAfterMapredDefaultAddition() {
		String value;
		conf.addConfFileFromClasspath("mapred-default.xml");
		value = conf.getProperty("mapreduce.framework.name");
		assertThat(value, is("local"));
		
		value = conf.getProperty("fs.defaultFS");
		assertThat(value, is("file:///"));
	}
	@Test
	public void testStateSet() {
		String value;
		conf.addConfFileFromClasspath("mapred-default.xml");
		conf.setProperty("mapreduce.framework.name", "yarn");
		value = conf.getProperty("mapreduce.framework.name");
		assertThat(value, is("yarn"));
		
		value = conf.getProperty("fs.defaultFS");
		assertThat(value, is("file:///"));
	}
	@Test
	public void testLocalConf() {
		String value;
		conf.addConfFile("/home/sunny/workspace/HadoopConf/LocalConf.xml");
		conf.setProperty("mapreduce.framework.name", "local");
		value = conf.getProperty("mapreduce.framework.name");
		assertThat(value, is("local"));
		
		value = conf.getProperty("fs.defaultFS");
		assertThat(value, is("file:///"));
	}
	
	@Test
	public void testLocalhostConf() {
		conf.addConfFile("/home/sunny/workspace/HadoopConf/LocalHostConf.xml");
		String value;
		
		value = conf.getProperty("fs.defaultFS");
		assertThat(value, is("hdfs://localhost/"));
		
		conf.setProperty("mapreduce.framework.name", "yarn");
		value = conf.getProperty("mapreduce.framework.name");
		assertThat(value, is("yarn"));
		
		
	}
	
	@Test
	public void testHadoopClusterConf() {
		
	}
}
