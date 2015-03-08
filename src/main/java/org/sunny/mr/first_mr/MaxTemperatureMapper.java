package org.sunny.mr.first_mr;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.common.collect.Multiset.Entry;

public class MaxTemperatureMapper extends
		Mapper<LongWritable, Text, IntWritable, IntWritable> {
	public static class Record {
		public int year;
		public int temp;
		public Record(int year, int temp) {
			this.year = year;
			this.temp = temp;
		}
	}
	/*
	 * 	15 4 -> YEAR
	 *	19 2 -> MONTH
	 * 	21 2 -> DAY
	 *	88 4 -> TEMP
	 */
	private static int getTemp(String line) {
		int temprature = 1;
		int sign = 1;
		int signPos = 87;
		int signLength = 1;
		String strSign = line.substring(signPos, signPos + signLength);
		if(strSign == "-") {
			sign = -1;
		}
		int tempraturePosition = 88;
		int tempratureLength = 4;
		temprature = Integer.parseInt(line.substring(tempraturePosition, tempraturePosition + tempratureLength));
		return (temprature) * sign;
	}
	private static int getQuality(String line) {
		int qualityPos = 92;
		int qualityLength = 1;
		return Integer.parseInt(line.substring(qualityPos, qualityPos + qualityLength));
	}
	private static int getMonth(String line) {
		int monthPos = 19;
		int monthLength = 2;
		return Integer.parseInt(line.substring(monthPos, monthPos + monthLength));		
	}

	private static int getYear(String line) {
		int yearPos = 15;
		int yearLength = 4;
		return Integer.parseInt(line.substring(yearPos, yearPos + yearLength));
	}
	static Record processWeatherRecord(String line) {
		int 	year 	= getYear(line);
		//int 	month 	= getMonth(line);		
		int	temp	= getTemp(line);
		int quality 	= getQuality(line);
		if(year < 1900 || year > 2000) {
			System.out.println("Invalid year = " + year);
			return null;
		}
		if(temp == 9999 || temp == -9999) {
			System.out.println("Invalid temp" + temp);
			return null;
		}
		if(quality != 0 && quality != 1 && quality != 4 && quality != 5 && quality != 9) {
			System.out.println("Invalid quality " + quality + " , " + temp);
			return null;
		}				
		return new Record(year, temp);		
	}
	@Override
	public void map(LongWritable key, Text value, Context context)
	      throws IOException, InterruptedException {
	    
	    String line = value.toString();
	    Record r = processWeatherRecord(line);
	    if(r != null) {
	    	context.write(new IntWritable(r.year), new IntWritable(r.temp));
	    }
	  }
}
