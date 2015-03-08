package org.sunny.mr.first_mr;

import org.apache.hadoop.util.ToolRunner;
import org.sunny.mr.first_mr.DI.MRModule;

import com.google.inject.Guice;
import com.google.inject.Injector;

public class App {
	public static void main(String[] args) throws Exception {
		Injector injector = Guice.createInjector(new MRModule());		 
		int exitcode = ToolRunner.run(injector.getInstance(AppRunner.class), args);
		System.exit(exitcode);
	}
}