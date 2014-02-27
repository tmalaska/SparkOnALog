package com.cloudera.sa.sparkonalog;

import com.cloudera.sa.sparkonalog.flume.client.RandomWordEventFlumeAvroClient;
import com.cloudera.sa.sparkonalog.flume.client.SimpleFlumeAvroClient;
import com.cloudera.sa.sparkonalog.hbase.HBaseCreateTable;
import com.cloudera.sa.sparkonalog.spark.streaming.job.basic.SparkStreamingFromFlumeExample;
//import com.cloudera.sa.sparkonalog.spark.streaming.job.basic.SparkStreamingFromFlumeScalaExample;
import com.cloudera.sa.sparkonalog.spark.streaming.job.wordcount.SparkStreamingFromFlumeToHBaseExample;
import com.cloudera.sa.sparkonalog.spark.streaming.job.wordcount.SparkStreamingFromFlumeToHBaseWindowingExample;

public class Main {
	public static void main(String[] args) throws Exception {
		if (args.length == 0) {
			outputHelp();
			return;
		}

		String command = args[0];
		String[] subArgs = new String[args.length - 1];

		System.arraycopy(args, 1, subArgs, 0, subArgs.length);

		if (command.equals("SimpleFlumeAvroClient")) {
			SimpleFlumeAvroClient.main(subArgs);
		} else if (command.equals("RandomWordEventFlumeAvroClient")) {
			RandomWordEventFlumeAvroClient.main(subArgs);
		} else if (command.equals("HBaseCreateTable")) {
			HBaseCreateTable.main(subArgs);
			
		} else if (command.equals("SparkStreamingFromFlumeExample")) {
			SparkStreamingFromFlumeExample.main(subArgs);
		} else if (command.equals("SparkStreamingFromFlumeScalaExample")) {
			//SparkStreamingFromFlumeScalaExample.main(subArgs);
		} else if (command.equals("SparkStreamingFromFlumeToHBaseExample")) {
			SparkStreamingFromFlumeToHBaseExample.main(subArgs);
		} else if (command.equals("SparkStreamingFromFlumeToHBaseWindowingExample")){ 
			SparkStreamingFromFlumeToHBaseWindowingExample.main(subArgs);
		} else {
			outputHelp();
		}
	}

	private static void outputHelp() {
		System.out.println("---");
		System.out.println("SimpleFlumeAvroClient");
		System.out.println("RandomWordEventFlumeAvroClient");
		System.out.println("HBaseCreateTable");
		System.out.println("SparkStreamingFromFlumeExample");
		System.out.println("SparkStreamingFromFlumeScalaExample");
		System.out.println("SparkStreamingFromFlumeToHBaseExample");
		System.out.println("---");
	}
}
