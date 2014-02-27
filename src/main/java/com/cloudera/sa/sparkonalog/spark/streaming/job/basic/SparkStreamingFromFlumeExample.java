package com.cloudera.sa.sparkonalog.spark.streaming.job.basic;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.flume.FlumeUtils;
import org.apache.spark.streaming.flume.SparkFlumeEvent;

public class SparkStreamingFromFlumeExample {
	public static void main(String[] args) {
		if (args.length == 0) {
			System.err
					.println("Usage: JavaFlumeEventCount <master> <host> <port>");
			System.exit(1);
		}

		String master = args[0];
		String host = args[1];
		int port = Integer.parseInt(args[2]);

		
		Duration batchInterval = new Duration(5000);

		System.out.println("-Starting Spark Context");
		System.out.println("-Spark_home:" + System.getenv("SPARK_HOME"));
		
		JavaStreamingContext sc = new JavaStreamingContext(master,
				"FlumeEventCount", batchInterval,
				System.getenv("SPARK_HOME"), "/home/cloudera/SparkOnALog.jar");

		//sc.ssc()
		
		//JavaDStream<SparkFlumeEvent> flumeStream = sc.flumeStream("localhost",
		//		port);

		System.out.println("-Setting up Flume Stream: " + host + " " + port);
		
		//JavaDStream<SparkFlumeEvent> flumeStream = sc.flumeStream(host, port);
		
		JavaDStream<SparkFlumeEvent> flumeStream = FlumeUtils.createStream(sc, host, port);
		
		//flumeStream.count();

		System.out.println("-count.map");
		
		flumeStream.count().print();
		
		flumeStream.count().map(new Function<Long, String>() {
			@Override
			public String call(Long in) {
				return "????????????? Received " + in + " flume events.";
			}
		}).print();

		System.out.println("-Starting Spark Context");
		
		sc.start();
		
		System.out.println("-Finished");
	}
}
