package com.cloudera.sa.sparkonalog.spark.streaming.job.basic;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.flume.FlumeUtils;
import org.apache.spark.streaming.flume.SparkFlumeEvent;

public class SparkStreamingFromFlumeExampleOld {
	public static void main(String[] args) {
		if (args.length == 0) {
			System.err
					.println("Usage: JavaFlumeEventCount <master> <host> <port> <nameOfJar>");
			System.exit(1);
		}

		String master = args[0];
		String host = args[1];
		int port = Integer.parseInt(args[2]);
		String nameOfJar = args[3];

		Duration batchInterval = new Duration(2000);

		System.out.println("-Starting Spark Context");
		
		
		JavaStreamingContext sc = new JavaStreamingContext(master,
				"FlumeEventCount", batchInterval, master, nameOfJar);

		//sc.ssc()
		
		//JavaDStream<SparkFlumeEvent> flumeStream = sc.flumeStream("localhost",
		//		port);

		System.out.println("-Setting up Flume Stream");
		
		JavaDStream<SparkFlumeEvent> flumeStream = FlumeUtils.createStream(sc, host, port);
		
		//flumeStream.count();

		System.out.println("-count.map");
		
		flumeStream.count().map(new Function<Long, String>() {
			@Override
			public String call(Long in) {
				return "Received " + in + " flume events.";
			}
		}).print();

		System.out.println("-Starting Spark Context");
		
		sc.start();
		
		System.out.println("-Finished");
	}
}
