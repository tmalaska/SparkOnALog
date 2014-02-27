package com.cloudera.sa.sparkonalog.spark.streaming.job.wordcount;

import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.flume.FlumeUtils;
import org.apache.spark.streaming.flume.SparkFlumeEvent;
 
import scala.Tuple2;

import com.cloudera.sa.sparkonalog.hbase.HBaseCounterIncrementor;
//import com.cloudera.sa.sparkonalog.hbase.HBaseCounterIncrementor;
import com.google.common.base.Optional;

public class SparkStreamingFromFlumeToHBaseWindowingExample {

	public static void main(String[] args) {
		if (args.length == 0) {
			System.err
					.println("Usage: SparkStreamingFromFlumeToHBaseWindowingExample {master} {host} {port} {table} {columnFamily} {windowInSeconds} {slideInSeconds");
			System.exit(1);
		}

		String master = args[0];
		String host = args[1];
		int port = Integer.parseInt(args[2]);
		String tableName = args[3];
		String columnFamily = args[4];
		int windowInSeconds = Integer.parseInt(args[5]);
		int slideInSeconds = Integer.parseInt(args[5]);
		
		Duration batchInterval = new Duration(2000);
		Duration windowInterval = new Duration(windowInSeconds * 1000);
		Duration slideInterval = new Duration(slideInSeconds * 1000);

		JavaStreamingContext sc = new JavaStreamingContext(master,
				"FlumeEventCount", batchInterval,
				System.getenv("SPARK_HOME"), "/home/cloudera/SparkOnALog.jar");
		
		final Broadcast<String> broadcastTableName = sc.sparkContext().broadcast(tableName);
		final Broadcast<String> broadcastColumnFamily = sc.sparkContext().broadcast(columnFamily);
		
		//JavaDStream<SparkFlumeEvent> flumeStream = sc.flumeStream(host, port);
		
		JavaDStream<SparkFlumeEvent> flumeStream = FlumeUtils.createStream(sc, host, port);
		
		
		JavaPairDStream<String, Integer> lastCounts = flumeStream
				.flatMap(new FlatMapFunction<SparkFlumeEvent, String>() {

					@Override
					public Iterable<String> call(SparkFlumeEvent event)
							throws Exception {
						String bodyString = new String(event.event().getBody()
								.array(), "UTF-8");
						return Arrays.asList(bodyString.split(" "));
					}
				}).map(new PairFunction<String, String, Integer>() {
					@Override
					public Tuple2<String, Integer> call(String str)
							throws Exception {
						return new Tuple2(str, 1);
					}
				}).reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {

					@Override
					public Integer call(Integer x, Integer y) throws Exception {
						// TODO Auto-generated method stub
						return x.intValue() + y.intValue();
					}
				}, windowInterval, slideInterval);
				
				
				lastCounts.foreach(new Function2<JavaPairRDD<String,Integer>, Time, Void>() {

					@Override
					public Void call(JavaPairRDD<String, Integer> values,
							Time time) throws Exception {
						
						values.foreach(new VoidFunction<Tuple2<String, Integer>> () {

							@Override
							public void call(Tuple2<String, Integer> tuple)
									throws Exception {
								HBaseCounterIncrementor incrementor = 
										HBaseCounterIncrementor.getInstance(broadcastTableName.value(), broadcastColumnFamily.value());
								incrementor.incerment("Counter", tuple._1(), tuple._2());
								System.out.println("Counter:" + tuple._1() + "," + tuple._2());
								
							}} );
						
						return null;
					}});
		
		

		sc.start();

	}
}
