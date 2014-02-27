package com.cloudera.sa.sparkonalog.flume.interceptor;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map.Entry;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;

import com.cloudera.sa.sparkonalog.common.CounterMap;
import com.cloudera.sa.sparkonalog.common.CounterMap.Counter;

public class CopyOfFlumeHBaseWordCountInterceptor implements Interceptor {

	String tableName;
	String columnFamilyName;
	int flushInterval;
	HTable hTable;
	CounterMap counterMap = new CounterMap();
	FlushThread flushThread;

	public CopyOfFlumeHBaseWordCountInterceptor(String tableName,
			String columnFamilyName, int flushInterval) {
		this.tableName = tableName;
		this.columnFamilyName = columnFamilyName;
		this.flushInterval = flushInterval;
	}

	@Override
	public void initialize() {
		Configuration hConfig = HBaseConfiguration.create();
		try {
			hTable = new HTable(hConfig, tableName);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		flushThread = new FlushThread(flushInterval);
		flushThread.start();
	}

	@Override
	public Event intercept(Event event) {

		try {
			String str = new String(event.getBody(), "UTF-8");
			String[] words = StringUtils.split(str, ' ');
			for (String word : words) {
				counterMap.increment(word, 1);
			}
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return event;
	}

	@Override
	public void close() {
		try {
			hTable.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		flushThread.stopFlushingLoop();
	}

	/**
	 * Builder which builds new instance of the StaticInterceptor.
	 */
	public static class Builder implements Interceptor.Builder {

		String tableName;
		String columnFamilyName;
		int flushIntervals;

		@Override
		public void configure(Context context) {
			tableName = context.getString("hbase-table", "flumeCounter");
			columnFamilyName = context.getString("hbase-column-family", "C");
			flushIntervals = Integer.parseInt(context.getString(
					"hbase-flush-intervals", "3000"));
		}

		@Override
		public Interceptor build() {
			return new CopyOfFlumeHBaseWordCountInterceptor(tableName,
					columnFamilyName, flushIntervals);
		}
	}

	@Override
	public List<Event> intercept(List<Event> events) {
		// TODO Auto-generated method stub
		return events;
	}

	public class FlushThread extends Thread {
		long sleepTime;
		boolean continueLoop = true;

		public FlushThread(long sleepTime) {
			this.sleepTime = sleepTime;
		}

		@Override
		public void run() {
			while (continueLoop) {
				try {
					flushToHBase();	
				} catch (IOException e) {
					e.printStackTrace();
					break;
				}
				
				try {
					Thread.sleep(sleepTime);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		
		private void flushToHBase() throws IOException {
			CounterMap pastCounterMap = counterMap;
			counterMap = new CounterMap();

			Increment increment = new Increment(Bytes.toBytes("counters"));

			for (Entry<String, Counter> entry : pastCounterMap.entrySet()) {
				increment.addColumn(Bytes.toBytes("C"),
						Bytes.toBytes(entry.getKey()), entry.getValue().value);
			}
			hTable.increment(increment);
		}
		
		public void stopFlushingLoop() {
			continueLoop = false;
		}

	}

	
}
