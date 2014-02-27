package com.cloudera.sa.sparkonalog.flume.interceptor;


import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.HostInterceptor;
import org.apache.flume.interceptor.Interceptor;
import org.apache.hadoop.util.StringUtils;

import com.cloudera.sa.sparkonalog.hbase.HBaseCounterIncrementor;

public class FlumeHBaseWordCountInterceptor implements Interceptor {

	String tableName;
	String columnFamilyName;
	int flushInterval;
	HBaseCounterIncrementor incrementor;

	public FlumeHBaseWordCountInterceptor(String tableName,
			String columnFamilyName, int flushInterval) {
		this.tableName = tableName;
		this.columnFamilyName = columnFamilyName;
		this.flushInterval = flushInterval;
		incrementor = HBaseCounterIncrementor.getInstance(tableName, columnFamilyName);
	}

	@Override
	public void initialize() {
		
	}

	@Override
	public Event intercept(Event event) {

		try {
			String str = new String(event.getBody(), "UTF-8");
			String[] words = StringUtils.split(str, ' ');
			for (String word : words) {
				incrementor.incerment("counter", word, 1);
				
			}
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return event;
	}

	@Override
	public void close() {
		
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
			tableName = context.getString("hbaseTableName", "flumeCounter");
			columnFamilyName = context.getString("hbaseColumnFamily", "C");
			flushIntervals = Integer.parseInt(context.getString(
					"hbase-flush-intervals", "3000"));
		}

		@Override
		public Interceptor build() {
			return new FlumeHBaseWordCountInterceptor(tableName,
					columnFamilyName, flushIntervals);
		}
	}

	@Override
	public List<Event> intercept(List<Event> events) {
		// TODO Auto-generated method stub
		return events;
	}

	

	
}
