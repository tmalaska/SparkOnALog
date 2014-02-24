package com.cloudera.sa.sparkonalog.flume.interceptor;

import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

public class FlumeDelayInterceptor implements Interceptor {

	int sleepTime = 0;
	
	public FlumeDelayInterceptor(int sleepTime) {
		this.sleepTime = sleepTime; 
	}
	
	public void close() {
		// TODO Auto-generated method stub

	}

	public void initialize() {
		// TODO Auto-generated method stub
	}

	public Event intercept(Event events) {
		try {
			Thread.sleep(sleepTime);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return events;
	}

	public List<Event> intercept(List<Event> events) {
		for (Event event : events) {
			intercept(event);
		}
		return events;
	}

	/**
	 * Builder which builds new instance of the StaticInterceptor.
	 */
	public static class Builder implements Interceptor.Builder {

		int sleepTime;

		@Override
		public void configure(Context context) {
			sleepTime = Integer.parseInt(context.getString("sleep-time", "1000"));
		}

		@Override
		public Interceptor build() {
			return new FlumeDelayInterceptor(sleepTime);
		}
	}

}