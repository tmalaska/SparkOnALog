package com.cloudera.sa.sparkonalog.flume.client;

import org.apache.flume.channel.MultiplexingChannelSelector;
import org.apache.flume.event.SimpleEvent;

public class RandomWordEventFlumeAvroClient extends SimpleFlumeAvroClient {

	protected static SimpleEvent generateEvent(int i) {
		SimpleEvent event = new SimpleEvent();
		
		MultiplexingChannelSelector v;
		
		char c1 = (char)(i % 26 + 65);
		char c2 = (char)(System.currentTimeMillis() % 26 + 65);
		
		String body = "Event body " + c1 + " " + c2;
		event.setBody(body.getBytes());
		return event;
	}
}
