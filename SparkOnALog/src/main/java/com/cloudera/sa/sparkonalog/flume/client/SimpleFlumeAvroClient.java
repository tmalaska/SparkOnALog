package com.cloudera.sa.sparkonalog.flume.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.NettyAvroRpcClient;
import org.apache.flume.api.RpcClientConfigurationConstants;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.SimpleEvent;

public class SimpleFlumeAvroClient {
	public static void main(String[] args) throws FileNotFoundException,
			IOException, EventDeliveryException {
		if (args.length == 0) {
			System.out
					.println("AvroClient {host} {port} {numOfEvents}");
			return;
		}

		String host = args[0];
		int port = Integer.parseInt(args[1]);
		int numberOfEvents = Integer.parseInt(args[2]);

		Properties starterProp = new Properties();
		starterProp.setProperty(RpcClientConfigurationConstants.CONFIG_HOSTS, "h1");
	    starterProp.setProperty(RpcClientConfigurationConstants.CONFIG_HOSTS_PREFIX + "h1",  host + ":" + port);
	    
		NettyAvroRpcClient client = (NettyAvroRpcClient) RpcClientFactory
				.getInstance(starterProp);

		System.out.println("Starting");
		for (int i = 0; i < numberOfEvents; i++) {

			if (i%100 == 0) {
				System.out.print(".");
			}
			
			SimpleEvent event = generateEvent(i);

			client.append(event);
		}
		System.out.println();
		System.out.println("Done");
	}

	protected static SimpleEvent generateEvent(int i) {
		SimpleEvent event = new SimpleEvent();

		char c1 = (char) (i % 26 + 65);
		char c2 = (char) (System.currentTimeMillis() % 26 + 65);

		String body = "Event body " + c1 + " " + c2;
		event.setBody(body.getBytes());
		return event;
	}
}
