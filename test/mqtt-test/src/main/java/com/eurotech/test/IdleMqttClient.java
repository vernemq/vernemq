/*
 * Copyright (c) 2013 Eurotech.
 */
package com.eurotech.test;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;

public class IdleMqttClient implements MqttTestConstants
{
	private static final MqttDefaultFilePersistence DATA_STORE = new MqttDefaultFilePersistence(TMP_DIR);

	private static MqttClient client;

	public static void main(String[] args) {
		try {
	    		MqttConnectOptions options = new MqttConnectOptions();
	    		options.setCleanSession(true);
	    		options.setKeepAliveInterval(CONNECTION_TIMEOUT * 5 / 1000);
	    		options.setWill(WILL_TOPIC, WILL_MESSAGE.getBytes(), 0, false);
	        	client = new MqttClient(BROKER_URL, "idle-mqtt-client", DATA_STORE);
	    		client.connect(options);
	    	
	    		Thread.sleep(60000);
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}
