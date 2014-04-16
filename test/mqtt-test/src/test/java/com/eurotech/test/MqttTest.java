/*
 * Copyright (c) 2013 Eurotech.
 */
package com.eurotech.test;

import java.nio.file.Path;
import java.nio.file.Paths;

import junit.framework.TestCase;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;
import org.junit.After;
import org.junit.Before;

public class MqttTest extends TestCase implements MqttTestConstants, MqttCallback 
{	
	private static final MqttDefaultFilePersistence DATA_STORE = new MqttDefaultFilePersistence(TMP_DIR);
	
	private MqttClient client;
	private String expectedResult;
	private String result;
	
	@Before
    public void setUp() throws Exception {		
    	MqttConnectOptions options = new MqttConnectOptions();
    	options.setCleanSession(true);        
        client = new MqttClient(BROKER_URL, CLIENT_ID, DATA_STORE);
    	client.setCallback(this);
    	client.connect(options);
    }

	@After
    public void tearDown() throws Exception {
    	if (client != null && client.isConnected()) {
    		client.disconnect();
        	while(client.isConnected()) {
        		Thread.sleep(TIMEOUT);
        	}
    	}
    }

    public void testBasics() throws Exception {
    	
    	//check basic pub/sub on QOS 0
    	expectedResult = "hello mqtt broker on QOS 0";
    	result = null;
    	client.subscribe("1/2/3");
    	client.publish("1/2/3", expectedResult.getBytes(), 0, false);
    	Thread.sleep(TIMEOUT);
    	assertEquals(expectedResult, result);
    	assertTrue(client.getPendingDeliveryTokens().length == 0);
    	client.unsubscribe("1/2/3");
    	
    	//check basic pub/sub on QOS 1
    	expectedResult = "hello mqtt broker on QOS 1";
    	result = null;
    	client.subscribe("a/b/c");
    	client.publish("a/b/c", expectedResult.getBytes(), 1, false);
    	Thread.sleep(TIMEOUT);
    	assertEquals(expectedResult, result);
    	assertTrue(client.getPendingDeliveryTokens().length == 0);
    	client.unsubscribe("a/b/c");
    	
    	//check basic pub/sub on QOS 2
    	expectedResult = "hello mqtt broker on QOS 2";
    	result = null;
    	client.subscribe("1/2");
    	client.publish("1/2", expectedResult.getBytes(), 2, false);
    	Thread.sleep(TIMEOUT);
    	assertEquals(expectedResult, result);
    	assertTrue(client.getPendingDeliveryTokens().length == 0);
    	client.unsubscribe("1/2");
    }
    
    public void testLongTopic() throws Exception {
    	// *****************************************
    	// check a simple # subscribe works
    	// *****************************************
    	client.subscribe("1/2/3/this_is_a_long_topic_that_wasnt_working/before/4/5/6/7/8");
    	expectedResult = "hello mqtt broker on long topic";
    	result = null;
    	client.publish("1/2/3/this_is_a_long_topic_that_wasnt_working/before/4/5/6/7/8", expectedResult.getBytes(), 0, false);
    	Thread.sleep(TIMEOUT);
    	assertEquals(expectedResult, result);
    	assertTrue(client.getPendingDeliveryTokens().length == 0);
    	
    	client.unsubscribe("1/2/3/this_is_a_long_topic_that_wasnt_working/before/4/5/6/7/8");
    	client.subscribe("1/2/3/this_is_a_long_topic_that_wasnt_working/before/4/5/6/7/8/#");
    	
    	expectedResult = "hello mqtt broker on long topic with hash";
    	result = null;
    	client.publish("1/2/3/this_is_a_long_topic_that_wasnt_working/before/4/5/6/7/8", expectedResult.getBytes(), 0, false);
    	Thread.sleep(TIMEOUT);
    	assertEquals(expectedResult, result);
    	assertTrue(client.getPendingDeliveryTokens().length == 0);
    	
    	expectedResult = "hello mqtt broker on long topic with hash again";
    	result = null;
    	client.publish("1/2/3/this_is_a_long_topic_that_wasnt_working/before/4/5/6/7/8/9/10/0", expectedResult.getBytes(), 0, false);
    	Thread.sleep(TIMEOUT);
    	assertEquals(expectedResult, result);
    	assertTrue(client.getPendingDeliveryTokens().length == 0);
    	
    	client.unsubscribe("1/2/3/this_is_a_long_topic_that_wasnt_working/before/4/5/6/7/8/#");
    }
    
    public void testOverlappingTopics() throws Exception {
    	
    	// *****************************************
    	// check a simple # subscribe works
    	// *****************************************
    	client.subscribe("#");
    	expectedResult = "hello mqtt broker on hash";
    	result = null;
    	client.publish("a/b/c", expectedResult.getBytes(), 0, false);
    	Thread.sleep(TIMEOUT);
    	assertEquals(expectedResult, result);
    	assertTrue(client.getPendingDeliveryTokens().length == 0);
    	
    	expectedResult = "hello mqtt broker on a different topic";
    	result = null;
    	client.publish("1/2/3/4/5/6", expectedResult.getBytes(), 0, false);
    	Thread.sleep(TIMEOUT);
    	assertEquals(expectedResult, result);
    	assertTrue(client.getPendingDeliveryTokens().length == 0);
    	
    	// *****************************************
    	// now subscribe on a topic that overlaps the root # wildcard - we should still get everything
    	// *****************************************
    	client.subscribe("1/2/3");
    	expectedResult = "hello mqtt broker on explicit topic";
    	result = null;
    	client.publish("1/2/3", expectedResult.getBytes(), 0, false);
    	Thread.sleep(TIMEOUT);
    	assertEquals(expectedResult, result);
    	assertTrue(client.getPendingDeliveryTokens().length == 0);
    	
    	expectedResult = "hello mqtt broker on some other topic";
    	result = null;
    	client.publish("a/b/c/d/e", expectedResult.getBytes(), 0, false);
    	Thread.sleep(TIMEOUT);
    	assertEquals(expectedResult, result);
    	assertTrue(client.getPendingDeliveryTokens().length == 0);
    	
    	// *****************************************
    	// now unsub hash - we should only get called back on 1/2/3
    	// *****************************************
    	client.unsubscribe("#");
    	expectedResult = "this should not come back...";
    	result = null;
    	client.publish("1/2/3/4", expectedResult.getBytes(), 0, false);
    	Thread.sleep(TIMEOUT);
    	assertNull(result);
    	assertTrue(client.getPendingDeliveryTokens().length == 0);
    	
    	expectedResult = "this should not come back either...";
    	result = null;
    	client.publish("a/b/c", expectedResult.getBytes(), 0, false);
    	Thread.sleep(TIMEOUT);
    	assertNull(result);
    	assertTrue(client.getPendingDeliveryTokens().length == 0);
    	
    	// *****************************************
    	// this should still come back since we are still subscribed on 1/2/3
    	// *****************************************
    	expectedResult = "we should still get this";
    	result = null;
    	client.publish("1/2/3", expectedResult.getBytes(), 0, false);
    	Thread.sleep(TIMEOUT);
    	assertEquals(expectedResult, result);
    	assertTrue(client.getPendingDeliveryTokens().length == 0);
    	
    	// *****************************************
    	// remove the remaining subscription
    	// *****************************************
    	client.unsubscribe("1/2/3");
    	
    	// *****************************************
    	// repeat the above full test but reverse the order of the subs
    	// *****************************************
    	client.subscribe("1/2/3");
    	expectedResult = "hello mqtt broker on hash";
    	result = null;
    	client.publish("1/2/3", expectedResult.getBytes(), 0, false);
    	Thread.sleep(TIMEOUT);
    	assertEquals(expectedResult, result);
    	assertTrue(client.getPendingDeliveryTokens().length == 0);
    	
    	expectedResult = "hello mqtt broker on a different topic - we shouldn't get this";
    	result = null;
    	client.publish("1/2/3/4/5/6", expectedResult.getBytes(), 0, false);
    	Thread.sleep(TIMEOUT);
    	assertNull(result);
    	assertTrue(client.getPendingDeliveryTokens().length == 0);
    	
    	client.subscribe("#");
    	expectedResult = "hello mqtt broker on some other topic topic";
    	result = null;
    	client.publish("a/b/c/d", expectedResult.getBytes(), 0, false);
    	Thread.sleep(TIMEOUT);
    	assertEquals(expectedResult, result);
    	assertTrue(client.getPendingDeliveryTokens().length == 0);
    	
    	expectedResult = "hello mqtt broker on some other topic";
    	result = null;
    	client.publish("1/2/3/4/5/6", expectedResult.getBytes(), 0, false);
    	Thread.sleep(TIMEOUT);
    	assertEquals(expectedResult, result);
    	assertTrue(client.getPendingDeliveryTokens().length == 0);
    	
    	client.unsubscribe("1/2/3");
    	
    	expectedResult = "this should come back...";
    	result = null;
    	client.publish("1/2/3/4", expectedResult.getBytes(), 0, false);
    	Thread.sleep(TIMEOUT);
    	assertEquals(expectedResult, result);
    	assertTrue(client.getPendingDeliveryTokens().length == 0);
    	
    	expectedResult = "this should come back too...";
    	result = null;
    	client.publish("a/b/c", expectedResult.getBytes(), 0, false);
    	Thread.sleep(TIMEOUT);
    	assertEquals(expectedResult, result);
    	assertTrue(client.getPendingDeliveryTokens().length == 0);
    	
    	expectedResult = "we should still get this as well";
    	result = null;
    	client.publish("1/2/3", expectedResult.getBytes(), 0, false);
    	Thread.sleep(TIMEOUT);
    	assertEquals(expectedResult, result);
    	assertTrue(client.getPendingDeliveryTokens().length == 0);
    	
    	client.unsubscribe("#");
    }
    
    public void testDots() throws Exception {
    	
    	// *****************************************
    	// check that dots are not treated differently
    	// *****************************************
    	client.subscribe("1/2/./3");
    	expectedResult = "hello mqtt broker with a dot";
    	result = null;
    	client.publish("1/2/./3", expectedResult.getBytes(), 0, false);
    	Thread.sleep(TIMEOUT);
    	assertEquals(expectedResult, result);
    	assertTrue(client.getPendingDeliveryTokens().length == 0);
    	
    	expectedResult = "should not get this";
    	result = null;
    	client.publish("1/2", expectedResult.getBytes(), 0, false);
    	Thread.sleep(TIMEOUT);
    	assertNull(result);
    	assertTrue(client.getPendingDeliveryTokens().length == 0);
    	
    	expectedResult = "should not get this";
    	result = null;
    	client.publish("1/2/3", expectedResult.getBytes(), 0, false);
    	Thread.sleep(TIMEOUT);
    	assertNull(result);
    	assertTrue(client.getPendingDeliveryTokens().length == 0);
    	
    	expectedResult = "should not get this";
    	result = null;
    	client.publish("1/2/./3/4", expectedResult.getBytes(), 0, false);
    	Thread.sleep(TIMEOUT);
    	assertNull(result);
    	assertTrue(client.getPendingDeliveryTokens().length == 0);
    	
    	client.unsubscribe("1/2/./3");
    	
    	// *****************************************
    	// check that we really are unsubscribed now
    	// *****************************************
    	expectedResult = "should not get this";
    	result = null;
    	client.publish("1/2/./3", expectedResult.getBytes(), 0, false);
    	Thread.sleep(TIMEOUT);
    	assertNull(result);
    	assertTrue(client.getPendingDeliveryTokens().length == 0);
    	expectedResult = "should not get this";
    	result = null;
    	client.publish("1/2/3", expectedResult.getBytes(), 0, false);
    	Thread.sleep(TIMEOUT);
    	assertNull(result);
    	assertTrue(client.getPendingDeliveryTokens().length == 0);
    	expectedResult = "should not get this";
    	result = null;
    	client.publish("1/2/./3/4", expectedResult.getBytes(), 0, false);
    	Thread.sleep(TIMEOUT);
    	assertNull(result);
    	assertTrue(client.getPendingDeliveryTokens().length == 0);
    }
    
    public void testActiveMQWildcards() throws Exception {
    	
    	// *****************************************
    	// check that ActiveMQ native wildcards are not treated differently
    	// *****************************************
    	client.subscribe("*/>/#");
    	expectedResult = "hello mqtt broker with fake wildcards";
    	result = null;
    	client.publish("*/>/1/2/3", expectedResult.getBytes(), 0, false);
    	Thread.sleep(TIMEOUT);
    	assertEquals(expectedResult, result);
    	assertTrue(client.getPendingDeliveryTokens().length == 0);
    	
    	expectedResult = "should not get this";
    	result = null;
    	client.publish("1", expectedResult.getBytes(), 0, false);
    	Thread.sleep(TIMEOUT);
    	assertNull(result);
    	assertTrue(client.getPendingDeliveryTokens().length == 0);
    	
    	expectedResult = "should not get this";
    	result = null;
    	client.publish("1/2", expectedResult.getBytes(), 0, false);
    	Thread.sleep(TIMEOUT);
    	assertNull(result);
    	assertTrue(client.getPendingDeliveryTokens().length == 0);

    	expectedResult = "should not get this";
    	result = null;
    	client.publish("1/2/3", expectedResult.getBytes(), 0, false);
    	Thread.sleep(TIMEOUT);
    	assertNull(result);
    	assertTrue(client.getPendingDeliveryTokens().length == 0);
    	
    	expectedResult = "should not get this";
    	result = null;
    	client.publish("*/2", expectedResult.getBytes(), 0, false);
    	Thread.sleep(TIMEOUT);
    	assertNull(result);
    	assertTrue(client.getPendingDeliveryTokens().length == 0);
    	
    	expectedResult = "should get this";
    	result = null;
    	client.publish("*/>/3", expectedResult.getBytes(), 0, false);
    	Thread.sleep(TIMEOUT);
    	assertEquals(expectedResult, result);
    	assertTrue(client.getPendingDeliveryTokens().length == 0);
    	
    	expectedResult = "should get this";
    	result = null;
    	client.publish("*/>", expectedResult.getBytes(), 0, false);
    	Thread.sleep(TIMEOUT);
    	assertEquals(expectedResult, result);
    	assertTrue(client.getPendingDeliveryTokens().length == 0);
    }

    public void testNativeMQTTWildcards() throws Exception {
    	
    	// *****************************************
    	// check that hash works right with plus
    	// *****************************************    	
    	client.subscribe("a/+/#");
    	expectedResult = "sub on everything below a but not a";
    	result = null;
    	client.publish("a/b", expectedResult.getBytes(), 0, false);
    	Thread.sleep(TIMEOUT);
    	assertEquals(expectedResult, result);
    	assertTrue(client.getPendingDeliveryTokens().length == 0);

    	expectedResult = "should not get this";
    	result = null;
    	client.publish("a", expectedResult.getBytes(), 0, false);
    	Thread.sleep(TIMEOUT);
    	assertNull(result);
    	assertTrue(client.getPendingDeliveryTokens().length == 0);
    	
    	// change sub to just a/#
    	client.unsubscribe("a/+/#");
    	client.subscribe("a/#");
    	
    	expectedResult = "sub on everything below a including a";
    	result = null;
    	client.publish("a/b", expectedResult.getBytes(), 0, false);
    	Thread.sleep(TIMEOUT);
    	assertEquals(expectedResult, result);
    	assertTrue(client.getPendingDeliveryTokens().length == 0);
    	
    	expectedResult = "sub on everything below a still including a";
    	result = null;
    	client.publish("a", expectedResult.getBytes(), 0, false);
    	Thread.sleep(TIMEOUT);
    	assertEquals(expectedResult, result);
    	assertTrue(client.getPendingDeliveryTokens().length == 0);
    	
    	expectedResult = "sub on everything below a still including a - should not get b";
    	result = null;
    	client.publish("b", expectedResult.getBytes(), 0, false);
    	Thread.sleep(TIMEOUT);
    	assertNull(result);
    	assertTrue(client.getPendingDeliveryTokens().length == 0);
    	
    	client.unsubscribe("a/#");    	
    }
    
    public void testWildcardPlus() throws Exception {
    	
    	// *****************************************
    	// check that unsub of hash doesn't affect other subscriptions
    	// *****************************************
    	client.subscribe("+/+/+");
    	expectedResult = "should get this 1";
    	result = null;
    	client.publish("1/2/3", expectedResult.getBytes(), 0, false);
    	Thread.sleep(TIMEOUT);
    	assertEquals(expectedResult, result);
    	assertTrue(client.getPendingDeliveryTokens().length == 0);
    	
    	expectedResult = "should get this 2";
    	result = null;
    	client.publish("a/2/3", expectedResult.getBytes(), 0, false);
    	Thread.sleep(TIMEOUT);
    	assertEquals(expectedResult, result);
    	assertTrue(client.getPendingDeliveryTokens().length == 0);
    	
    	expectedResult = "should get this 3";
    	result = null;
    	client.publish("1/b/c", expectedResult.getBytes(), 0, false);
    	Thread.sleep(TIMEOUT);
    	assertEquals(expectedResult, result);
    	assertTrue(client.getPendingDeliveryTokens().length == 0);
    	
    	expectedResult = "should not get this";
    	result = null;
    	client.publish("1/2", expectedResult.getBytes(), 0, false);
    	Thread.sleep(TIMEOUT);
    	assertNull(result);
    	assertTrue(client.getPendingDeliveryTokens().length == 0);
    	
    	expectedResult = "should not get this either";
    	result = null;
    	client.publish("1/2/3/4", expectedResult.getBytes(), 0, false);
    	Thread.sleep(TIMEOUT);
    	assertNull(result);
    	assertTrue(client.getPendingDeliveryTokens().length == 0);
    }
    
    public void testSubs() throws Exception {
    	client.subscribe("1/2/3");
    	client.subscribe("a/+/#");
    	client.subscribe("#");
    	
    	expectedResult = "should get everything";
    	result = null;
    	client.publish("1/2/3/4", expectedResult.getBytes(), 0, false);
    	Thread.sleep(TIMEOUT);
    	assertEquals(expectedResult, result);
    	assertTrue(client.getPendingDeliveryTokens().length == 0);
    	
    	expectedResult = "should get everything";
    	result = null;
    	client.publish("a/1/2", expectedResult.getBytes(), 0, false);
    	Thread.sleep(TIMEOUT);
    	assertEquals(expectedResult, result);
    	assertTrue(client.getPendingDeliveryTokens().length == 0);
    	
    	client.unsubscribe("a/+/#");
    	client.unsubscribe("#");
    	
    	expectedResult = "should still get 1/2/3";
    	result = null;
    	client.publish("1/2/3", expectedResult.getBytes(), 0, false);
    	Thread.sleep(TIMEOUT);
    	assertEquals(expectedResult, result);
    	assertTrue(client.getPendingDeliveryTokens().length == 0);
    	
    	expectedResult = "should not get anything else";
    	result = null;
    	client.publish("a/2/3", expectedResult.getBytes(), 0, false);
    	Thread.sleep(TIMEOUT);
    	assertNull(result);
    	assertTrue(client.getPendingDeliveryTokens().length == 0);
    	
    	expectedResult = "should not get anything else";
    	result = null;
    	client.publish("a", expectedResult.getBytes(), 0, false);
    	Thread.sleep(TIMEOUT);
    	assertNull(result);
    	assertTrue(client.getPendingDeliveryTokens().length == 0);
    }

    public void testDupClientId() throws Exception {
    	MqttClient client2 = null;
    	try {
	    	MqttConnectOptions options = new MqttConnectOptions();
	    	options.setCleanSession(true);
	    	
	    	StringBuffer sb = new StringBuffer();
	    	sb.append(System.getProperty("java.io.tmpdir"));
	    	sb.append(System.getProperty("file.separator"));
	    	sb.append("client2");
	    	String tmpDir2 = sb.toString();
	    	MqttDefaultFilePersistence dataStore2 = new MqttDefaultFilePersistence(tmpDir2);
	    	
	    	client2 = new MqttClient(BROKER_URL, CLIENT_ID, dataStore2);
	    	client2.setCallback(this);
	    	client2.connect(options);
	    	
	    	Thread.sleep(TIMEOUT);
	    	
	    	//at this point, client2 should be connected and client should not
	    	assertTrue(client2.isConnected());
	    	assertFalse(client.isConnected());
    	}
    	catch (Exception e) {
    		// we expect an exception to be received
    		e.printStackTrace();
    		throw e;
    	}
    	finally {
    		if (client2 != null && client2.isConnected()) {
		    	client2.disconnect();
		    	while(client2.isConnected()) {
		    		Thread.sleep(TIMEOUT);
		    	}
    		}
    	}
    }
    
    
    

    public void testCleanSession() throws Exception {

    	MqttClient client2 = null;
    	try {
	    	MqttConnectOptions options = new MqttConnectOptions();
	    	options.setCleanSession(false);
	    	options.setConnectionTimeout(CONNECTION_TIMEOUT);
	    	options.setWill("l/w/t", new String("This is an LWT").getBytes(), 1, false);
	    	
	    	StringBuffer sb = new StringBuffer();
	    	sb.append(System.getProperty("java.io.tmpdir"));
	    	sb.append(System.getProperty("file.separator"));
	    	sb.append("client2");
	    	String tmpDir2 = sb.toString();
	    	MqttDefaultFilePersistence dataStore2 = new MqttDefaultFilePersistence(tmpDir2);
	    	
	    	client2 = new MqttClient(BROKER_URL, "client_2", dataStore2);
	    	client2.setCallback(this);
	    	client2.connect(options);
    	
	    	// subscribe and wait for the retain message to arrive
	    	client2.subscribe("a/b/c", 1);
	    	Thread.sleep(TIMEOUT);
	    	assertTrue(client2.getPendingDeliveryTokens().length == 0);

	    	// disconnect
	    	client2.disconnect();
	    	while(client2.isConnected()) {
	    		Thread.sleep(TIMEOUT);
	    	}

	    	// publish message with retain
	    	expectedResult = "should not get anything on publish only after the subscribe";
	    	result = null;
	    	client.publish("a/b/c", expectedResult.getBytes(), 1, false);
	    	
	    	// reconnect with client2 and wait for the pending messages to be sent
	    	client2.connect(options);
	    	Thread.sleep(TIMEOUT*2);
	    	assertEquals(expectedResult, result);
	    	assertTrue(client2.getPendingDeliveryTokens().length == 0);	    	
    	}
    	catch (Exception e) {
    		e.printStackTrace();
    		throw e;
    	}
    	finally {
    		if (client2 != null && client2.isConnected())
	    	client2.disconnect();
	    	while(client2.isConnected()) {
	    		Thread.sleep(TIMEOUT);
	    	}
    	}
    }

    
    public void testRetain() throws Exception {
    	
    	// publish with retain
    	// publish message with retain
    	expectedResult = "should not get anything on publish only after the subscribe";
    	result = null;
    	client.publish("a/b/c", expectedResult.getBytes(), 1, true);
    	Thread.sleep(TIMEOUT);
    	assertNull(result);
    	assertTrue(client.getPendingDeliveryTokens().length == 0);    	

    	MqttClient client2 = null;
    	try {
	    	MqttConnectOptions options = new MqttConnectOptions();
	    	options.setCleanSession(true);
	    	options.setConnectionTimeout(CONNECTION_TIMEOUT);
	    	
	    	StringBuffer sb = new StringBuffer();
	    	sb.append(System.getProperty("java.io.tmpdir"));
	    	sb.append(System.getProperty("file.separator"));
	    	sb.append("client_retain_2");
	    	String tmpDir2 = sb.toString();
	    	MqttDefaultFilePersistence dataStore2 = new MqttDefaultFilePersistence(tmpDir2);
	    	
	    	client2 = new MqttClient(BROKER_URL, "client_retain_2", dataStore2);
	    	client2.setCallback(this);
	    	client2.connect(options);
    	
	    	// subscribe and wait for the retain message to arrive
	    	client2.subscribe("a/b/c", 1);
	    	Thread.sleep(TIMEOUT*2);
	    	assertEquals(expectedResult, result);
	    	assertTrue(client2.getPendingDeliveryTokens().length == 0);
    	}
    	catch (Exception e) {
    		e.printStackTrace();
    		throw e;
    	}
    	finally {

    		if (client2 != null && client2.isConnected()) {
    			client2.disconnect();
		    	while(client2.isConnected()) {
		    		Thread.sleep(TIMEOUT);
		    	}
    		}
    		
    		// clear the retained message
    		result = null;
        	client.publish("a/b/c", "".getBytes(), 1, true);
        	Thread.sleep(TIMEOUT);
        	assertNull(result);
        	assertTrue(client.getPendingDeliveryTokens().length == 0);    		
    	}
    }

    
    public void testLWT() throws Exception {
    	
    	//sub on client 1
    	client.subscribe(WILL_TOPIC);
    	expectedResult = WILL_MESSAGE;
    	result = null;
    	Thread.sleep(TIMEOUT);

    	// start client 2
		Path currentRelativePath = Paths.get("");
		String s = currentRelativePath.toAbsolutePath().toString();
		System.out.println("Current relative path is: " + s);
    	Process p = Runtime.getRuntime().exec("java -jar " + s + "/target/mqtt-test-1.0.0-jar-with-dependencies.jar");
    	System.err.println("");
    	
    	// wait for the client to connect and then kill the client
    	Thread.sleep(CONNECTION_TIMEOUT * 2);
    	p.destroy();

    	// sleep a bit more and verify we got the LWT on client 1
    	Thread.sleep(CONNECTION_TIMEOUT * 15);
    	assertEquals(expectedResult, result);
    	assertTrue(client.getPendingDeliveryTokens().length == 0);
    }


    
    public void testKeepAlive() throws Exception {
//    	fail("Not implemented");
    }

    
	public void connectionLost(Throwable cause) {
		System.err.println("Connection Lost");
	}

	public void messageArrived(String topic, MqttMessage message) throws Exception {
		System.err.println("Message Arrived on " + topic + " with " + new String(message.getPayload()));
		result = new String(message.getPayload());
	}

	public void deliveryComplete(IMqttDeliveryToken token) {
		System.err.println("Delivery Complete: " + token.getMessageId());
	}
}
