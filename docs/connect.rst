.. _connect:

Connecting to VerneMQ
=====================

As MQTT is the main application protocol spoken by VerneMQ, you could use any protocol compliant MQTT client library. This page gives an overview of the different options you have.

C & C++
-------

We recommend the official `Paho MQTT client library <http://eclipse.org/paho/clients/c/embedded/>`_. A simple example looks like the following:

.. code-block:: C

    MQTTPacket_connectData data = MQTTPacket_connectData_initializer;
    int rc = 0;
    char buf[200];
    MQTTString topicString = MQTTString_initializer;
    char* payload = "mypayload";
    int payloadlen = strlen(payload);int buflen = sizeof(buf);
    
    data.clientID.cstring = "me";
    data.keepAliveInterval = 20;
    data.cleansession = 1;
    len = MQTTSerialize_connect(buf, buflen, &data); /* 1 */
    
    topicString.cstring = "mytopic";
    len += MQTTSerialize_publish(buf + len, buflen - len, 0, 0, 0, 0, topicString, payload, payloadlen); /* 2 */
    
    len += MQTTSerialize_disconnect(buf + len, buflen - len); /* 3 */
    
    rc = Socket_new("127.0.0.1", 1883, &mysock);
    rc = write(mysock, buf, len);
    rc = close(mysock);

Ruby
----

We recommend the `Ruby-MQTT client library <https://github.com/njh/ruby-mqtt>`_. A simple example looks like the following:

.. code-block:: Ruby

    require 'rubygems'
    require 'mqtt'
    
    # Publish example
    MQTT::Client.connect('myserver.example.com') do |c|
      c.publish('topic', 'message')
    end
    
    # Subscribe example
    MQTT::Client.connect('myserver.example.com') do |c|
      # If you pass a block to the get method, then it will loop
      c.get('test') do |topic,message|
        puts "#{topic}: #{message}"
      end
    end

Python
------

We recommend the official `Paho MQTT client library <http://eclipse.org/paho/clients/python/>`_. A simple example looks like the following:

.. code-block:: Python

    import paho.mqtt.client as mqtt
    
    # The callback for when the client receives a CONNACK response from the server.
    def on_connect(client, userdata, rc):
        print("Connected with result code "+str(rc))
    	# Subscribing in on_connect() means that if we lose the connection and
    	# reconnect then subscriptions will be renewed.
    	client.subscribe("$SYS/#")
    
    # The callback for when a PUBLISH message is received from the server.
    def on_message(client, userdata, msg):
    	print(msg.topic+" "+str(msg.payload))
    
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    
    client.connect("iot.eclipse.org", 1883, 60)
    
    # Blocking call that processes network traffic, dispatches callbacks and
    # handles reconnecting.
    # Other loop*() functions are available that give a threaded interface and a
    # manual interface.
    client.loop_forever()

Java
----

We recommend the official `Paho MQTT client library <http://eclipse.org/paho/clients/java/>`_. A simple example looks like the following:

.. code-block:: Java

    import org.eclipse.paho.client.mqttv3.MqttClient;
    import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
    import org.eclipse.paho.client.mqttv3.MqttException;
    import org.eclipse.paho.client.mqttv3.MqttMessage;
    import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
    
    public class MqttPublishSample {
    
        public static void main(String[] args) {
    
            String topic        = "MQTT Examples";
            String content      = "Message from MqttPublishSample";
            int qos             = 2;
            String broker       = "tcp://iot.eclipse.org:1883";
            String clientId     = "JavaSample";
            MemoryPersistence persistence = new MemoryPersistence();
    
            try {
                MqttClient sampleClient = new MqttClient(broker, clientId, persistence);
                MqttConnectOptions connOpts = new MqttConnectOptions();
                connOpts.setCleanSession(true);
                System.out.println("Connecting to broker: "+broker);
                sampleClient.connect(connOpts);
                System.out.println("Connected");
                System.out.println("Publishing message: "+content);
                MqttMessage message = new MqttMessage(content.getBytes());
                message.setQos(qos);
                sampleClient.publish(topic, message);
                System.out.println("Message published");
                sampleClient.disconnect();
                System.out.println("Disconnected");
                System.exit(0);
            } catch(MqttException me) {
                System.out.println("reason "+me.getReasonCode());
                System.out.println("msg "+me.getMessage());
                System.out.println("loc "+me.getLocalizedMessage());
                System.out.println("cause "+me.getCause());
                System.out.println("excep "+me);
                me.printStackTrace();
            }
        }
    }

Go
--

We recommend the official `Paho MQTT client library <http://eclipse.org/paho/clients/golang/>`_. A simple example looks like the following:

.. code-block:: Go

    package main

    import (
      "fmt"
      //import the Paho Go MQTT library
      MQTT "git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git"
      "os"
      "time"
    )
    
    //define a function for the default message handler
    var f MQTT.MessageHandler = func(msg MQTT.Message) {
      fmt.Printf("TOPIC: %s\n", msg.Topic())
      fmt.Printf("MSG: %s\n", msg.Payload())
    }
    
    func main() {
      //create a ClientOptions struct setting the broker address, clientid, turn
      //off trace output and set the default message handler
      opts := MQTT.NewClientOptions().SetBroker("tcp://iot.eclipse.org:1883")
      opts.SetClientId("go-simple")
      opts.SetTraceLevel(MQTT.Off)
      opts.SetDefaultPublishHandler(f)
    
      //create and start a client using the above ClientOptions
      c := MQTT.NewClient(opts)
      _, err := c.Start()
      if err != nil {
        panic(err)
      }
    
      //subscribe to the topic /go-mqtt/sample and request messages to be delivered
      //at a maximum qos of zero, wait for the receipt to confirm the subscription
      if receipt, err := c.StartSubscription(nil, "/go-mqtt/sample", MQTT.QOS_ZERO); err != nil {
        fmt.Println(err)
        os.Exit(1)
      } else {
        <-receipt
      }
    
      //Publish 5 messages to /go-mqtt/sample at qos 1 and wait for the receipt
      //from the server after sending each message
      for i := 0; i < 5; i++ {
        text := fmt.Sprintf("this is msg #%d!", i)
        receipt := c.Publish(MQTT.QOS_ONE, "/go-mqtt/sample", text)
        <-receipt
      }
    
      time.Sleep(3 * time.Second)
    
      //unsubscribe from /go-mqtt/sample
      if receipt, err := c.EndSubscription("/go-mqtt/sample"); err != nil {
        fmt.Println(err)
        os.Exit(1)
      } else {
        <-receipt
      }
    
      c.Disconnect(250)
    }

PHP
---

We recommend the `phpMQTT library <https://github.com/bluerhinos/phpMQTT>`_. A simple example looks like the following:

.. code-block:: PHP

    <?php
    require("../phpMQTT.php");

    $mqtt = new phpMQTT("example.com", 1883, "phpMQTTClient");
    if(!$mqtt->connect()){
	    exit(1);
    }

    // Simple Publish Example
    $mqtt->publish("test/topic/example/","Hello World!", 0);


    // Simple Subscribe Example
    $topics['test/topic/example'] = array("qos" => 0, "function" => "procmsg");
    $mqtt->subscribe($topics,0);
    while($mqtt->proc()){
        // receive loop
    }

    $mqtt->close();

    function procmsg($topic, $msg){
        echo "Msg Recieved: ".date("r")."\nTopic:{$topic}\n$msg\n";
    }
    ?>

Javascript
----------

We recommend the official `Paho MQTT client library <http://eclipse.org/paho/clients/js/>`_. This library is meant to be used in the web browser. It requires that VerneMQ has a websocket listener configured. A simple example for using the client on a webpage could look like the following:

.. code-block:: Javascript

    // Create a client instance
    client = new Paho.MQTT.Client(location.hostname, Number(location.port), "clientId");
    
    // set callback handlers
    client.onConnectionLost = onConnectionLost;
    client.onMessageArrived = onMessageArrived;
    
    // connect the client
    client.connect({onSuccess:onConnect});
    
    
    // called when the client connects
    function onConnect() {
      // Once a connection has been made, make a subscription and send a message.
      console.log("onConnect");
      client.subscribe("/World");
      message = new Paho.MQTT.Message("Hello");
      message.destinationName = "/World";
      client.send(message); 
    }
    
    // called when the client loses its connection
    function onConnectionLost(responseObject) {
      if (responseObject.errorCode !== 0) {
        console.log("onConnectionLost:"+responseObject.errorMessage);
      }
    }
    
    // called when a message arrives
    function onMessageArrived(message) {
      console.log("onMessageArrived:"+message.payloadString);
    }

Lua
---

We recommend the `mqtt_lua client library <https://github.com/geekscape/mqtt_lua>`_. The library requires A simple example looks like the following:

.. code-block:: Lua

    -- Define a function which is called by mqtt_client:handler(),
    -- whenever messages are received on the subscribed topics
    
      function callback(topic, message)
        print("Received: " .. topic .. ": " .. message)
        if (message == "quit") then running = false end
      end
    
    -- Create an MQTT client instance, connect to the MQTT server and
    -- subscribe to the topic called "test/2"
    
      MQTT = require("mqtt_library")
      MQTT.Utility.set_debug(true)
      mqtt_client = MQTT.client.create("localhost", nil, callback)
      mqtt_client:connect("lua mqtt client"))
      mqtt_client:subscribe({"test/2"})
    
    -- Continously invoke mqtt_client:handler() to process the MQTT protocol and
    -- handle any received messages.  Also, publish a message on topic "test/1"
    
      running = true
    
      while (running) do
        mqtt_client:handler()
        mqtt_client:publish("test/1", "test message")
        socket.sleep(1.0)  -- seconds
      end


Arduino
-------

We recommend the `knolleary MQTT client library <https://github.com/knolleary/pubsubclient>`_. The library requires the Arduino Ethernet Shield. A simple example looks like the following:

.. code-block:: C

    #include <SPI.h>
    #include <Ethernet.h>
    #include <PubSubClient.h>
    
    // Update these with values suitable for your network.
    byte mac[]    = {  0xDE, 0xED, 0xBA, 0xFE, 0xFE, 0xED };
    byte server[] = { 172, 16, 0, 2 };
    byte ip[]     = { 172, 16, 0, 100 };
    
    void callback(char* topic, byte* payload, unsigned int length) {
      // handle message arrived
    }
    
    EthernetClient ethClient;
    PubSubClient client(server, 1883, callback, ethClient);
    
    void setup()
    {
      Ethernet.begin(mac, ip);
      if (client.connect("arduinoClient")) {
        client.publish("outTopic","hello world");
        client.subscribe("inTopic");
      }
    }
    
    void loop()
    {
      client.loop();
    }
    

