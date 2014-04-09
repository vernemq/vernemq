import mosquitto


def new(descr):
    return mosquitto.Mosquitto(descr)


def connect(c):
    c.connect("127.0.0.1")


def disconnect(c):
    c.disconnect()


def publish(c, topic, payload, qos=0, retain=False):
    c.publish(topic, payload, qos, retain)


def subscribe(c, topic, qos=0):
    c.subscribe(topic, qos)


def unsubscribe(c, topic):
    c.unsubscribe(topic)


def _on_connect(mosq, obj, rc):
    if rc == 0:
        print("Connected Successfully")
    else:
        print("Cant connect, error " + str(rc))


def _on_disconnect(mosq, obj, rc):
    print("Disconnected successfully")


def _on_publish(mosq, obj, mid):
    print("Message " + str(mid) + " published")


def _on_message(mosq, obj, msg):
    print("Message received on topic " + msg.topic
          + " with QoS " + str(msg.qos) + " and payload " + msg.payload)


def _on_subscribe(mosq, obj, mid, qos_list):
    print("Subscribed with mid " + str(mid))


def _on_unsubscribe(mosq, obj, mid):
    print("Unsubscribed with mid " + str(mid))


def main(client):
    connect(client)
    client.on_connect = _on_connect
    client.on_disconnect = _on_disconnect
    client.on_publish = _on_publish
    client.on_message = _on_message
    client.on_subscribe = _on_subscribe
    client.on_unsubscribe = _on_unsubscribe

    subscribe(client, "finance/ibm/+")

    while client.loop() == 0:
        pass
