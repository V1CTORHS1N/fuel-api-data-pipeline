import paho.mqtt.client as mqtt

class MqttObj:

    def __init__(self, broker="127.0.0.1", port=1883, callback=None):
        self.broker = broker
        self.port = port
        self.client = mqtt.Client()
        self.client.on_message = callback
        self.client.connect(self.broker, self.port)
        self.client.loop_start()

    def subscribe(self, topic: str):
        self.client.subscribe(topic)

    def publish(self, topic: str, message: str):
        self.client.publish(topic, message).wait_for_publish()

    def disconnect(self):
        self.client.disconnect()
        self.client.loop_stop()
