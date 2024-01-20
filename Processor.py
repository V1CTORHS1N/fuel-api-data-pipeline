import paho.mqtt.subscribe as subscribe
from MqttObj import MqttObj
import pandas as pd
import json
import threading


def preprocessing(data: dict):
    data['latitude'] = float(json.loads(data['location'].replace("'", '"'))['latitude'])
    data['longitude'] = float(json.loads(data['location'].replace("'", '"'))['longitude'])
    data['state'] = data['state_x']
    for i in ['brandid', 'stationcode', 'stationid', 'state_x', 'state_y', 'location']:
        data.pop(i)
    data['price'] = float(data['price'])
    data['address'] = data['address'].strip().upper()
    data['lastupdated'] = str(pd.to_datetime(data['lastupdated'], format='%d/%m/%Y %H:%M:%S'))
    return data


class Processor:
    def __init__(self):
        self.client = MqttObj()
        threading.Thread(target=self.get_subscribe).start()

    def publish(self, data):
        self.client.publish(topic="fuel-api/processed", message=str(json.dumps(data)))

    def on_message(self, client, userdata, message):
        data = message.payload.decode("utf-8")
        data = json.loads(data)
        data = preprocessing(data)
        self.publish(data)

    def get_subscribe(self):
        subscribe.callback(self.on_message, "fuel-api/raw", hostname="127.0.0.1", port=1883)
