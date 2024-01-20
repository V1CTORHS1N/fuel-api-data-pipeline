import MqttObj as mqtt
import requests
import datetime as dt
import uuid
import pandas as pd
import json


class Updater:
    def __init__(self, url: dict):
        self.auth_token = None
        self.url = url
        self.mqtt = mqtt.MqttObj()
        self.api_key = ""                       # TODO: Add API Key
        self.auth_header = ""                   # TODO: Add Auth Header
        self.get_auth()
        self.header = {
            'authorization': self.auth_token,
            'content-type': "application/json; charset=utf-8",
            'apikey': self.api_key,
            'transactionid': None,
            'requesttimestamp': None,
            'states': 'NSW'
        }

    def get_auth(self):
        querystring = {"grant_type": "client_credentials"}
        headers = {
            'content-type': "application/json",
            'authorization': self.auth_header
        }
        auth_response = requests.get(self.url["auth"], headers=headers, params=querystring).json()
        self.auth_token = f"Bearer {auth_response['access_token']}"

    def update(self, key: str):
        _header = self.header.copy()
        _header['transactionid'] = str(uuid.uuid4())
        _header['requesttimestamp'] = dt.datetime.utcnow().strftime("%d/%m/%Y %H:%M:%S %p")
        data = requests.get(self.url[key], headers=_header).json()
        if len(data['prices']) != 0:
            stations = pd.DataFrame(data['stations'], dtype=str)
            stations['code'] = stations['code'].astype(int)
            prices = pd.DataFrame(data['prices'], dtype=str)
            prices['stationcode'] = prices['stationcode'].astype(int)
            merged = pd.merge(stations, prices, left_on='code', right_on='stationcode')
            for i in json.loads(merged.to_json(orient='records')):
                if i['fueltype'] == 'EV':
                    continue
                self.mqtt.publish(topic="fuel-api/raw", message=str(json.dumps(i)))
