import psycopg2
import paho.mqtt.subscribe as subscribe
import json
import threading


def pgconnect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')

        # TODO: Fill in the database details
        conn = psycopg2.connect(host='',
                                database='',
                                user='',
                                password='')
        print("Connected")
    except Exception as e:
        print("Unable to connect to the database")
        print(e)
    return conn


class Database:
    def __init__(self):
        self.conn = pgconnect()
        self.cur = self.conn.cursor()
        self.initialize()
        threading.Thread(target=self.get_subscribe).start()

    def on_message(self, client, userdata, message):
        data = message.payload.decode("utf-8")
        data = json.loads(data)
        self.insert(data)

    def get_subscribe(self):
        subscribe.callback(self.on_message, "fuel-api/processed", hostname="127.0.0.1", port=1883)

    def initialize(self):
        create_table = """
        CREATE TABLE IF NOT EXISTS fuel (
            brand VARCHAR(20) NOT NULL,
            code INTEGER NOT NULL,
            "name" VARCHAR(100) NOT NULL,
            address VARCHAR(100) NOT NULL,
            fueltype VARCHAR(3) NOT NULL,
            price FLOAT NOT NULL,
            lastupdated TIMESTAMP NOT NULL,
            latitude FLOAT NOT NULL,
            longitude FLOAT NOT NULL,
            "state" VARCHAR(3) NOT NULL,
            PRIMARY KEY (code, fueltype, lastupdated)
        )
        """
        self.cur.execute(create_table)
        self.conn.commit()

    def insert(self, data):
        insert_data = """
        INSERT INTO fuel (brand, code, "name", address, fueltype, price, lastupdated, latitude, longitude, "state")
        VALUES (%(brand)s, %(code)s, %(name)s, %(address)s, %(fueltype)s, %(price)s, %(lastupdated)s, %(latitude)s, %(longitude)s, %(state)s)
        """
        # Check if the data is already in the database
        check_data = """
        SELECT * FROM fuel WHERE code = %(code)s AND fueltype = %(fueltype)s AND lastupdated = %(lastupdated)s
        """
        self.cur.execute(check_data, data)
        if self.cur.fetchone() is None:
            self.cur.execute(insert_data, data)
        self.conn.commit()
