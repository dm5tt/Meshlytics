import json
import time
import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient
from datetime import datetime

class MeshlyticsConfig:
    def __init__(self, mqtt_broker, mqtt_port, mqtt_user, mqtt_password, mqtt_topic,
                 influxdb_host, influxdb_port, influxdb_user, influxdb_password, influxdb_database):
        self.mqtt_broker = mqtt_broker
        self.mqtt_port = mqtt_port
        self.mqtt_user = mqtt_user
        self.mqtt_password = mqtt_password
        self.mqtt_topic = mqtt_topic
        self.influxdb_host = influxdb_host
        self.influxdb_port = influxdb_port
        self.influxdb_user = influxdb_user
        self.influxdb_password = influxdb_password
        self.influxdb_database = influxdb_database


class MeshlyticsInfluxDB:
    def __init__(self, config: MeshlyticsConfig):
        self.client = InfluxDBClient(
            host=config.influxdb_host,
            port=config.influxdb_port,
            username=config.influxdb_user,
            password=config.influxdb_password,
            database=config.influxdb_database
        )

    def write_data(self, measurement, tags, fields):
        data = {
            "measurement": measurement,
            "tags": tags,
            "fields": fields,
            "time": datetime.utcnow().isoformat()
        }
        self.client.write_points([data])


class Meshlytics:
    def __init__(self, config: MeshlyticsConfig, influxdb_handler: MeshlyticsInfluxDB):
        self.config = config
        self.influxdb_handler = influxdb_handler
        self.packet_count = 0
        self.current_minute = datetime.now().minute
        self.unique_senders = set()  # Set to track unique senders per minute

        self.client = mqtt.Client()
        self.client.username_pw_set(config.mqtt_user, config.mqtt_password)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

    def start(self):
        self.client.connect(self.config.mqtt_broker, self.config.mqtt_port, 60)
        self.client.loop_forever()

    def on_connect(self, client, userdata, flags, rc):
        print(f"Connected with result code {rc}")
        client.subscribe(self.config.mqtt_topic)

    def on_message(self, client, userdata, msg):
        try:
            self.packet_count += 1
            self.check_minute_rollover()

            payload = json.loads(msg.payload)
            sender = self.to_hex_string(payload.get('from', 0))
            self.unique_senders.add(sender)  # Add sender to the set of unique senders

            common_fields, specific_fields, measurement = self.process_payload(payload)

            if specific_fields:
                self.influxdb_handler.write_data(measurement, {"sender": sender}, specific_fields)

            if any(value is not None for value in common_fields.values()):
                self.influxdb_handler.write_data("common_data", {"sender": sender}, common_fields)

        except Exception as e:
            print(f"Error processing message: {e}")

    def to_hex_string(self, value):
        return f"{value:08x}"

    def check_minute_rollover(self):
        current_minute = datetime.now().minute
        if current_minute != self.current_minute:
            self.log_packet_count()
            self.log_unique_senders()
            self.unique_senders.clear()  # Reset the set for the new minute
            self.current_minute = current_minute

    def log_packet_count(self):
        self.influxdb_handler.write_data(
            measurement="packet_stats",
            tags={"source": "mqtt_broker"},
            fields={"packet_count": self.packet_count}
        )
        self.packet_count = 0

    def log_unique_senders(self):
        # Log the number of unique senders in the past minute
        unique_sender_count = len(self.unique_senders)
        self.influxdb_handler.write_data(
            measurement="unique_sender_stats",
            tags={"source": "mqtt_broker"},
            fields={"unique_sender_count": unique_sender_count}
        )

    def process_payload(self, payload):
        common_fields = {
            "channel": payload.get('channel'),
            "from_field": payload.get('from'),
            "hop_start": payload.get('hop_start'),
            "hops_away": payload.get('hops_away'),
            "rssi": payload.get('rssi'),
            "snr": float(payload.get('snr')) if payload.get('snr') is not None else None,
            "timestamp": payload.get('timestamp'),
        }

        specific_fields = {}
        measurement = ""

        if 'payload' in payload:
            if payload['type'] == 'telemetry':
                measurement = "telemetry"
                specific_fields = {
                    "air_util_tx": float(payload['payload'].get('air_util_tx', 0)),
                    "battery_level": payload['payload'].get('battery_level'),
                    "channel_utilization": float(payload['payload'].get('channel_utilization', 0)),
                    "voltage": float(payload['payload'].get('voltage', 0)),
                    "uptime_seconds": payload['payload'].get('uptime_seconds'),
                    "barometric_pressure": float(payload['payload'].get('barometric_pressure', 0)),
                    "current": float(payload['payload'].get('current', 0)),
                    "gas_resistance": float(payload['payload'].get('gas_resistance', 0)),
                    "iaq": float(payload['payload'].get('iaq', 0)),
                    "lux": float(payload['payload'].get('lux', 0)),
                    "relative_humidity": float(payload['payload'].get('relative_humidity', 0)),
                    "temperature": float(payload['payload'].get('temperature', 0)),
                    "white_lux": float(payload['payload'].get('white_lux', 0)),
                    "wind_direction": float(payload['payload'].get('wind_direction', 0)),
                    "wind_gust": float(payload['payload'].get('wind_gust', 0)),
                    "wind_lull": float(payload['payload'].get('wind_lull', 0)),
                    "wind_speed": float(payload['payload'].get('wind_speed', 0))
                }
            elif payload['type'] == 'nodeinfo':
                measurement = "nodeinfo"
                specific_fields = {
                    "hardware": payload['payload'].get('hardware'),
                    "role": payload['payload'].get('role'),
                    "longname": payload['payload'].get('longname'),
                    "shortname": payload['payload'].get('shortname')
                }
            elif payload['type'] == 'position':
                measurement = "position"
                specific_fields = {
                    "altitude": payload['payload'].get('altitude'),
                    "latitude_i": payload['payload'].get('latitude_i'),
                    "longitude_i": payload['payload'].get('longitude_i'),
                    "precision_bits": payload['payload'].get('precision_bits')
                }

        return common_fields, specific_fields, measurement


if __name__ == "__main__":
    config = MeshlyticsConfig(
        mqtt_broker="dummy_broker.com",
        mqtt_port=1883,
        mqtt_user="dummy_pw",
        mqtt_password="dummy_pw",
        mqtt_topic="msh/EU_868/2/json/LongFast/#",
        influxdb_host="dummy_influx.com",
        influxdb_port=8086,
        influxdb_user="dummx_user",
        influxdb_password="dummy_pw",
        influxdb_database="meshtastic"
    )

    influxdb_handler = MeshlyticsInfluxDB(config)
    meshlytics = Meshlytics(config, influxdb_handler)
    meshlytics.start()
