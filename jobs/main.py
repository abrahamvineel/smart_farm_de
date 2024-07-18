import os
from confluent_kafka import SerializingProducer
import simplejson as json

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
SOIL_MOISTURE_SENSOR_TOPIC = os.getenv('SOIL_MOISTURE_SENSOR_TOPIC', 'soil_moisture_data')
TEMPERATURE_AND_LIGHT_SENSOR_TOPIC = os.getenv('TEMPERATURE_AND_LIGHT_SENSOR_TOPIC', 'temperature_light_data')
IRRIGATION_WATER_SENSOR_TOPIC = os.getenv('IRRIGATION_WATER_SENSOR_TOPIC', 'irrigation_water_data')
AV_GPS_TOPIC = os.getenv('AV_GPS_TOPIC', 'av_gpa_data')
LIVE_STOCK_SENSOR_TOPIC = os.getenv('LIVE_STOCK_SENSOR_TOPIC', 'live_stock_sensor_data')
DRONE_TOPIC = os.getenv('DRONE_TOPIC', 'drone_data')

if __name__ = "__main__":
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda err: print(f'Kafka error: {err}')
    }