import os
from confluent_kafka import SerializingProducer
import simplejson as json
from datetime import datetime, timedelta
import random 
import uuid
import json 
import time

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
SOIL_MOISTURE_SENSOR_TOPIC = os.getenv('SOIL_MOISTURE_SENSOR_TOPIC', 'soil_moisture_data')
TEMPERATURE_AND_LIGHT_SENSOR_TOPIC = os.getenv('TEMPERATURE_AND_LIGHT_SENSOR_TOPIC', 'temperature_light_data')
IRRIGATION_WATER_SENSOR_TOPIC = os.getenv('IRRIGATION_WATER_SENSOR_TOPIC', 'irrigation_water_data')
AV_GPS_TOPIC = os.getenv('AV_GPS_TOPIC', 'av_gps_data')
LIVE_STOCK_SENSOR_TOPIC = os.getenv('LIVE_STOCK_SENSOR_TOPIC', 'live_stock_sensor_data')
DRONE_TOPIC = os.getenv('DRONE_TOPIC', 'drone_data')

def get_next_time():
    start_time = datetime.now()
    start_time += timedelta(seconds=random.randint(30, 60))
    return start_time

def simulate_sensor_location():
    sensor_location = {}
    sensor_location['latitude'] = random.uniform(0.005, 0.005)
    sensor_location['longitude'] = random.uniform(0.005, 0.005)
    return sensor_location


def generate_soil_moisture_data():
    sensor_id = random.randrange(1, 9999, 1)
    location = simulate_sensor_location()
    return {
        'id': uuid.uuid4(),
        'sensorId': sensor_id,
        'timestamp': get_next_time().isoformat(),
        'location': (location['latitude'], location['longitude']),
        'moisturePercentage': random.randrange(20, 60, 1),
        'company': 'smart_sensor',
        'model': 'T230',
        'year': random.randrange(2010, 2024, 1)
    }

def generate_light_temp_data():
    sensor_id = random.randrange(1, 9999, 1)
    location = simulate_sensor_location()
    return {
        'id': uuid.uuid4(),
        'sensorId': sensor_id,
        'timestamp': get_next_time().isoformat(),
        'location': (location['latitude'], location['longitude']),
        'airTemperature': random.randrange(0, 90, 1),
        'soilTemperature': random.randrange(0, 60, 1),
        'leafTemperature': random.randrange(0, 60, 1),
        'lightIntensity': random.randrange(20, 60, 1),
        'PAR': random.randrange(20, 60, 1),
        'DLI': random.randrange(20, 60, 1),
        'company': 'smart_sensor',
        'model': 'T230',
        'year': random.randrange(2010, 2024, 1)
    }

def generate_irrigation_water_data():
    sensor_id = random.randrange(1, 9999, 1)
    location = simulate_sensor_location()
    return {
        'id': uuid.uuid4(),
        'sensorId': sensor_id,
        'timestamp': get_next_time().isoformat(),
        'location': (location['latitude'], location['longitude']),
        'soilWaterPotential': random.randrange(0, 90, 1),
        'rainfallAmount': random.randrange(0, 60, 1),
        'rainfallDetection': random.randrange(0, 1, 1),
        'waterFlowRate': random.randrange(0, 10, 1),
        'company': 'smart_sensor',
        'model': 'T230',
        'year': random.randrange(2010, 2024, 1)
    }

def generate_av_gps_data():
    sensor_id = random.randrange(1, 9999, 1)
    location = simulate_sensor_location()
    return {
        'id': uuid.uuid4(),
        'sensorId': sensor_id,
        'timestamp': get_next_time().isoformat(),
        'location': (location['latitude'], location['longitude']),
        'company': 'smart_sensor',
        'model': 'T230',
        'year': random.randrange(2010, 2024, 1)
    }

def generate_livestock_sensor_data():
    sensor_id = random.randrange(1, 9999, 1)
    location = simulate_sensor_location()
    return {
        'id': uuid.uuid4(),
        'sensorId': sensor_id,
        'timestamp': get_next_time().isoformat(),
        'location': (location['latitude'], location['longitude']),
        'bodyTemperature': random.randrange(0, 90, 1),
        'heartRate': random.randrange(0, 60, 1),
        'respiratoryRate': random.randrange(0, 10, 1),
        'activityLevel': random.randrange(0, 10, 1),
        'company': 'smart_sensor',
        'model': 'T230',
        'year': random.randrange(2010, 2024, 1)
    }

def generate_drone_data():
    sensor_id = random.randrange(1, 9999, 1)
    location = simulate_sensor_location()
    return {
        'id': uuid.uuid4(),
        'sensorId': sensor_id,
        'timestamp': get_next_time().isoformat(),
        'location': (location['latitude'], location['longitude']),
        'aerialImagesLocation': 'cloudStorage/location',
        'thermalImaging': 'cloudStorage/location',
        'sprayCoverage': random.randrange(0, 10, 1),
        'applicationRate': random.randrange(0, 10, 1),
        'company': 'smart_sensor',
        'model': 'T230',
        'year': random.randrange(2010, 2024, 1)
    }

def json_serializer(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivey failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def produce_data_to_kafka(producer, topic, data):
    producer.produce(
        topic, 
        key=str(data['id']),
        value=json.dumps(data, default=json_serializer).encode('utf-8'),
        on_delivery=delivery_report
    )

    producer.flush()

def simulate_smart_farm(producer):
    while True:
        soil_moisture_data = generate_soil_moisture_data()
        temp_light_data = generate_light_temp_data()
        irrigation_water_data = generate_irrigation_water_data()
        av_gps_data = generate_av_gps_data()
        live_stock_sensor_data = generate_livestock_sensor_data()
        drone_data = generate_drone_data()
        
        produce_data_to_kafka(producer, SOIL_MOISTURE_SENSOR_TOPIC, soil_moisture_data)
        produce_data_to_kafka(producer, TEMPERATURE_AND_LIGHT_SENSOR_TOPIC, temp_light_data)
        produce_data_to_kafka(producer, IRRIGATION_WATER_SENSOR_TOPIC, irrigation_water_data)
        produce_data_to_kafka(producer, AV_GPS_TOPIC, av_gps_data)
        produce_data_to_kafka(producer, LIVE_STOCK_SENSOR_TOPIC, live_stock_sensor_data)
        produce_data_to_kafka(producer, DRONE_TOPIC, drone_data)
        
        time.sleep(5)

if __name__ == "__main__":
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda err: print(f'Kafka error: {err}')
    }

    producer = SerializingProducer(producer_config)

    try:
        simulate_smart_farm(producer)
    except KeyboardInterrupt:
        print('simulation ended by the user')
    except Exception as e:
        print(f'Unexpected error occured: {e}')