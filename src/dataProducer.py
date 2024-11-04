# Copyright (C), 2024-2025, bl33h, Mendezg1
# FileName: dataProducer.py
# Author: Sara Echeverria, Ricardo Méndez
# Version: I
# Creation: 03/11/2024
# Last modification: 03/11/2024

import json
import random
import numpy as np
from time import sleep
from kafka import KafkaProducer

# kafka producer configuration
producer = KafkaProducer(
    bootstrap_servers=['164.92.76.15:9092'],
    # send bytes directly
    value_serializer=lambda x: x  
)

# sensor ranges and options
temperature_min, temperature_max = 0, 110
humidity_min, humidity_max = 0, 100
wind_directions = ['N', 'NO', 'O', 'SO', 'S', 'SE', 'E', 'NE']

def generateSensorData():
    """Generate temperature, humidity, and wind direction data with specified distributions."""
    
    temperature = np.random.normal(loc=55, scale=10)
    temperature = max(min(temperature, temperature_max), temperature_min)
    
    humidity = np.random.normal(loc=55, scale=10)
    humidity = int(max(min(humidity, humidity_max), humidity_min))
    
    wind_direction = random.choice(wind_directions)
    
    return temperature, humidity, wind_direction

def encodeData(temperature, humidity, wind_direction):
    """Encode temperature, humidity, and wind direction into 3 bytes."""
    
    temp_encoded = int((temperature - temperature_min) / (temperature_max - temperature_min) * 255)
    humid_encoded = int((humidity - humidity_min) / (humidity_max - humidity_min) * 255)
    wind_encoded = wind_directions.index(wind_direction)
    
    # combine into three bytes
    packed_data = (temp_encoded << 16) | (humid_encoded << 8) | wind_encoded
    return packed_data.to_bytes(3, 'big')

def sendData():
    while True:
        temperature, humidity, wind_direction = generateSensorData()
        encoded_data = encodeData(temperature, humidity, wind_direction)
        producer.send('12345', value=encoded_data)
        print(f"Data sent: {encoded_data}")
        
        # interval between sending data
        sleep(random.randint(1, 3))  

if __name__ == "__main__":
    sendData()