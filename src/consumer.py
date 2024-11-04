# Copyright (C), 2024-2025, bl33h, Mendezg1
# FileName: consumer.py
# Author: Sara Echeverria, Ricardo Méndez
# Version: I
# Creation: 03/11/2024
# Last modification: 03/11/2024

import numpy as np
from kafka import KafkaConsumer
import matplotlib.pyplot as plt

# enable interactive plotting
plt.ion()

# sensor ranges as defined in the producer
temperature_min, temperature_max = 0, 110
humidity_min, humidity_max = 0, 100
wind_directions = ['N', 'NO', 'O', 'SO', 'S', 'SE', 'E', 'NE']

# kafka consumer configuration
consumer = KafkaConsumer(
    '12345',
    group_id='foo2',
    bootstrap_servers='164.92.76.15:9092',
    auto_offset_reset='earliest',
    # Receive bytes directly
    value_deserializer=lambda x: x  
)

# initialize lists to store the decoded data
all_temperature = []
all_humidity = []
all_wind_direction = []

def decodeData(encoded_data):
    """Decode 3 bytes of data back into temperature, humidity, and wind direction."""
    
    packed_data = int.from_bytes(encoded_data, 'big')
    temp_encoded = (packed_data >> 16) & 0xFF
    humid_encoded = (packed_data >> 8) & 0xFF
    wind_encoded = packed_data & 0x07
    
    temperature = temp_encoded / 255 * (temperature_max - temperature_min) + temperature_min
    humidity = humid_encoded / 255 * (humidity_max - humidity_min) + humidity_min
    wind_direction = wind_directions[wind_encoded]
    
    return temperature, humidity, wind_direction

def plotAllData(temperature, humidity, wind_direction):
    plt.clf()
    plt.subplot(3, 1, 1)
    plt.plot(temperature, label='Temperature (°C)')
    plt.legend()
    plt.subplot(3, 1, 2)
    plt.plot(humidity, label='Humidity (%)')
    plt.legend()
    plt.subplot(3, 1, 3)
    plt.plot(wind_direction, label='Wind Direction')
    plt.legend()
    plt.pause(0.05)

for message in consumer:
    temperature, humidity, wind_direction = decodeData(message.value)
    print(f"Received message: Temp={temperature}, Humidity={humidity}, Wind={wind_direction}")
    all_temperature.append(temperature)
    all_humidity.append(humidity)
    all_wind_direction.append(wind_direction)
    
    plotAllData(all_temperature, all_humidity, all_wind_direction)

plt.show()