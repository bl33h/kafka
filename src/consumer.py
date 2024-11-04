import json
from kafka import KafkaConsumer
import matplotlib.pyplot as plt

# enable interactive plotting
plt.ion()  

# kafka consumer configuration
consumer = KafkaConsumer(
    '12345',
    group_id='foo2',
    bootstrap_servers='164.92.76.15:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

all_temperature = []
all_humidity = []
all_wind_direction = []

def plotAllData(temperature, humidity, wind_direction):
    """plot temperature, humidity, and wind direction data in real-time."""
    
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
    payload = message.value
    
    # debug: print each message received
    print("Received message:", payload)  

    temperature = payload.get('temperature', 0)
    humidity = payload.get('humidity', 0)
    wind_direction = payload.get('windDirection', 'N/A')

    all_temperature.append(temperature)
    all_humidity.append(humidity)
    all_wind_direction.append(wind_direction)

    plotAllData(all_temperature, all_humidity, all_wind_direction)

plt.show()