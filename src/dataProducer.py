import json
import random
import numpy as np
from time import sleep
from kafka import KafkaProducer

# kafka producer configuration
producer = KafkaProducer(
    bootstrap_servers=['164.92.76.15:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# sensor ranges and options
temperatureMin, temperatureMax = 0, 110
humidityMin, humidityMax = 0, 100
windDirections = ['N', 'NO', 'O', 'SO', 'S', 'SE', 'E', 'NE']

def generateSensorData():
    """Generate temperature, humidity, and wind direction data with specified distributions."""
    
    temperature = np.random.normal(loc=55, scale=10)
    temperature = max(min(temperature, temperatureMax), temperatureMin)
    
    humidity = np.random.normal(loc=55, scale=10)
    humidity = int(max(min(humidity, humidityMax), humidityMin))
    
    windDirection = random.choice(windDirections)
    
    return {
        'temperature': round(temperature, 2),
        'humidity': humidity,
        'windDirection': windDirection
    }

def sendData():
    """Send sensor data to the Kafka topic in an infinite loop."""
    
    while True:
        data = generateSensorData()
        producer.send('12345', value=data)
        print(f"Data sent: {data}")
        
        # interval between sending data
        sleep(random.randint(15, 30))  

if __name__ == "__main__":
    sendData()