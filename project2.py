# Import necessary libraries
import paho.mqtt.client as mqtt
import json
import pymongo
from pymongo import MongoClient
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# MongoDB connection settings
MONGO_HOST = 'localhost'
MONGO_PORT = 27017
MONGO_DB = 'iot_data'
MONGO_COLLECTION = 'mqtt_messages'

# RabbitMQ settings
RABBITMQ_HOST = 'localhost'
RABBITMQ_PORT = 5672
RABBITMQ_USER = 'guest'
RABBITMQ_PASS = 'guest'
RABBITMQ_EXCHANGE = 'mqtt_exchange'

# MQTT settings
MQTT_TOPIC = 'iot_data/#'

# Initialize MongoDB client
mongo_client = MongoClient(MONGO_HOST, MONGO_PORT)
db = mongo_client[MONGO_DB]
collection = db[MONGO_COLLECTION]

# Define MQTT callback functions
def on_connect(client, userdata, flags, rc):
    logger.info(f"Connected to MQTT broker with result code {rc}")
    client.subscribe(MQTT_TOPIC)

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode('utf-8'))
        # Process the incoming message, perform parsing, validation, and transformation
        # Example: Save the message to MongoDB
        save_to_mongodb(payload)
    except Exception as e:
        logger.error(f"Error processing MQTT message: {e}")

def save_to_mongodb(message):
    try:
        # Insert the message into MongoDB
        collection.insert_one(message)
        logger.info("Message saved to MongoDB successfully.")
    except Exception as e:
        logger.error(f"Error inserting message into MongoDB: {e}")

# Initialize MQTT client
mqtt_client = mqtt.Client()
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

# Connect to RabbitMQ
mqtt_client.connect(RABBITMQ_HOST, RABBITMQ_PORT, 60)
mqtt_client.username_pw_set(username=RABBITMQ_USER, password=RABBITMQ_PASS)

# Start the MQTT loop
mqtt_client.loop_forever()
