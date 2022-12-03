from kafka import KafkaProducer
from const import *
import sys


print("Starting producer")
print("To exit press CTRL+C or type exit as topic name")
    
producer = KafkaProducer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT])

while True:
    input_topic = input('Enter topic name: ')
    if input_topic == 'exit':
        break
    input_msg = input('Enter message: ')
    producer.send(input_topic, value=input_msg.encode())
    print ('Message sent')

producer.flush()
