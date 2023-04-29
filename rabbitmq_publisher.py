from pika import BlockingConnection, BasicProperties, ConnectionParameters
import random


QUEUE_NAME = 'priority'

def priority_message(topic, message, priority):
    connection = BlockingConnection()
    try:
        channel = connection.channel()
        props = BasicProperties(
            priority=priority,           
        )
        channel.basic_publish(exchange='', routing_key=QUEUE_NAME, body=message, properties=props)
    except Exception as e:
        print(e)
    finally:
        connection.close()


for i in range(1,200):
    priority = random.randrange(1,200)
    priority_message(
        f'priority.{priority}',
        f'priority task{priority}',
        priority
    )