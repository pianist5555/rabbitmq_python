from pika import BlockingConnection, BasicProperties, ConnectionParameters
 
 
def message(topic, message):
    connection = BlockingConnection()
    try:
        channel = connection.channel()
        props = BasicProperties(content_type='text/plain', delivery_mode=1)
        channel.basic_publish('incoming', topic, message, props) # incoming exchange로 publish
    finally:
        connection.close()


def priority_message(topic, message):
    connection = BlockingConnection()
    try:
        channel = connection.channel()
        props = BasicProperties(
            priority=99,  # Set the message priority
            content_type='text/plain', 
            delivery_mode=1
        )
        channel.basic_publish('incoming', topic, message, props)
    except Exception as e:
        print(e)
    finally:
        connection.close()
 
 
message('hello.1', 'hello task1')
message('world.2', 'world task2')
priority_message('priority.3', 'priority task3')