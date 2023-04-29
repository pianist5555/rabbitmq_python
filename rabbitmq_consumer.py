from pika import BlockingConnection, BasicProperties
 
 
def on_message(channel, method_frame, header_frame, body):
    label = method_frame.routing_key
    print('-- 새 메시지 --')
    print('label : ', label)
    print('body : ', body)
 
    channel.basic_ack(delivery_tag=method_frame.delivery_tag)
 
 
connection = BlockingConnection()
channel = connection.channel()
#channel.basic_consume(queue='hello', on_message_callback=on_message)
#channel.basic_consume(queue='world', on_message_callback=on_message)
channel.basic_consume(queue='priority', on_message_callback=on_message)
 
print("메시지 수신 대기 중")
try:
    channel.start_consuming()
except KeyboardInterrupt:
    channel.stop_consuming()
 
connection.close()