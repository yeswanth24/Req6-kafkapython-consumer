from kafka import KafkaConsumer

brokers=["localhost:9092"]
topicName = "s3messages"

consumer = KafkaConsumer(topicName,group_id= "one_group",
bootstrap_servers=brokers,auto_offset_reset='earliest')

#for message in consumer:
    #print('Topic: %s:Partion: %d Message:%s' %(message.topic,message.partition,message.value))
from producer import messageSender 
filename='filedetails.txt'

f = open(filename,'a')
for message in consumer :
    f.write(str(message.value))
f.close()

while True:
    pass 