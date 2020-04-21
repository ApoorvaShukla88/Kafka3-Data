from time import sleep
from json import dumps
from kafka import KafkaProducer


producer = KafkaProducer(bootstraps_servers=['localhost:9092'],
                         value_serializer=lambda m: dumps(m).encode('ascii')
                         )


for i in range(1000):
    data = {'number' : i}
    print(data)
    producer.send('test', value=data)
    sleep(2)



