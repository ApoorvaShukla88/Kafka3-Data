from kafka import KafkaConsumer, TopicPartition
from kafka import KafkaConsumer
from json import loads


class Branch_consumer:
    branch_id = 0

    def __init__(self):
        consumer = KafkaConsumer(
            bootstrap_servers=['localhost:9092'],
            value_deserializer=lambda m: loads(m.decode('ascii'),
                                               group_id=branch_id + 1,
                                               ))

    def handleMessages(self):
        for message in self.consumer:
            message = message.value
            print('{} received'.format(message))


if __name__ == "__main__":
    consumer1 = Branch_consumer()
    consumer2 = Branch_consumer()

    consumer1.handleMessages()
    consumer2.handleMessages()
