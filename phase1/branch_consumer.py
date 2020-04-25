from kafka import KafkaConsumer, TopicPartition
from kafka import KafkaConsumer
from json import loads


class Branch:

    # branch_id = 0

    def __init__(self, branch_id):
        consumer = KafkaConsumer(
            bootstrap_servers=['localhost:9092'],
            value_deserializer=lambda m: loads(m.decode('ascii'),

                                               ))
        topic_customer = TopicPartition('bank-customer-test', branch_id)
        #topic_transaction = TopicPartition('bank-customer-new', branch_id)
        partitions = list()
        partitions.append(topic_customer)
        consumer.assign(partitions)
        self.consumer = consumer
        self.branch_id = branch_id
        # branch_id = branch_id + 1

    def handleMessages(self):
        for message in self.consumer:
            message = message.value
            print('branch {} received {}'.format(self.branch_id, message))



