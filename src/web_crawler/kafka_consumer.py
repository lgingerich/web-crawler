from confluent_kafka import Consumer, KafkaError
import json

class ScraperConsumer:
    def __init__(self, topics, bootstrap_servers='localhost:9092', group_id='scraper_group'):
        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest'
        })
        self.consumer.subscribe(topics)

    def consume(self):
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Consumer error: {msg.error()}")
                    break
            yield json.loads(msg.value().decode('utf-8'))

    def close(self):
        self.consumer.close()