from confluent_kafka import Producer
import json

class ScraperProducer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.producer = Producer({
            'bootstrap.servers': bootstrap_servers
        })

    def delivery_report(self, err, msg):
        if err is not None:
            print(f'Message delivery failed: {err}')
        else:
            print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

    def send_data(self, topic, data):
        self.producer.produce(
            topic,
            json.dumps(data).encode('utf-8'),
            callback=self.delivery_report
        )
        self.producer.flush()