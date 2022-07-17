import sys
import yaml
from confluent_kafka import Producer
import requests
import json

class KafkaProducer:
    def __init__(self, cfg_path):
        self.cfg_path = cfg_path
        self.con_config = self.read_producer_config()

    
    def read_producer_config(self):
        return yaml.safe_load(open(self.cfg_path))

    def get_data_from_api(self):
        session = requests.Session()
        url = self.con_config['API']['BUS_API_URL'] 
        return session.get(url).text 

    def delivery_callback(self,err, msg):
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            sys.stderr.write('%% Message delivered to %s [%d]\n' %
                             (msg.topic(), msg.partition()))  
    def run(self):
        producer_config =  {
                                    'bootstrap.servers': self.con_config['CLOUDKARAFKA']['CLOUDKARAFKA_BROKERS'],
                                    'session.timeout.ms': 6000,
                                    'default.topic.config': {'auto.offset.reset': 'smallest'},
                                    'security.protocol': 'SASL_SSL',
	                                'sasl.mechanisms': 'SCRAM-SHA-256',
                                    'sasl.username': self.con_config['CLOUDKARAFKA']['CLOUDKARAFKA_USERNAME'],
                                    'sasl.password': self.con_config['CLOUDKARAFKA']['CLOUDKARAFKA_PASSWORD']
                                } 
        topic_name = self.con_config['CLOUDKARAFKA']['CLOUDKARAFKA_TOPIC']       
        producer = Producer(**producer_config)
        rows_from_api = json.loads(self.get_data_from_api())["vehicles"]

        for row in rows_from_api:
            message = json.dumps(row)
            producer.produce(topic_name, message, callback=self.delivery_callback)
        producer.flush()
