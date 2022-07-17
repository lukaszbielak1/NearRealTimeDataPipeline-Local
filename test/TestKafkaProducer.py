import unittest
import context
from src.KafkaProducer import KafkaProducer

import json

class TestKafkaProducer(unittest.TestCase):
    def setUp(self):
        self.config_path = "./test/config.yaml"
        self.producer = KafkaProducer(self.config_path)
        
    def test_read_producer_config(self):
        self.assertEqual(self.producer.read_producer_config()['TEST_CASE']['TEST_CASE'] , "PASS")
    
    def test_int_get_data_from_api(self):
        self.assertIsNotNone(json.loads(self.producer.get_data_from_api())["lastUpdate"] )
    
if __name__ == '__main__':
    unittest.main()