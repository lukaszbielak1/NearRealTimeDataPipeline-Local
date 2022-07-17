from src.KafkaProducer import KafkaProducer


def main():
    config_path = "./config/config.yaml"
    producer = KafkaProducer(config_path)
    producer.run()

if __name__ == "__main__":
    main()
