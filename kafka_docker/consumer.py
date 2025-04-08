from kafka import KafkaConsumer
import json

# Kafka 配置
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'restaurant-data'

# 初始化 Kafka 消费者
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# 监听数据
for message in consumer:
    restaurant_info = message.value
    print(f"Received: {restaurant_info}")
