from kafka import KafkaProducer
import requests
import json
import time

# Kafka 配置
KAFKA_BROKER = 'localhost:9092'  # Kafka 服务器地址
TOPIC_NAME = 'restaurant-data'  # 你的 Kafka Topic

# 初始化 Kafka 生产者
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # 序列化 JSON 数据
)

# Google Maps API 配置
GOOGLE_MAPS_API_KEY = "AIzaSyCAb9Fa9YITF0uKHFS7C8F9VwAfeC3xr3Y"
API_URL = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"

params = {
    "location": "1.3521,103.8198",  # 新加坡中心点坐标 (纬度, 经度)
    "radius": "5000",  # 搜索半径（米）
    "type": "restaurant",  # 只查找餐厅
    "key": GOOGLE_MAPS_API_KEY
}

while True:
    response = requests.get(API_URL, params=params)
    data = response.json()

    if "results" in data:
        for restaurant in data["results"]:
            restaurant_info = {
                "name": restaurant.get("name"),
                "address": restaurant.get("vicinity"),
                "rating": restaurant.get("rating", 0),
                "user_ratings_total": restaurant.get("user_ratings_total", 0),
                "location": restaurant["geometry"]["location"]
            }
            print(f"Sending: {restaurant_info}")
            producer.send(TOPIC_NAME, restaurant_info)  # 发送数据到 Kafka

    time.sleep(10)  # 每 10 秒获取一次数据
