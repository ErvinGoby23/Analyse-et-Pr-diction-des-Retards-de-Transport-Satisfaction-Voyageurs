from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

for i in range(5):
    message = {"id": i, "texte": f"Message test {i}"}
    producer.send("test_topic", message)
    print("✅ Envoyé :", message)
    time.sleep(1)

producer.flush()
producer.close()
