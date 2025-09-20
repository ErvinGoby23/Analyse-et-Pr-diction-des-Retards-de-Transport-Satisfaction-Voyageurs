from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "retards",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="groupe-retards",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

print("👂 Consumer en écoute sur 'retards'…")

for msg in consumer:
    incident = msg.value
    print("---- Incident ----")
    print("Date :", incident.get("creation_time"))
    print("ID   :", incident.get("situation_id"))
    print("Résumé :", incident.get("summary"))
    print("Description :", incident.get("description"))
    print("Gravité :", incident.get("severity"))
    print()
