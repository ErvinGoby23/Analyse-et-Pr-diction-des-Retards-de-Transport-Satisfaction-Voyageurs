import time
import json
import requests
import xml.etree.ElementTree as ET
from kafka import KafkaProducer

# URL du flux temps réel SNCF (SIRI Lite)
SIRI_URL = "https://proxy.transport.data.gouv.fr/resource/sncf-siri-lite-situation-exchange"

# Namespace XML utilisé par SIRI
NAMESPACE = {"siri": "http://www.siri.org.uk/siri"}

def siri_xml_to_dicts(xml_bytes):
    """Transforme le XML SIRI Lite en une liste de dictionnaires Python"""
    root = ET.fromstring(xml_bytes)
    incidents = []

    for sit in root.findall(".//siri:PtSituationElement", NAMESPACE):
        d = {}

        def get(tag):
            el = sit.find(f"siri:{tag}", NAMESPACE)
            return el.text if el is not None else None

        d["creation_time"] = get("CreationTime")
        d["situation_id"] = get("SituationNumber")
        d["participant"] = get("ParticipantRef")
        d["progress"] = get("Progress")

        # Résumé et description
        summary = sit.find(".//siri:Summary", NAMESPACE)
        description = sit.find(".//siri:Description", NAMESPACE)
        d["summary"] = summary.text if summary is not None else None
        d["description"] = description.text if description is not None else None

        # Gravité si disponible
        severity = sit.find(".//siri:Severity", NAMESPACE)
        d["severity"] = severity.text if severity is not None else None

        incidents.append(d)

    return incidents

def main():
    # Connexion Kafka
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        retries=5,
        linger_ms=50,
    )
    topic = "retards"

    print("⏳ Producer SNCF démarré. Lecture toutes les 2 minutes…")

    while True:
        try:
            resp = requests.get(SIRI_URL, timeout=20)
            if resp.status_code != 200:
                print(f"[WARN] Erreur HTTP {resp.status_code}")
            else:
                incidents = siri_xml_to_dicts(resp.content)
                if not incidents:
                    print("ℹ️ Aucun incident trouvé cette fois.")
                else:
                    for inc in incidents:
                        producer.send(topic, inc)
                    producer.flush()
                    print(f"✅ {len(incidents)} incidents envoyés sur '{topic}'")
        except Exception as e:
            print(f"[ERROR] {e}")

        time.sleep(120)  # Le flux est rafraîchi toutes les 2 minutes

if __name__ == "__main__":
    main()
