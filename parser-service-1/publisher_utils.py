import os
import json
import requests
import base64
from google.cloud import pubsub_v1

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "operaciones-peru")
IS_LOCAL = os.getenv("IS_LOCAL", "true").lower() == "true"  # por defecto True

# Instancia global del publisher solo si es producción
publisher = pubsub_v1.PublisherClient() if not IS_LOCAL else None

def publish_message(topic: str, payload: dict):
    if IS_LOCAL:
        # Simulación HTTP para test local
        url = f"http://localhost:8001/{topic}"
        res = requests.post(url, json=payload)
        print(f"[LOCAL] Enviado a {url} | status={res.status_code}")
    else:
        topic_path = publisher.topic_path(GCP_PROJECT_ID, topic)
        message_bytes = json.dumps(payload).encode("utf-8")
        publisher.publish(topic_path, message_bytes)
        print(f"[PROD] Publicado en Pub/Sub: {topic_path}")
