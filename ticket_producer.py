import os
import sys
import uuid
import json
import time
import random
from dotenv import load_dotenv
from datetime import datetime, UTC
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

def build_ticket():
    type_list = ['Problème récurrent', 'Demande de service', 'Maintenance préventive', 'Incident']
    priority_liste = ['Haute', 'Moyenne', 'Basse']
    random_type = random.randint(0,len(type_list)-1)
    random_priority = random.randint(0,len(priority_liste)-1)
    return {
        "schema": {
            "type": "struct",
            "fields": [
                {"field": "ticket_id", "type": "string"},
                {"field": "client_id", "type": "string"},
                {"field": "created_at", "type": "string"},
                {"field": "request", "type": "string"},
                {"field": "request_type", "type": "string"},
                {"field": "priority", "type": "string"}
            ],
            "optional": False,
            "name": "Ticket"
        },
        "payload": {
            "ticket_id": f"{datetime.now(UTC).isoformat()}_{str(uuid.uuid4())[:8]}",
            "client_id": f"CL{str(uuid.uuid4())[:8]}",
            "created_at": datetime.now(UTC).isoformat(),
            "request": "Problème",
            "request_type": type_list[random_type],
            "priority": priority_liste[random_priority]
        }
    }

def main():
    # Chargement de l'environnement
    load_dotenv()
    conf = { 'bootstrap.servers': os.getenv('KAFKA_SERVER')}
    topic = os.getenv('REDPANDA_TOPIC')
    # Production de tickets
    producer = Producer(conf)
    try:
        while True:
            ticket = build_ticket()
            producer.produce(
                topic=topic,
                key=ticket["payload"]["ticket_id"],
                value=json.dumps(ticket)
            )
            producer.flush()
            print(f"Ticket envoyé : {ticket["payload"]['ticket_id']}")
            time.sleep(10)
    except KeyboardInterrupt:
        print("Arrêt")

if __name__ == '__main__':
    sys.exit(main())