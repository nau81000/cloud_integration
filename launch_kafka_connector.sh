#!/bin/bash

/etc/confluent/docker/run &
echo "Attente de Kafka Connect..."
sleep 40
echo "Création du connecteur JDBC..."
curl -s -X POST -H "Content-Type: application/json" \
  --data @"/connector-config/mysql-sink-config.json" \
  http://localhost:8083/connectors
echo "Connecteur JDBC envoyé."
wait
