#!/bin/bash
echo "Creando topicos Kafka..."

kafka-topics --create \
  --bootstrap-server kafka:29092 \
  --topic iot.air_quality.streaming \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists

kafka-topics --create \
  --bootstrap-server kafka:29092 \
  --topic iot.air_quality.streaming.dlq \
  --partitions 1 \
  --replication-factor 1 \
  --if-not-exists

echo "Topicos creados exitosamente."
kafka-topics --list --bootstrap-server kafka:29092
