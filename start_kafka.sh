#!/bin/bash

# Start Kafka using Docker Compose
docker-compose up -d

echo "Kafka is starting up. Please wait..."
sleep 10
echo "Kafka should now be ready."