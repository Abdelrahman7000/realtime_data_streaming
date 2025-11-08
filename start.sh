#!/bin/bash
set -e
NETWORK_NAME="streaming-etl_a8a139_airflow"

# Create external Docker network if it doesn't exist
if ! docker network inspect "$NETWORK_NAME" >/dev/null 2>&1; then
  echo "Creating external Docker network: $NETWORK_NAME"
  docker network create "$NETWORK_NAME"
else
  echo "Docker network '$NETWORK_NAME' already exists."
fi

# Initialize Astro project if not already initialized
if [ ! -d ".astro" ]; then
  echo "Initializing Astro project..."
  astro dev init
else
  echo "Astro project already initialized."
fi

echo "Starting all containers..."
astro dev start
