#!/bin/bash
set -e

if [ -z "$TARGET_API_KEY" ]; then
  echo "ERROR: TARGET_API_KEY env var is required"
  echo "Usage: TARGET_API_KEY=your_key sh run-ingestion.sh"
  exit 1
fi

echo "Building and starting ingestion..."
docker compose down --volumes --remove-orphans
docker compose build --no-cache
docker compose up --abort-on-container-exit --exit-code-from ingestion

echo "Done."
