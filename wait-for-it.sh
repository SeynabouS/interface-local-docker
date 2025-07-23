#!/usr/bin/env bash

hostport="$1"
shift

host="${hostport%%:*}"
port="${hostport##*:}"

echo "⏳ Waiting for $host:$port to be available..."

# Wait for port to be open
while ! nc -z "$host" "$port"; do
  sleep 1
done

# Additional check if pg_isready is available
if command -v pg_isready &> /dev/null; then
  echo "Port open, verifying PostgreSQL is ready..."
  while ! pg_isready -h "$host" -p "$port" -q; do
    sleep 2
  done
else
  echo "pg_isready not available, waiting 5 seconds to ensure PostgreSQL is ready..."
  sleep 5
fi

echo "✅ $host:$port is now available. Starting application..."
exec "$@"