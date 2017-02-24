#!/bin/bash
set -e

if [ "$1" = 'worker' ]; then
    echo "starting celery workers..."
    exec celery -A app.tasks worker --loglevel=INFO --concurrency=10
fi

echo "starting command..."
exec "$@"
