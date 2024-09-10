#!/bin/bash
set -e

# Initialize the database
airflow db init
airflow db migrate
# Execute the command passed to the container
exec "$@"

