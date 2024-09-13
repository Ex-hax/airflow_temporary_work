#!/bin/bash

# Wait for webserver to start (optional, in case you want to ensure it starts first)
sleep 10

# Create the default admin user for Airflow
airflow users create \
    --username admin \
    --password 1234 \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email watchara.c.lee@gmail.com
