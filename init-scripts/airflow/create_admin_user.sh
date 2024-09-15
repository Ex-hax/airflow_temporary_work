#!/bin/bash

# Create the default admin user for Airflow
airflow users create --username admin --firstname Admin --lastname User --role Admin --password 1234 --email watchara.c.lee@gmail.com
