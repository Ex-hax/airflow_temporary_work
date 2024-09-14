#!/bin/bash

# Initialize the database
airflow db init &
airflow db migrate
