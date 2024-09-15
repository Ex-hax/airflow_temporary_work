# use airflow image
FROM apache/airflow:latest

# as root user
USER root

# Install vim and other necessary tools
RUN apt-get update && apt-get install -y \
    vim \
    curl \
    wget \
    git \
    && rm -rf /var/lib/apt/lists/*

COPY ./.airflowignore /opt/airflow/dags/.airflowignore