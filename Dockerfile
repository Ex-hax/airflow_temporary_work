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

COPY ./init-scripts/airflow/python_install_lib.sh /opt/airflow/init-scripts/python_install_lib.sh
RUN /bin/bash -c "su - airflow -c '/bin/bash /opt/airflow/init-scripts/python_install_lib.sh'"