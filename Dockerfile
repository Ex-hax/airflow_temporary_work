# use airflow image
FROM apache/airflow

# as root user
USER root

# Install vim and other necessary tools
RUN apt-get update && apt-get install -y \
    vim \
    curl \
    wget \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy custom_operator folder
COPY plugins /plugins

USER root
# Copy the entrypoint script
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]