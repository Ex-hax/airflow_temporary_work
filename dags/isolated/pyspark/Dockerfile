# Use an official Python runtime as a parent image with platform linux/amd64
# FROM --platform=linux/amd64 python:3.9-slim-buster
FROM python:3.9-slim-buster

# Set the working directory to /app
WORKDIR /app

# Install OpenJDK 11
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk-headless && \
    apt-get clean

# Set the JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Install findutils (required for PySpark)
RUN apt-get install -y findutils

# Install procps
RUN apt-get install -y procps

# Copy the current directory contents into the container at /app
COPY . /app

# Install necessary Python libraries
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

RUN pip install esdk-obs-python --trusted-host pypi.org

# Set environment variables for PySpark
ENV PYSPARK_PYTHON=python
ENV PYSPARK_DRIVER_PYTHON=python

# Set the default command to run your Python script
CMD ["python", "run.py"]
