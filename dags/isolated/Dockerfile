# Use an official Python runtime as a parent image
FROM python:3.7-slim

# Set the working directory
WORKDIR /usr/src/app

# Copy the requirements file (if you have one) and install any needed packages specified there
COPY . ./

# Install pymssql and any other dependencies
RUN pip install pymssql==2.2.7

# Copy your application code (if necessary)
# COPY . .

# Command to run when starting the container
CMD ["python", "mssql_connector.py"]  # Replace with your script if needed
