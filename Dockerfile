FROM python:3.9-slim

# Add a build argument for changed files
ARG CHANGED_FILES

# Set an environment variable with the changed files
ENV CHANGED_FILES=$CHANGED_FILES

# Set the working directory in the container
WORKDIR /app

# Install AWS CLI and jq
RUN apt-get update && apt-get install -y python3 jq && pip install awscli

# Install Python dependencies
COPY _app/requirements.txt .
RUN pip install -r requirements.txt 

# Copy necessary files
COPY ./src/ ./src/
COPY _app/ ./

RUN chmod +x src/entrypoint.sh

ENTRYPOINT [ "src/entrypoint.sh" ]

# Use the official Apache Spark base image
FROM apache/spark:3.4.1-python3

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

USER root

# Install any needed packages specified in requirements.txt
RUN pip3 install --no-cache-dir -r requirements.txt
USER ${SPARK_USER}

# Command to run tests when the container launches
CMD []