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
