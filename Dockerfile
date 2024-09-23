# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt /app/

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Install cron and wget
RUN apt-get update && apt-get -y install cron wget && apt-get clean && rm -rf /var/lib/apt/lists/*

# Copy the current directory contents into the container at /app
COPY . /app

# Create a directory for TLD files
RUN mkdir /app/tld_files

# Copy crontab file to the cron.d directory
COPY crontab /etc/cron.d/domain-checker-cron

# Ensure the crontab file has a newline at the end
RUN echo "" >> /etc/cron.d/domain-checker-cron

# Set permissions on the crontab file
RUN chmod 0644 /etc/cron.d/domain-checker-cron

# Apply cron job
RUN crontab /etc/cron.d/domain-checker-cron

# Create the log file to be able to run tail
RUN touch /var/log/cron.log

# Run the command on container startup
CMD ["cron", "-f"]
