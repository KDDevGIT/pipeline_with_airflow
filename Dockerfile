FROM apache/airflow:2.7.1

# Install Python MySQL connector
RUN pip install mysql-connector-python

#Installs required dependecies for Yahoo Finance Data
RUN pip install yfinance

# Switch to root user to install system level dependencies
USER root

# Install MySQL client for additional MySQL-related tools
RUN apt-get update && apt-get install -y mysql-client

# Switch back to airflow user
USER airflow
