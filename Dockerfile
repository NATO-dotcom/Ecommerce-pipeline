# Start from the official Airflow image
FROM apache/airflow:3.3.2

# Switch to the root user to install system-level packages (Java)
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         default-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Set the JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/default-java

# Switch back to the standard airflow user to install Python packages
USER airflow

# Install PySpark
RUN pip install --no-cache-dir pyspark