FROM apache/airflow:2.7.1-python3.9

USER root
# Install Java (Required for Spark)
RUN apt-get update && apt-get install -y openjdk-11-jdk && apt-get clean

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64

USER airflow
# Install PySpark inside the image build process
RUN pip install --no-cache-dir pyspark==3.5.0