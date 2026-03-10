FROM apache/airflow:2.8.0

USER root
RUN apt-get update && \
    apt-get install -y default-jdk && \
    apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$PATH:$JAVA_HOME/bin

COPY jars/ /opt/airflow/jars/

USER airflow
RUN pip install pyspark==3.5.0 delta-spark==3.1.0 loguru faker