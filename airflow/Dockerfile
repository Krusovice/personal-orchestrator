FROM apache/airflow:2.10.5

USER root

RUN apt-get update && \
    apt-get install -y rclone && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow
