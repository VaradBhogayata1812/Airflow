# Using a newer stable version of Apache Airflow
FROM apache/airflow:2.4.0

# Running as root to install packages
USER root

# Install necessary tools and Google Cloud SDK
RUN apt-get update && \
    apt-get install -y curl gnupg lsb-release && \
    echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg && \
    apt-get update && \
    apt-get install -y google-cloud-sdk

# Change back to the airflow user for security reasons
USER airflow

COPY etlkeys.json /opt/airflow/
ENV GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/etlkeys.json
COPY bashfile /bashfile
COPY dags/ /opt/airflow/dags/
ENTRYPOINT ["/bin/bash"]
CMD ["/bashfile"]
