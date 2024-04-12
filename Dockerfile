FROM apache/airflow:2.1.0

# Install dependencies for Google Cloud SDK
USER root
RUN apt-get update && apt-get install -y curl gnupg && \
    echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add - && \
    apt-get update && apt-get install -y google-cloud-sdk

USER airflow

COPY home/varadbhogayata78/etl-project-418923-8dc23d8dbe8c.json /opt/airflow/
ENV GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/etl-project-418923-8dc23d8dbe8c.json
COPY bashfile /bashfile
COPY dags/ /opt/airflow/dags/
ENTRYPOINT ["/bin/bash"]
CMD ["/bashfile"]
