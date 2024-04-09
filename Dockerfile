FROM apache/airflow:2.1.0
COPY bashfile /bashfile
COPY dags/ /opt/airflow/dags/
ENTRYPOINT ["/bin/bash"]
CMD ["/bashfile"]
