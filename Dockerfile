FROM apache/airflow:2.1.0
COPY bashfile /bashfile
CMD ["/bashfile"]
