FROM apache/airflow:2.1.0
COPY bashfile
RUN chmod +x /init-airflow.sh
CMD ["/init-airflow.sh"]
