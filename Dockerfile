FROM apache/airflow:2.1.0
COPY bashfile /init-airflow.sh
RUN chmod +x /init-airflow.sh
CMD ["/init-airflow.sh"]
