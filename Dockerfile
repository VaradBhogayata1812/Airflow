FROM apache/airflow:2.1.0
COPY bashfile /bashfile
ENTRYPOINT ["/bin/bash"]
CMD ["/bashfile"]
ENV GOOGLE_APPLICATION_CREDENTIALS C:\Users\varad\OneDrive\Desktop\etl-project-418923-8dc23d8dbe8c
COPY keyfile.json C:\Users\varad\OneDrive\Desktop\etl-project-418923-8dc23d8dbe8c
