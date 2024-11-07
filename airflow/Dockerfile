FROM apache/airflow:slim-2.10.2-python3.12

USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-cncf-kubernetes==8.4.2 \
    psycopg2-binary==2.9.9

RUN pip install apache-airflow-providers-postgres==5.13.1 \
    numpy==2.1.3 pandas==2.2.3

# Switch to root to change permissions
USER root

# Copy plugins to the Airflow plugins directory
COPY plugins /opt/airflow/plugins

# Update permissions for the /opt/airflow directory
RUN chown -R airflow /opt/airflow

# Switch back to airflow user
USER airflow