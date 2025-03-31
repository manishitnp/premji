FROM apache/airflow:2.8.1-python3.8

RUN pip install --no-cache-dir --upgrade wget
RUN pip install --no-cache-dir --upgrade pip
RUN python -m pip install --upgrade pip
RUN pip install --no-cache-dir --upgrade awscli

RUN pip install --no-cache-dir --upgrade python-dotenv
RUN pip install --no-cache-dir --upgrade unzip

ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW_ENV=PROD
ENV PYTHONPATH=/opt/airflow


RUN mkdir -p /opt/airflow/dependencies
COPY requirements.txt /opt/airflow/dependencies/
RUN pip install -r /opt/airflow/dependencies/requirements.txt
RUN rm -rf /opt/airflow/dependencies/

COPY dags /opt/airflow/dags

COPY airflow/airflow.cfg /opt/airflow/airflow.cfg