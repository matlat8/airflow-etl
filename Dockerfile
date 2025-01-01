FROM apache/airflow:2.10.3
ADD requirements.txt .

# Install git
USER root
RUN apt-get update && apt-get install -y git

# Install python packages
USER airflow
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt
