FROM apache/airflow:3.0.6
COPY requirements.txt /requirements.txt
# Pinning apache-airflow stops pip from upgrading Airflow by accident
RUN pip install --no-cache-dir "apache-airflow==3.0.6" -r /requirements.txt
