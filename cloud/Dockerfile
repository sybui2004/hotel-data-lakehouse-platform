FROM apache/airflow:2.7.1

USER root

RUN apt-get update && \
    apt-get install -y \
    git \
    libgomp1 \
    curl \
    apt-transport-https \
    ca-certificates \
    gnupg

RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" \
    > /etc/apt/sources.list.d/google-cloud-sdk.list

RUN curl https://packages.cloud.google.com/apt/doc/apt-key.gpg \
    | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg

RUN apt-get update && \
    apt-get install -y google-cloud-sdk

USER airflow

RUN pip install git+https://github.com/mpgreg/airflow-provider-great-expectations.git@87a42e275705d413cd4482134fc0d94fa1a68e6f

RUN pip install apache-airflow-providers-docker==3.7.5