FROM apache/airflow:2.7.1

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    git \
    libgomp1 \
    curl \
    apt-transport-https \
    ca-certificates \
    gnupg && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg \
    | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" \
    > /etc/apt/sources.list.d/google-cloud-sdk.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends google-cloud-sdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

# Installed at build time (instead of _PIP_ADDITIONAL_REQUIREMENTS on every
# container start) and pinned to Airflow's constraints so pip cannot upgrade
# Airflow itself.
RUN PYTHON_VERSION="$(python --version | cut -d ' ' -f 2 | cut -d '.' -f 1-2)" && \
    pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    apache-airflow-providers-google \
    google-cloud-storage \
    google-cloud-bigquery \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

RUN pip install --no-cache-dir git+https://github.com/mpgreg/airflow-provider-great-expectations.git@87a42e275705d413cd4482134fc0d94fa1a68e6f

RUN pip install --no-cache-dir apache-airflow-providers-docker==3.7.5
