from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="hotel-datalake-pipeline",
    start_date=datetime(2026, 3, 9),
    schedule=None,
    catchup=False
) as dag:

    create_cluster = BashOperator(
        task_id='create_cluster',
        bash_command="""
        gcloud auth activate-service-account --key-file=$GOOGLE_APPLICATION_CREDENTIALS

        if gcloud dataproc clusters describe my-cluster --region=asia-southeast1 >/dev/null 2>&1; then
            echo "Cluster exists"
        else
            gcloud dataproc clusters create my-cluster \
                --region=asia-southeast1 \
                --zone=asia-southeast1-a \
                --single-node \
                --master-machine-type=e2-standard-4 \
                --master-boot-disk-size=50
        fi
        """
    )

    upload_to_gcs = BashOperator(
        task_id='upload_to_gcs',
        bash_command="""
        python /opt/airflow/utils/export_data_to_datalake.py
        """
    )

    upload_spark_job = BashOperator(
        task_id='upload_spark_job',
        bash_command="""
        gsutil cp /opt/airflow/spark-script/transform_lake_to_warehouse.py \
        gs://hotel-data-lake/jobs/
        """
    )

    submit_spark_job = BashOperator(
        task_id='submit_transform',
        bash_command="""
        gcloud auth activate-service-account --key-file=$GOOGLE_APPLICATION_CREDENTIALS

        gcloud dataproc jobs submit pyspark \
        gs://hotel-data-lake/jobs/transform_lake_to_warehouse.py \
        --cluster=my-cluster \
        --region=asia-southeast1 \
        """
    )
    delete_cluster = BashOperator(
        task_id='delete_cluster',
        bash_command="""
        gcloud auth activate-service-account --key-file=$GOOGLE_APPLICATION_CREDENTIALS

        gcloud dataproc clusters delete my-cluster \
        --region=asia-southeast1 \
        --quiet
        """
    )

    create_cluster >> upload_to_gcs >> upload_spark_job >> submit_spark_job >> delete_cluster