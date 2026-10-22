from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime

CLUSTER_NAME = "my-cluster"
REGION = "asia-southeast1"
ZONE = "asia-southeast1-a"
BUCKET = "hotel-data-lake"

GCLOUD_AUTH = "gcloud auth activate-service-account --key-file=$GOOGLE_APPLICATION_CREDENTIALS"

with DAG(
    dag_id="hotel-datalake-pipeline",
    start_date=datetime(2026, 3, 9),
    schedule=None,
    catchup=False
) as dag:

    create_cluster = BashOperator(
        task_id='create_cluster',
        bash_command=f"""
        {GCLOUD_AUTH}

        if gcloud dataproc clusters describe {CLUSTER_NAME} --region={REGION} >/dev/null 2>&1; then
            echo "Cluster exists"
        else
            gcloud dataproc clusters create {CLUSTER_NAME} \
                --region={REGION} \
                --zone={ZONE} \
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
        bash_command=f"""
        {GCLOUD_AUTH}

        gsutil cp /opt/airflow/spark-script/transform_lake_to_warehouse.py \
        gs://{BUCKET}/jobs/
        """
    )

    submit_spark_job = BashOperator(
        task_id='submit_transform',
        bash_command=f"""
        {GCLOUD_AUTH}

        gcloud dataproc jobs submit pyspark \
        gs://{BUCKET}/jobs/transform_lake_to_warehouse.py \
        --cluster={CLUSTER_NAME} \
        --region={REGION}
        """
    )

    # Always tear down the cluster, even if an upstream task failed,
    # so a broken run does not leave a billable cluster running.
    delete_cluster = BashOperator(
        task_id='delete_cluster',
        trigger_rule=TriggerRule.ALL_DONE,
        bash_command=f"""
        {GCLOUD_AUTH}

        gcloud dataproc clusters delete {CLUSTER_NAME} \
        --region={REGION} \
        --quiet
        """
    )

    # delete_cluster runs with ALL_DONE, so it would mark the run as successful
    # even when the transform failed. This watcher fails the run in that case.
    @task(trigger_rule=TriggerRule.ONE_FAILED, retries=0)
    def watcher():
        raise AirflowException("One or more upstream tasks failed.")

    # The data upload uses the Python client (no gcloud state), so it can run
    # while the cluster is being created.
    create_cluster >> upload_spark_job
    [upload_spark_job, upload_to_gcs] >> submit_spark_job >> delete_cluster

    list(dag.tasks) >> watcher()
