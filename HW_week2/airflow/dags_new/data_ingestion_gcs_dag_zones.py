import os
import logging

from datetime import datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
import pyarrow.csv as pv
import pyarrow.parquet as pq

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")


gcp_zones_upload_workflow = DAG(
    dag_id="data_ingestion_gcs_dag_zones",
    start_date=datetime.today(),
    schedule_interval="@once",
    tags=['dtc-de'],
)

zones_url = 'https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv'
zones_shapes_url = 'https://s3.amazonaws.com/nyc-tlc/misc/taxi_zones.zip'
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


# NOTE: DAG declaration - using a Context Manager (an implicit way)
with gcp_zones_upload_workflow:

    download_zones_csv_task = BashOperator(
        task_id="download_zones_csv_task",
        bash_command=f"curl -sSLf {zones_url} > {path_to_local_home}/zones.csv"
    )

    # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
    local_zones_csv_to_gcs_task = PythonOperator(
        task_id="local_zones_csv_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": "raw/zones.csv",
            "local_file": f"{path_to_local_home}/zones.csv",
        },
    )

    download_zones_zip_task = BashOperator(
        task_id="download_zones_zip_task",
        bash_command=f"curl -sSLf {zones_shapes_url} > {path_to_local_home}/zones_shapes.zip"
    )

    # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
    local_zones_zip_to_gcs_task = PythonOperator(
        task_id="local_zones_zip_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": "raw/zones_shapes.zip",
            "local_file": f"{path_to_local_home}/zones_shapes.zip",
        },
    )

    download_zones_csv_task >> local_zones_csv_to_gcs_task >> download_zones_zip_task >> local_zones_zip_to_gcs_task
