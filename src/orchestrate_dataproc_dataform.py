from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)
import os

PROJECT_ID = os.getenv("PROJECT_ID")
REGION = os.getenv("REGION")
REPOSITORY_ID = os.getenv("DATAFORM_REPO_ID")
DATAPROC_CLUSTER = os.getenv("DATAPROC_CLUSTER")
BUCKET = os.getenv("BUCKET")

PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": DATAPROC_CLUSTER},
    "pyspark_job": {"main_python_file_uri": f"gs://{BUCKET}/jobs/ingestion.py"},
}

with DAG(
    dag_id="orchestrate_dataproc_dataform",
    start_date=days_ago(1),
    schedule="*/30 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["medallion", "gcp"],
) as dag:

    ingest_raw = DataprocSubmitJobOperator(
        task_id="ingest_raw_dataproc",
        project_id=PROJECT_ID,
        region=REGION,
        job=PYSPARK_JOB,
    )

    compile_dataform = DataformCreateCompilationResultOperator(
        task_id="compile_dataform",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        compilation_result={"git_commitish": "main"},
    )

    run_dataform = DataformCreateWorkflowInvocationOperator(
        task_id="run_dataform_workflow",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workflow_invocation={
            "compilation_result": "{{ ti.xcom_pull('compile_dataform')['name'] }}"
        },
    )

    ingest_raw >> compile_dataform >> run_dataform
