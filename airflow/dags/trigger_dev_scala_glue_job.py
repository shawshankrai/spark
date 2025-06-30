from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
from datetime import datetime, timedelta

with DAG(
    dag_id="trigger_dev_scala_glue_job",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Set to your desired schedule or use Airflow's scheduler
    catchup=False,
    default_args={
        'retries': 3,
        'retry_delay': timedelta(minutes=10),
    },
) as dag:

    run_glue = AwsGlueJobOperator(
        task_id="run_dev_scala_glue_job",
        job_name="dev-scala-glue-job",  # From your dev.tfvars
        region_name="us-east-1",        # From your dev.tfvars
        retries=3,
        retry_delay=timedelta(minutes=10),
        # Optionally, you can pass script_args or override script_location if needed
        # script_location="s3://my-dev-glue-bucket/scripts/spark-project-assembly-0.1.0.jar",
        # script_args={"--key": "value"}
    ) 