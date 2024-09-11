from airflow import DAG
from datetime import datetime
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.bash import BashOperator

with DAG("dag_s3", start_date = datetime(2021,12,1),
         schedule_interval = "@daily", catchup = False) as dag:
            sensor_file_s3 = S3KeySensor(
                    task_id = "sensor_file_s3",
                    aws_conn_id = "s3_athena",
                    bucket_key = "s3://airflow-localiza/data-input/df_test.csv"
            )

            is_ok = BashOperator(
                    task_id = "is_ok",
                    bash_command = "echo 'File exists!'"
            )


            sensor_file_s3 >> is_ok