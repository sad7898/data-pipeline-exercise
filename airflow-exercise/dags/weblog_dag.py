from datetime import date, datetime, timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator

dagDefaultArgs = {
    "owner": "Sirathee K",
    "start_date": datetime.now(),
    "email": ["siradhee_k@hotmail.com"],
    "retries": 1,
}
with DAG(
    "process_web_log",
    "extract timestamp and size from weblog, transform and load to output tar file",
    default_args=dagDefaultArgs,
    schedule_interval=timedelta(days=1)
) as dag:
    data_source_url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0321EN-SkillsNetwork/ETL/accesslog.txt"
    extract = BashOperator(
        task_id="extract_data",
        bash_command="curl {0} | cut -d' ' -f1 > ../extracted_data.txt".format(data_source_url)
    )
    transform = BashOperator(
        task_id="transform_data",
        bash_command="grep '198.46.149.143' ../extracted_data.txt > ../transformed_data.txt"
    )
    load = BashOperator(
        task_id="load_data",
        bash_command="tar -cvf weblog.tar ../transformed_data.txt"
    )
    extract >> transform >> load
