import uuid
import datetime
from airflow import DAG
from airflow.providers.yandex.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator,
)


YC_FOLDER_ID = "b1gdgk3cgb5jadldklmk"                 
YC_DP_SSH_PUBLIC_KEY = "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDgLOH55VItHoxllYDTUOca9TLyMSRa5dF0qX7cRTu4ffu0nhNRaWNBEj1ZfN4b8ic0wD0gGvf6ifSVH5UG/48TvU2IYwUBawsQ6EeUqXvjHz21mVZK7YIE7pHowuilyqSvj1ynQa3IWPAsYIZZueBwqoDgVSttyGlJAXjt7lIrvzD2BMliJ/RFqBfs0q5MkPvBhPBUYRIUGy5PU31t+D32UiBYTg9ry28T8cAtI6Hjt/4KzY2kFOW0MqP74kJQTWQOWtNZSKKKusV3K2IXUOLafYUiSznPAqU0ckWbljz7xr3RUQOeMj56VJ/A7pTrbDm+WYf46wBUnxISh9e/pjYN"          
YC_DP_SUBNET_ID = "e9bc1da6og6g6657dcl2"                            
YC_DP_SA_ID = "ajenq329fgeh02klfc9d"                 
YC_DP_AZ = 'ru-central1-a'
YC_DP_LOGS_BUCKET = "dp-logs-pavlova"
YC_SOURCE_BUCKET = "dp-data-pavlova"
YC_SCRIPTS_BUCKET = "dp-scripts-pavlova"


default_args = {
    'owner': 'airflow',
    'start_date': datetime.datetime(2026, 1, 1),
}

with DAG(
    dag_id='credit_risk_etl',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['credit_risk', 'spark'],
) as dag:

    # 1. Создание кластера
    create_cluster = DataprocCreateClusterOperator(
        task_id='create_cluster',
        folder_id=YC_FOLDER_ID,
        cluster_name=f'credit-risk-{uuid.uuid4()}',
        ssh_public_keys=YC_DP_SSH_PUBLIC_KEY,
        subnet_id=YC_DP_SUBNET_ID,
        s3_bucket=YC_DP_LOGS_BUCKET,
        service_account_id=YC_DP_SA_ID,
        zone=YC_DP_AZ,
        cluster_image_version='2.1',
        masternode_resource_preset='s2.small',
        masternode_disk_size=80,
        computenode_resource_preset='s2.small',
        computenode_disk_size=80,
        computenode_count=1,
        services=['YARN', 'SPARK', 'HDFS'],
        connection_id='yc-airflow-sa',
    )

    # 2. Запуск PySpark-задачи (исправлено: args вместо job_args)
    run_etl = DataprocCreatePysparkJobOperator(
        task_id='run_etl',
        cluster_id=create_cluster.output['cluster_id'],
        main_python_file_uri=f's3a://{YC_SCRIPTS_BUCKET}/scripts/process_loans.py',
        args=[
            f's3a://{YC_SOURCE_BUCKET}/input/df_2014-18_selected.csv',
            f's3a://{YC_SOURCE_BUCKET}/output/{{{{ ds }}}}'
        ],
        connection_id='yc-airflow-sa',
    )

   
    delete_cluster = DataprocDeleteClusterOperator(
        task_id='delete_cluster',
        cluster_id=create_cluster.output['cluster_id'],
        connection_id='yc-airflow-sa',
        trigger_rule='all_done',
    )

    create_cluster >> run_etl >> delete_cluster