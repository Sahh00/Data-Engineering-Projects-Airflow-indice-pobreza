from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from operators.extracao_indice import extracao
from airflow.utils.dates import days_ago
from airflow.models import DAG



with DAG(
    dag_id='PobrezaIndiceDAG',
    start_date=days_ago(1),
    catchup=True
) as dags:

    file_path_minio = "s3a://datalake/bronze/indices_pobreza_consolidado"

    
    extracao_indice = PythonOperator(
        task_id='extracao_indice',
        python_callable=extracao,
        op_kwargs={
            's3_path': file_path_minio, 
        }
    )

    transformacao_indice = SparkSubmitOperator(
        task_id='transformacao_indice',
        application='/home/dev-exata/Documents/projects/AirflowIndicesBrasil/src/spark/silver_indice.py',
        name='transformacao_indice',
        application_args=[
            '--path_origem', 's3a://datalake/bronze/indices_pobreza_consolidado.csv',
            '--path_dest', 's3a://datalake/silver'
        ],
        packages="io.delta:delta-core_2.12:1.0.0,org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-bundle:1.11.375",
        conf={
            "spark.hadoop.fs.s3a.endpoint": "http://localhost:9000",
            "spark.hadoop.fs.s3a.access.key": "minioadmin",
            "spark.hadoop.fs.s3a.secret.key": "minioadmin",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"
        },
        conn_id='spark_default',
        verbose=True,
    )

    gold_insiders_indice = SparkSubmitOperator(
        task_id='gold_indice',
        application='/home/dev-exata/Documents/projects/AirflowIndicesBrasil/src/spark/gold_indice.py',
        name='insiders_indice',
        application_args=[
            '--path_origem', 's3a://datalake/silver/indices_pobreza',
            '--path_dest', 's3a://datalake/gold'
        ],
        packages="io.delta:delta-core_2.12:1.0.0,org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-bundle:1.11.375",
        conf={
            "spark.hadoop.fs.s3a.endpoint": "http://localhost:9000",
            "spark.hadoop.fs.s3a.access.key": "minioadmin",
            "spark.hadoop.fs.s3a.secret.key": "minioadmin",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"
        },
        conn_id='spark_default',
        verbose=True)
    
extracao_indice >> transformacao_indice >> gold_insiders_indice