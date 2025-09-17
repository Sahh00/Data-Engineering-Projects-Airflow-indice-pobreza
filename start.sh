#!/bin/bash

source venv/bin/activate

export SPARK_HOME="/home/dev-exata/Documents/projects/AirflowIndicesBrasil/spark-3.1.3-bin-hadoop3.2"

export AIRFLOW_HOME="$(pwd)/Airflow"


#$SPARK_HOME/sbin/start-thriftserver.sh &> /tmp/spark-thriftserver.log &

disown
# Executa o Airflow em modo standalone
exec airflow standalone