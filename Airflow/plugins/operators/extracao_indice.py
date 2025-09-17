import kagglehub
from kagglehub import KaggleDatasetAdapter
import pandas as pd
import boto3
import io
import os

def extracao(s3_path: str):
    # Chaves de acesso do MinIO
    ACCESS_KEY = "minioadmin"
    SECRET_KEY = "minioadmin"
    MINIO_ENDPOINT = "http://localhost:9000"

    print("Iniciando processo de extração e upload para MinIO...")

    # 1. Configurar o cliente S3 para MinIO
    try:
        s3_client = boto3.client(
            's3',
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
            config=boto3.session.Config(signature_version='s3v4')
        )
        print("Cliente S3 (MinIO) configurado com sucesso.")
    except Exception as e:
        print(f"Erro ao configurar o cliente S3: {e}")
        raise

    # 2. Carrega os dados usando KaggleHub (retorna um DataFrame do Pandas)
    try:
        pandas_df = kagglehub.load_dataset(
            KaggleDatasetAdapter.PANDAS,
            "fidelissauro/indices-pobreza-brasil",
            "indices_pobreza_consolidado.csv",
        )
        print("Dados do Kaggle carregados no DataFrame do Pandas.")
        print("Primeiras 5 linhas do DataFrame:\n", pandas_df.head())
    except Exception as e:
        print(f"Erro ao carregar dados do KaggleHub: {e}")
        raise

    # 3. Preparar o caminho S3
    if s3_path.startswith("s3a://"):
        clean_s3_path = s3_path[len("s3a://"):]
    elif s3_path.startswith("s3://"):
        clean_s3_path = s3_path[len("s3://"):]
    else:
        print("Caminho S3 inválido. Deve começar com 's3a://' ou 's3://'.")
        raise ValueError("Caminho S3 inválido")

    parts = clean_s3_path.split('/', 1)
    if len(parts) < 2:
        print("Caminho S3 não contém bucket e chave de objeto.")
        raise ValueError("Caminho S3 incompleto")

    bucket_name = parts[0]
    object_key = parts[1]
    
    # Adicionando a extensão .csv se o caminho não a tiver
    if not object_key.lower().endswith(".csv"):
        object_key = object_key.rsplit('.', 1)[0] + ".csv"
    
    print(f"Bucket de destino: {bucket_name}")
    print(f"Chave do objeto de destino: {object_key}")

    # 4. Converte o DataFrame do Pandas para um buffer de arquivo CSV
    try:
        csv_buffer = io.BytesIO()
        pandas_df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        print(f"DataFrame Pandas convertido para CSV em memória. Tamanho: {len(csv_buffer.getvalue())} bytes")
    except Exception as e:
        print(f"Erro ao converter DataFrame para CSV: {e}")
        raise

    # 5. Envia o arquivo para o MinIO
    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body=csv_buffer.getvalue()
        )
        print(f"Dados salvos com sucesso em: s3://{bucket_name}/{object_key}")
    except Exception as e:
        print(f"Erro ao enviar arquivo para o MinIO em s3://{bucket_name}/{object_key}: {e}")
        raise
    
    print("Processo de extração e upload concluído com sucesso.")