from pyspark.sql import SparkSession
from pyspark import SparkConf
from os.path import join, os
import pandas as pd
import argparse


def insider_vulnerabilidade(spark,df):
    df.createOrReplaceTempView("TMP_VULNERABILIDADE_POBREZA")

    df_vulnerabilidade_pobreza_final = spark.sql("""
        SELECT DISTINCT
            FORMAT_NUMBER(POPULACAO_ESTIMADA,2) AS POPULACAO_ESTIMADA,
            SUM(INDIGENAS_EXTREMA_POBREZA) AS INDIGENAS_EXTREMA_POBREZA,
            SUM(QUILOMBOLAS_EXTREMA_POBREZA) AS QUILOMBOLAS_EXTREMA_POBREZA,
            SUM(CIGANOS_EXTREMA_POBREZA) AS CIGANOS_EXTREMA_POBREZA,
            regexp_replace(REFERENCIA, '/', '-') AS DATA_REFERENCIA
        FROM TMP_VULNERABILIDADE_POBREZA
        WHERE 1=1
        GROUP BY regexp_replace(REFERENCIA, '/', '-'), POPULACAO_ESTIMADA
        ORDER BY DATA_REFERENCIA DESC
                                             
    """)
    return df_vulnerabilidade_pobreza_final



def export_to_delta(df, dest):
    df.write.format("delta").mode("overwrite").save(dest)


def main(spark, path_origem, path_dest):
    
    df = spark.read.format("delta").load(path_origem)
    df_pobreza_final = insider_vulnerabilidade(spark,df)

    table_dest = join(path_dest, 'insiders_vulnerabilidade_pobreza')
    
    #Exporta para Delta
    export_to_delta(df_pobreza_final, table_dest)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Arguments Insiders Vulnerabilidade Pobreza")

    parser.add_argument("--path_origem", required=True)
    parser.add_argument("--path_dest", required=True)
    
    args = parser.parse_args()


    spark = SparkSession.builder \
        .appName("MinioGold") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


    main(spark, args.path_origem, args.path_dest)
    spark.stop()