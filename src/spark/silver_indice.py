from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from os.path import join, os
import argparse


def transformacao_tmp_indice(df):
    
    tmp_indice_pobreza = df.select(
        col('referencia').alias('REFERENCIA'),
        col('periodo').alias('PERIODO'),
        col('pobreza').alias('POBREZA').cast('integer'),
        col('extrema_pobreza').alias('EXTREMA_POBREZA').cast('integer'),
        col('total').alias('TOTAL').cast('integer'),
        col('populacao_estimada').alias('POPULACAO_ESTIMADA').cast('integer'),
        col('porcentagem_pobreza').alias('PERCENTUAL_POBREZA').cast('float'),
        col('porcentagem_extrema_pobreza').alias('PERCENTUAL_EXTREMA_POBREZA').cast('float'),
        col('porcentagem_vulnerabilidade').alias('PERCENTUAL_VULNERABILIDADE').cast('float'),
        col('familias_pobreza').alias('FAMILIAS_POBREZA').cast('integer'),
        col('familias_extrema_pobreza').alias('FAMILIAS_EXTREMA_POBREZA').cast('integer'),
        col('familias_vulnerabilidade').alias('FAMILIAS_VULNERABILIDADE').cast('integer'),
        col('indigenas_extrema_pobreza').alias('INDIGENAS_EXTREMA_POBREZA').cast('integer'),
        col('indigenas_vulnerabilidade').alias('INDIGENAS_VULNERABILIDADE').cast('integer'),
        col('quilombolas_pobreza').alias('QUILOMBOLAS_POBREZA').cast('integer'),
        col('quilombolas_extrema_pobreza').alias('QUILOMBOLAS_EXTREMA_POBREZA').cast('integer'),
        col('quilombolas_vulnerabilidade').alias('QUILOMBOLAS_VULNERABILIDADE').cast('integer'),
        col('ciganos_pobreza').alias('CIGANOS_POBREZA').cast('integer'),
        col('ciganos_extrema_pobreza').alias('CIGANOS_EXTREMA_POBREZA').cast('integer'),
        col('ciganos_vulnerabilidade').alias('CIGANOS_VULNERABILIDADE').cast('integer')
    )
    return tmp_indice_pobreza

def export_to_delta(df, dest):
   
    df.write.format("delta").mode("overwrite").save(dest)


def main(spark, path_origem, path_dest):
    
    df = spark.read.csv(header=True, path=path_origem)
    df_pobreza_final = transformacao_tmp_indice(df)

    table_dest = join(path_dest, 'indices_pobreza')
    
    # Exporta para Delta
    export_to_delta(df_pobreza_final, table_dest)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Arguments Transformacao Indice Pobreza")

    parser.add_argument("--path_origem", required=True)
    parser.add_argument("--path_dest", required=True)
    
    args = parser.parse_args()


    spark = SparkSession.builder \
        .appName("TranformacaoMinIO") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


    main(spark, args.path_origem, args.path_dest)
    spark.stop()