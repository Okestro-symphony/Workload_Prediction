import os
import sys
from configparser import ConfigParser
import socket
import time

from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import ArrayType, DoubleType, StructType, StructField, StringType, TimestampType
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import pandas_udf, PandasUDFType, lit, when
import pandas as pd
import pymysql


from dependencies.elastic_spark import ElasticSpark
from prediction_models_spark import predict_prophet
from prediction_models_spark import predict_arima
from prediction_models_spark import predict_autoreg

project_path = os.path.dirname(os.path.dirname(__file__))

if socket.gethostname() == 'bigdata-hdfs-spark-0-3':
    config = ConfigParser()
    config.read(project_path+'/../config.ini')
    config.set('SPARK','MASTER',"local[8]")
else:
    config = ConfigParser()
    config.read('config.ini')

hdfs_host, hdfs_port, hdfs_id, hdfs_pw = config.get('HDFS', 'HOST'), config.get('HDFS', 'PORT'),  config.get('HDFS', 'USER'), config.get('HDFS', 'PASSWORD')

db_host, db_port, db_id, db_pw, db_schema = config.get('MARIADB', 'HOST'), config.get('MARIADB', 'PORT'),  config.get('MARIADB', 'USER'), config.get('MARIADB', 'PASSWORD'), config.get('MARIADB','DATABASE')
db = pymysql.connect(host=db_host,port=int(db_port), user= db_id, passwd=db_pw, db= db_schema, charset='utf8')

def retrieve_machine_to_predict():
    return ["vm"]

def retrieve_model_to_predict(db):
    sql = "SELECT * FROM T_ALGORITHM"
    df=pd.read_sql(sql,db)
    df = df[df['USG_AT']=='Y'][['PVDR_ID','USG_AT','ALGORITHM_NM']]
    model_dict = {}
    for index, row in df.iterrows():
        machine , model = row['PVDR_ID'], row['ALGORITHM_NM']
        machine = 'vm' if 'openstack' in machine else 'pod'
        model_dict[machine]= model
    try:
        model_dict['vm']
        model_dict['pod']
        return model_dict
    except:
        return {'vm':'Prophet','pod':'Prophet'}

def retrieve_selected_model(model_name):
    model = predict_prophet
    if model_name == 'Prophet':
        model = predict_prophet
    if model_name == 'Auto arima':
        model = predict_arima
    if model_name == 'Autoreg':
        model = predict_autoreg
    return model


def retrieve_metrics_to_predict():
    return {"vm": ["cpu"], "pod":[]}

def retrieve_df_from_csv(spark_session, path):
    df = spark_session.read \
        .option("header", True) \
        .format("csv").load(path)
    return df

def retrieve_df_from_parquet():
    pass

def change_df_column_names(df, metric):
    if metric == 'cpu':
        df = df.select(F.col('datetime').alias('timestamp'), F.col('mean_cpu_usage').alias('avg(value)'),
                       F.col('host_name').alias('host_id'))
    if metric == 'memory':
        df = df.select(F.col('datetime').alias('timestamp'), F.col('mean_memory_usage').alias('avg(value)'),
                       F.col('host_name').alias('host_id'))
    if metric == 'network-in':
        df = df.select(F.col('datetime').alias('timestamp'), F.col('mean_network_in_bytes').alias('avg(value)'),
                       F.col('host_name').alias('host_id'))
    if metric == 'network-out':
        df = df.select(F.col('datetime').alias('timestamp'), F.col('mean_network_out_bytes').alias('avg(value)'),
                       F.col('host_name').alias('host_id'))
    if metric == 'diskio-read':
        df = df.select(F.col('datetime').alias('timestamp'), F.col('mean_read_bytes').alias('avg(value)'),
                       F.col('host_name').alias('host_id'))
    if metric == 'diskio-write':
        df = df.select(F.col('datetime').alias('timestamp'), F.col('mean_write_bytes').alias('avg(value)'),
                       F.col('host_name').alias('host_id'))

    return df

def main():
    start = time.time()
    model_dict = retrieve_model_to_predict(db)
    metric_dict = retrieve_metrics_to_predict()
    print(f"model_dict: {model_dict} , metric_dict: {metric_dict} ")
    elastic_spark = ElasticSpark(config=config, app_name="prediction_job")
    spark_session = elastic_spark.spark_session
    metrics_to_predict = retrieve_metrics_to_predict()
    for machine in retrieve_machine_to_predict():
        model_selected_by_machine = model_dict[machine]
        metrics_selected_by_machine = metric_dict[machine]
        for metric in metrics_selected_by_machine:
            metric_to_read = metric.split("-")[0] if machine == 'vm' else 'pod'
            df = retrieve_df_from_csv(spark_session,
                                      f"hdfs://{hdfs_id}:{hdfs_pw}@{hdfs_host}:{hdfs_port}/preprocessed_data/{metric_to_read}.csv")
            df = change_df_column_names(df, metric)
            df = df.withColumn('metric',lit(metric))
            model = retrieve_selected_model(model_selected_by_machine)
            result_df = df.groupBy('host_id') \
                .apply(model) \
                .withColumnRenamed('host_id', 'host_id') \
                .withColumnRenamed('ds', 'timestamp') \
                .withColumnRenamed('yhat', 'predict_value') \
                .withColumnRenamed('yhat_lower', 'predict_min') \
                .withColumnRenamed('yhat_upper', 'predict_max') \
                .withColumn('model', lit(model_selected_by_machine))
            result_df.explain()
            elastic_spark.load_dataframe_to_es(result_df, f'aiplatform-metric-{machine}-prediction-{metric}-spark-test', mode="Overwrite")
            end = time.time()
            print(f"elapsed_time:{end - start}")
if __name__ == '__main__':
    main()
