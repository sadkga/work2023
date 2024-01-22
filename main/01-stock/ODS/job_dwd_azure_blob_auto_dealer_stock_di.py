# coding=utf-8
"""
 -- @ Creater      : sadkga sadkga@88.com
 -- @ Since        : 2024-01-22 14:02:43
 -- @ LastAuthor   : sadkga sadkga@88.com
 -- @ LastTime     : 2024-01-22 14:04:27
 -- @ Location     : \\code\\main\\01-stock\\ODS\\job_dwd_azure_blob_auto_dealer_stock_di.py
 -- @ Message      : 
 -- @ Copyright (c) 2024 by sadkga@88.com, All Rights Reserved. 
 """
"""
--********************************************************************--
--所属主题: 库存域
--功能描述: 进销商库存数据，经销商代码
--创建者: 王兆翔
--创建日期:2023-05-10
--********************************************************************--
"""




import pandas as pd
import os
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, HiveContext
def get_spark_connection():
    """创建spark对象"""
    spark = SparkSession.builder.config(conf=SparkConf().setAppName("pyspark—to-hive").set(
        "spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false").set("spark.driver.memory", "4g").set("spark.executor.memory", "4g")).enableHiveSupport().getOrCreate()
    sc = spark.sparkContext
    hc = HiveContext(sc)
    hc.setConf("hive.metastore.warehouse.dir",
               "/boschfs/warehouse/azure_blob/")
    return spark


def insert_data(df, database, data, tmp_table, schema):
    """数据与hive进行交互"""

    df = sparkConn.createDataFrame(df)
    df.createOrReplaceTempView(tmp_table)

    # 删表
    sql1 = """
    DROP TABLE IF EXISTS {0}.ods_azure_blob_auto_{1}_di
    """.format(database, data)

    # 建表
    sql2 = """
     create table if not exists {0}.ods_azure_blob_auto_{1}_di (
        {2}
    )
    partitioned by (ds string) stored as parquet 
    location "boschfs://boschfs/warehouse/{0}.ods_azure_blob_auto_{1}_di"
    """.format(database, data, schema)

    sql4 = """
    INSERT overwrite TABLE {0}.ods_azure_blob_auto_{1}_di PARTITION (ds=${bdp.system.bizdate})
    select {2} from {3}
    """.format(database, data, schema, tmp_table)

    sparkConn.sql(sql1)
    sparkConn.sql(sql2)
    sparkConn.sql(sql4)

    sparkConn.catalog.dropTempView("tmp_table")


if __name__ == '__main__':

    database = 'boschpro'
    data = 'dealer_stock'
    table = f'ods_azure_blob_auto_{data}_di'
    tmp_table = table + '_tmp_${bdp.system.bizdate}'

    sparkConn = get_spark_connection()
    sparkConn.sparkContext.setLogLevel("Error")

    sql2 = """
    show tables in {0} like 'ods_*auto_dealer_stock_*_df'
    """.format(database)
    df = sparkConn.sql(sql2)
    df.show()

    # 获取ods对应的数据表名
    table_list = []
    print(df)
    for table in df.collect():
        print(table)
        table_name = table.tableName
        table_list.append(table_name)

    # 获取合并的df列表

    df_list = []

    # 取出数据合并
    for table in table_list:
        kehu = table.split('_')[6]

        sql = """
        select * from {0}.{1} where ds = '${bdp.system.bizdate}'
        """.format(database, table)

        df = sparkConn.sql(sql)
        df = df.toPandas()
        df['kehu'] = kehu
        df.rename(columns=str.lower, inplace=True)
        df_list.append(df)

    df_merge = pd.concat(df_list)

    col = list(df_merge.columns)
    print(col)

    for col_name in col:
        df_merge[col_name] = df_merge[col_name].astype(str)

    col.remove('ds')
    schema = ' string,'.join(col) + ' string'

    insert_data(df_merge, database, data, tmp_table, schema)
