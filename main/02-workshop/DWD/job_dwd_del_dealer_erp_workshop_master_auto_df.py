"""
--********************************************************************--
--所属主题: 库存域
--功能描述: extra与erp的workshop数据
--创建者: 王兆翔
--创建日期:2023-05-10
--********************************************************************--
"""
import pandas as pd
import datetime
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
    hc.setConf("hive.metastore.warehouse.dir", "/boschfs/warehouse/azure_blob/")
    return spark

def insert_data(df, database, table, tmp_table, schema):
    """数据与hive进行交互"""

    df = sparkConn.createDataFrame(df)
    df.createOrReplaceTempView(tmp_table)

     # 删表
    sql1 = """
    DROP TABLE IF EXISTS {0}.{1}
    """.format(database,table)


    # 建表
    sql2= """
     create table if not exists {0}.{1} (
        {2}
    )
    partitioned by (pday string) stored as parquet 
    location "boschfs://boschfs/warehouse/{0}.{1}"
    """.format(database, table,schema)
    print(table,database,schema)

    sql4 = """
    insert overwrite table {0}.{1} partition (pday=${bdp.system.bizdate})
    select {2} from {3}
    """.format(database, table,schema,tmp_table)


    
    # sparkConn.sql(sql1)
    sparkConn.sql(sql2)
    sparkConn.sql(sql4)

    sparkConn.catalog.dropTempView("tmp_table")


if __name__ == '__main__':

    database = 'boschpro'
    table = 'dwd_del_dealer_erp_workshop_master_df'
    tmp_table = table + '_tmp_${bdp.system.bizdate}'

    

    sparkConn = get_spark_connection()
    sparkConn.sparkContext.setLogLevel("Error")


    sql2 = """
    show tables in {0} like 'ods_mau_dealer_erp_workshop_master_*_df'
    """.format(database)
    df=sparkConn.sql(sql2)
    df.show()
    
    # 获取ods对应的数据表名
    table_list = []
    print(df)
    for t in df.collect():
        print(table)
        table_name = t.tableName
        table_list.append(table_name)

    # 获取当前日期
    current_time = datetime.datetime.now()
    formatted_time = current_time.strftime("%Y-%m-%d %H:%M:%S")
    
    
    # 取出数据合并
    df_list = []
    for tn in table_list:
        
        sql = """
        select * from {0}.{1} where pday = '${bdp.system.bizdate}'
        """.format(database,tn)

        df = sparkConn.sql(sql)
        df = df.toPandas()
        df.rename(columns=str.lower, inplace=True)
        df_list.append(df)


    df_merge = pd.concat(df_list)
    df_merge['etl_load_time'] = formatted_time
    col = list(df_merge.columns)
    col.remove('pday')
    print(col)

    for col_name in col:
      df_merge[col_name] = df_merge[col_name].astype(str)
    
    
    schema = ' string,'.join(col) + ' string'
 
    insert_data(df_merge, database, table, tmp_table, schema)
