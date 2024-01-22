"""
--********************************************************************--
--所属主题: 库存域
--功能描述: 进销商库存数据料号mapping merge
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
    hc.setConf("hive.metastore.warehouse.dir", "/boschfs/warehouse/azure_blob/")
    return spark

def insert_data(df, database, data, tmp_table, schema):
    """数据与hive进行交互"""



     # 删表
    sql1 = """
    DROP TABLE IF EXISTS {0}.{1}
    """.format(database, data)



    # 建表
    sql2= """
     create table if not exists {0}.{1} (
        {2}
    )
    partitioned by (ds string) stored as parquet 
    location "boschfs://boschfs/warehouse/{0}.{1}"
    """.format(database, data,schema)

    sparkConn.sql(sql1)
    
    sparkConn.sql(sql2)


    # 测试上传列与hive历史表已有列是否匹配
    test = """
    select * from {0}.{1} limit 1
    """.format(database, data)

    t=sparkConn.sql(test)
    
    for col in list(df.columns):
        if col not in list(t.columns) and col != 'ds':
            sql3=""" 
            ALTER TABLE {0}.{1} add columns ({2} STRING comment '')
            """.format(database, data,col)
            sparkConn.sql(sql3)
            

        
 
    
    for col in list(t.columns):
        if col not in list(df.columns) and col != 'ds':
            df[col] = 'NULL'
    print(df.columns)        
    print(t.columns) 

    schema1 = ','.join(t.columns).replace(',ds', '').replace(',id', '')   
    print(schema1)
                

    df = sparkConn.createDataFrame(df)
    df.createOrReplaceTempView(tmp_table)


    sql4 = """
    INSERT overwrite TABLE {0}.{1} PARTITION (ds=${bdp.system.bizdate})
    select {2} from {3}
    """.format(database, data,schema1,tmp_table)

    sql5 = """
    alter table {0}.{1} drop partition(ds < ${yyyyMMdd, -7d});
    """.format(database, data)


    sparkConn.sql(sql4)
    # sparkConn.sql(sql5)
    sparkConn.catalog.dropTempView("tmp_table")


if __name__ == '__main__':

    database = 'boschpro'
    table = f'dwd_del_dealer_product_code_map_df'
    tmp_table = table + '_tmp_${bdp.system.bizdate}'

    

    sparkConn = get_spark_connection()
    sparkConn.sparkContext.setLogLevel("Error")


    sql2 = """
    show tables in {0} like 'ods_dealer_product_code_mapping_*'
    """.format(database)
    df=sparkConn.sql(sql2)
    df.show()
    
    # 获取ods对应的数据表名
    table_list = []

    for i in df.collect():
        print(i)
        table_name = i.tableName
        table_list.append(table_name)

    # 获取合并的df列表
    
    df_list = []
    print(table_list)
    

    # 取出数据合并
    for i in table_list:
        kehu = i.split('_')[5]

        sql = """
        select * from {0}.{1} where ds = '${bdp.system.bizdate}'
        """.format(database,i)

        df = sparkConn.sql(sql)
        df = df.toPandas()
        df['customercode'] = kehu
        df.rename(columns=str.lower, inplace=True)
        df_list.append(df)


    df_merge = pd.concat(df_list)
    print(df_merge)

    col = list(df_merge.columns)
    print(col)

    for col_name in col:
        df_merge[col_name] = df_merge[col_name].astype(str)
    
    col.remove('ds')
    schema = ' string,'.join(col) + ' string'

    insert_data(df_merge, database, table, tmp_table, schema)