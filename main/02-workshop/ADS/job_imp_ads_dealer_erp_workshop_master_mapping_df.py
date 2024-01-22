"""
--********************************************************************--
--所属主题: 库存域
--功能描述: extra与erp的workshop数据
--创建者: 王兆翔
--创建日期:2023-05-10
--********************************************************************--
"""
# -*- coding: utf-8 -*-
import requests
import json
import datetime
import pandas as pd
import difflib
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, HiveContext,Window
from pyspark.sql.functions import lit,col,udf,when,row_number,desc
from pyspark.sql.types import *

def get_spark_connection():
    """创建spark对象"""
    spark = SparkSession.builder.config(conf=SparkConf()
            .setAppName("ads_workshop_mapping")
            .set("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
            .set("spark.driver.memory", " 8g")
            .set("spark.executor.memory", "8g")
            .set("spark.executor.instances", "6")
            .set("spark.executor.cores", '8')
            .set('spark.sql.session.timeZone','Asia/Shanghai')) \
            .enableHiveSupport() \
            .getOrCreate()
    sc = spark.sparkContext
    hc = HiveContext(sc)
    hc.setConf("hive.metastore.warehouse.dir", "/boschfs/warehouse/azure_blob/")
    return spark


def process_addr_sfe(df,addr):
    """去除特殊字符"""
    print(df.shape)
    df[addr] = df[addr] \
            .replace("`", "", regex=True) \
            .replace("#", "", regex=True) \
            .replace("/n",'', regex=True) \
            .replace("-",'', regex=True) \
            .replace("0",'', regex=True) \
            .replace("/",'', regex=True) \
            .replace('"','', regex=True) \
            .replace('\n','', regex=True) \

    return df


# udf, 处理返回相似度
def similarity(s1, s2):
    return difflib.SequenceMatcher(None, s1, s2).ratio()

if __name__ == '__main__':
    # spark
    sparkConn = get_spark_connection()
    sparkConn.sparkContext.setLogLevel("Error")
    sparkConn.conf.set('spark.sql.session.timeZone','Asia/Shanghai')                # 时区
    sparkConn.sql('set spark.sql.hive.convertMetastoreParquet=false')               # 序列化parquet
    sparkConn.sql('set mapreduce.input.fileinputformat.input.dir.recursive=true')   # 文件递归读取

    # defind
    database = 'boschpro'
    target_table = 'ads_dealer_erp_workshop_master_mapping_df'
    erp_table = 'dwd_del_dealer_erp_workshop_master_df'
    extra_table = 'dim_wks_extra_workshop_master_data'
    dim_table = 'dim_pub_standard_address_mf'
    entity_table = 'ods_extra_dbo_client_external_entity_df'
    erp_source = 'erp'
    extra_source = 'extra'
    erp_select = {
                   'workshop_name'      :  'erp_workshop_name',
                   'erp_workshopid'     :  'erp_workshop_client_code',
                   'contact_mobile'     :  'erp_workshop_contact_mobile',
                   'workshop_address'   :  'erp_workshop_address',
                   'from_app'           :  'erp_data_source'
                   }

    extra_select = {
                    'client_name'   : 'extra_workshop_name',
                    'client_code'   : 'extra_workshop_client_code',
                    'contact_phone' : 'extra_workshop_contact_mobile',
                    'detail_address': 'extra_workshop_address',
                    'client_id'     : 'extra_workshop_internal_id'
                    }

    erp_rname = {
                    'standard_province' : 'erp_workshop_province',
                    'standard_city'     : 'erp_workshop_city',
                    'standard_district' : 'erp_workshop_district'
                    }

    extra_rname = {
                    'standard_province' : 'extra_workshop_province',
                    'standard_city'     : 'extra_workshop_city',
                    'standard_district' : 'extra_workshop_district'
                    }

    # dim_pub_standard_address_mf
    dim = sparkConn.sql(f'select * from {database}.{dim_table}')
    entity = sparkConn.sql(f'select distinct external_entity,external_app,client_id from {database}.{entity_table} where ds = ${bdp.system.bizdate} and external_app like "11800%"')

    """todo1: 匹配省市区"""
    # ERP
    key_erp = list(erp_select.keys())
    key1 = ','.join(key_erp)
    df = sparkConn.sql(
    f"select {key1} from {database}.{erp_table} where pday = ${bdp.system.bizdate} and workshop_address rlike '[\u4e00-\u9fa5]+'")

    df =(df.join(dim,
                    (df[key_erp[1]]  == dim['reference_id']) &
                    (df[key_erp[3]]  == dim['address'])
                    ,'left_outer')) \
            .select( key_erp[0]
                    ,key_erp[1]
                    ,key_erp[2]
                    ,key_erp[3]
                    ,key_erp[4]
                    ,'standard_province'
                    ,'standard_city'
                    ,'standard_district'
                    ) \
            .toPandas()
    df_erp = df.rename(columns=erp_select)
    df_erp.rename(columns=erp_rname,inplace=True)
    erp_addr = erp_select['workshop_address']
    df_erp = process_addr_sfe(df_erp,erp_addr)  # 清洗
    erp_col = list(df_erp.columns)
    for col_name in erp_col:
        df_erp[col_name] = df_erp[col_name].astype(str)
    df_erp = sparkConn.createDataFrame(df_erp)




    # Extra
    key_extra = list(extra_select.keys())
    key = ','.join(extra_select.keys())
    df2 = sparkConn.sql(f'select {key} from {database}.{extra_table} where ds = ${bdp.system.bizdate} and active_status=1')
    df2 = df2.withColumn("extra_data_source",lit(extra_source))
    df_extra =(df2.join(dim,
                    (df2[key_extra[4]]  == dim['reference_id']) &
                    (df2[key_extra[3]]  == dim['address'])
                    ,'left_outer')) \
            .select( key_extra[0]
                    ,key_extra[1]
                    ,key_extra[2]
                    ,key_extra[3]
                    ,key_extra[4]
                    ,'standard_province'
                    ,'standard_city'
                    ,'standard_district'
                    ,'extra_data_source') \
            .toPandas()
    df_extra.rename(columns=extra_rname,inplace=True)
    df_extra = df_extra.rename(columns=extra_select)
    extra_addr = extra_select['detail_address']
    df_extra = process_addr_sfe(df_extra,extra_addr)  # 清洗
    extra_col = list(df_extra.columns)
    for col_name in extra_col:
        df_extra[col_name] = df_extra[col_name].astype(str)
    print(extra_col)
    df_extra = sparkConn.createDataFrame(df_extra)
    # df_erxtra.where(col('extra_workshop_province') = '')

    """todo2: 合并erp与extra数据，算匹配度"""

    df_merge = df_extra.join(df_erp,(df_extra.extra_workshop_province ==  df_erp.erp_workshop_province) &
                                    (df_extra.extra_workshop_city == df_erp.erp_workshop_city) &
                                    (df_extra.extra_workshop_district ==  df_erp.erp_workshop_district) &
                                    (df_extra.extra_workshop_address.rlike('[\u4e00-\u9fa5]+')) &
                                    (df_erp.erp_workshop_address.rlike('[\u4e00-\u9fa5]+'))
                                     ,"right_outer")

    print('=============================')
    df_merge = df_merge.fillna('')   
    # 对两个列进行相似度计算，写入第三列
    similarity_udf = udf(similarity)
    df_merge = df_merge.withColumn("match_rate", similarity_udf(col("extra_workshop_name"), col("erp_workshop_name")))
    df_col = list(df_merge.columns)
    df_col.remove('match_rate')
    window_spec = Window.partitionBy(df_col).orderBy(desc("match_rate"))
    df_merge = df_merge.withColumn('rn',row_number().over(window_spec))
    df_merge = df_merge.where(col("rn") == 1).drop('rn')

    
    # 判断手机号两列是否相等，并将结果写入第三列
    df_merge = df_merge.withColumn("same_contact_mobile", when(col("extra_workshop_contact_mobile") == col("erp_workshop_contact_mobile") , "y").otherwise("n"))

    # 判断是否存在于extra external
    df_col = list(df_merge.columns)
    """step 1: 给实体表中的数据打上标记"""
    df_merge = (df_merge.join(entity,(df_merge.erp_workshop_client_code == entity.external_entity) &
                                          (df_merge.erp_data_source == entity.external_app) &
                                          (df_merge.extra_workshop_internal_id == entity.client_id) 
                                          ,'left_outer')) \
                                          .select(df_col+['external_entity']).withColumnRenamed("external_entity", "is_external_entity") \
                                          .withColumn('is_external_entity',when(col('is_external_entity').isNotNull(), "y").otherwise("n")) 

    """step 2: 给实体表中存在的erp数据打上标记"""
    df_merge = (df_merge.join(entity,(df_merge.erp_workshop_client_code == entity.external_entity) &
                                          (df_merge.erp_data_source == entity.external_app) 
                                          ,'left_outer')) \
                                          .select(df_col+['is_external_entity','external_entity']) \
                                          .withColumn('external_entity',when(col('external_entity').isNotNull(), "y").otherwise("n")) 

    # 写入时间列
    current_time = datetime.datetime.now()
    formatted_time = current_time.strftime("%Y-%m-%d %H:%M:%S")
    df_merge = df_merge.withColumn("etl_load_time",lit(formatted_time))


    df_col = list(df_merge.columns)
    null_col = ['null' if x in extra_col or x=='match_rate' else x for x in df_col]
    null_str = ','.join(null_col)
    df_merge.createOrReplaceTempView('df_rn')
    sql2 = """  
            select
                *
            from {0}
            where ((match_rate >= 0.9 and external_entity = 'n') or (match_rate < 0.2 and is_external_entity = 'y') or (same_contact_mobile = 'y' and extra_workshop_contact_mobile != '')) and erp_workshop_name != '' 
            union all
            select
              {1}
            from {0}
            where match_rate < 0.9 and erp_workshop_name != '' and same_contact_mobile = 'n' and external_entity = 'n'
    """.format('df_rn',null_str)
    result = sparkConn.sql(sql2)
    sparkConn.catalog.dropTempView('df_rn')
    result = result.drop("external_entity")
    result =result.dropDuplicates().withColumn("pday",lit(${bdp.system.bizdate}))
    sparkConn.sql(f'alter table {database}.{target_table} drop if exists partition(pday=${bdp.system.bizdate})')
    sparkConn.sql("SET hive.exec.dynamic.partition.mode=nonstrict")
    result.show()
    result.write \
            .mode('overwrite') \
            .insertInto(f"{database}.{target_table}")
