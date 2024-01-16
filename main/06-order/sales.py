#!/usr/bin/env python
# coding=utf-8
"""
 -- @ Creater      : sadkga sadkga@88.com
 -- @ Since        : 2023-12-25 17:01:18
 -- @ LastAuthor   : sadkga sadkga@88.com
 -- @ LastTime     : 2023-12-25 18:39:21
 -- @ Location     : \\code\\main\\06-order\\sales.py
 -- @ Message      : 
 -- @ Copyright (c) 2023 by sadkga@88.com, All Rights Reserved. 
 """
# import sqlite3
# from pyspark import SparkConf, SparkContext
# from pyspark.sql import SparkSession, HiveContext
# import pyspark.sql.functions as F
# import pyspark.sql.types as T
import pandas as pd


# def get_spark_connection():
#     spark = SparkSession \
#         .builder \
#         .config(conf=SparkConf().setAppName("pyspark—to-sqlserver")  # type: ignore
#                 .set("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
#                 .set("spark.driver.memory", "8g")
#                 .set("spark.executor.memory", "8g")
#                 .set("spark.executor.instances", "4")
#                 .set("spark.executor.cores", '6')) \
#         .enableHiveSupport() \
#         .getOrCreate()

#     return spark


# # 创建sparksession
# spark = get_spark_connection()
# spark.sparkContext.setLogLevel("Error")

path1 = 'C:/Users/WZH8SGH/Desktop/下载/stg_pcd_v_aamm_gen_material_df.parquet'

df_mart = pd.read_parquet(path1)
df = df_mart.query("select * from df_mart")
print(df)
