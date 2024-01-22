# coding=utf-8
"""
 -- @ Creater      : sadkga sadkga@88.com
 -- @ Since        : 2024-01-22 16:16:52
 -- @ LastAuthor   : sadkga sadkga@88.com
 -- @ LastTime     : 2024-01-22 16:16:52
 -- @ Location     : \\code\\main\\11-workshop\\Test\\workshop_address_push_gen2.py
 -- @ Message      : 
 -- @ Copyright (c) 2024 by sadkga@88.com, All Rights Reserved. 
 """
from pyspark import SparkConf
from pyspark.sql import SparkSession
from azure.storage.blob import ContainerClient
import hashlib


class TOBLOB():
    def __init__(self,blob_name,table_name):
        self.blob_name = blob_name
        self.table_name = table_name

    def get_blob_clib(self,blob_name,f):
        """获取blob客户端,上传数据"""
        connection_string = "https://dlsaaddpnorth3001.blob.core.chinacloudapi.cn/test-data"
        sas_token = 'sp=racwdlm&st=2023-10-23T06:40:51Z&se=2023-12-31T14:40:51Z&spr=https&sv=2022-11-02&sr=c&sig=%2B7oZvaaZMrPa%2FV7G8DZKKUdHdzpqtR9J79bsD4KT7pY%3D'
        container = ContainerClient.from_container_url(f'{connection_string}?{sas_token}')
    
        container.upload_blob(name=blob_name, data=f,overwrite=True)


    def get_spark(self):
        """获取spark对象"""
        spark = SparkSession.builder.config(
            conf=SparkConf()
            .setAppName("hive—to-blob")
            .set("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
            .set("spark.driver.memory", "4g")
            .set("spark.executor.memory", "4g")) \
            .enableHiveSupport() \
            .getOrCreate()

        return spark

    def hive_data_to_csv(self,spark):
        """hive数据转csv"""
        sql = f"""select client_name, detail_address from boschpro.{self.table_name}
                 where ds =${bdp.system.bizdate}
                 group by client_name, detail_address """
        df = spark.sql(sql)

        df.show()
        f = df.toPandas().to_csv(index=False)
        return f
    

if __name__ == '__main__':

    # assignment
    blob_name = f'test/WZX/03-workshop/workshop_address.csv'
    table_name = 'dim_wks_extra_workshop_master_data'

    # get_mode
    Blob = TOBLOB(blob_name,table_name)
    # spark
    spark = Blob.get_spark()
    # to_csv
    f = Blob.hive_data_to_csv(spark)

    # # upload
    Blob.get_blob_clib(blob_name,f)



# https://proddataplatcn3blob01.blob.core.chinacloudapi.cn/ads-wks-bss-parts-sales