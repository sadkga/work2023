# coding=utf-8
"""
 -- @ Creater      : sadkga sadkga@88.com
 -- @ Since        : 2024-01-22 16:17:09
 -- @ LastAuthor   : sadkga sadkga@88.com
 -- @ LastTime     : 2024-01-22 16:17:09
 -- @ Location     : \\code\\main\\11-workshop\\Test\\pull_workshop_address_distance.py
 -- @ Message      : 
 -- @ Copyright (c) 2024 by sadkga@88.com, All Rights Reserved. 
 """
from datetime import datetime,timedelta 
import calendar
import time
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, HiveContext
import pandas as pd
import pyarrow.parquet as pq
from azure.storage.blob import ContainerClient
from azure.storage.blob.blockblobservice import BlockBlobService
import json
import os
import sys
import numpy as np

# @resource_reference{"/STOCK/kangzong23.csv"}
# @resource_reference{"/STOCK/kangzong23.csv"}

def get_blob_clib(blobContainName):
    """获取blob客户端以及所需目录下的blob名称"""
    connection_string = "https://dlsaaddpnorth3001.blob.core.chinacloudapi.cn/test-data"
    sas_token = 'sp=racwdlm&st=2023-10-23T06:40:51Z&se=2023-12-31T14:40:51Z&spr=https&sv=2022-11-02&sr=c&sig=%2B7oZvaaZMrPa%2FV7G8DZKKUdHdzpqtR9J79bsD4KT7pY%3D'
    container = ContainerClient.from_container_url(f'{connection_string}?{sas_token}')
    return container



class Azure_blob():
    """数据模型化"""

    def __init__(self, data_list, container):
        self.data_list = data_list
        self.container = container
        # self.blobContainName =blobContainName

    def get_spark_connection(self):
        spark = SparkSession \
            .builder \
            .config(conf=SparkConf().setAppName("kangzong_data") 
            .set("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs","false") 
            .set("spark.driver.memory", "9g")
            .set("spark.executor.memory", "9g")
            .set("spark.executor.instances", "4")
            .set("spark.executor.cores", '8')
            .set('spark.default.parallelism','60'))\
            .enableHiveSupport() \
            .getOrCreate()
        sc = spark.sparkContext
        hc = HiveContext(sc)
        hc.setConf("hive.metastore.warehouse.dir", "/boschfs/warehouse/azure_blob/")
        return spark

    def get_blob_service(self):
            """下载连接"""
            return BlockBlobService(account_name='dlsaaddpnorth3001',
                                    sas_token= 'https://dlsaaddpnorth3001.blob.core.chinacloudapi.cn/test-data?sp=racwdlm&st=2023-10-23T06:40:51Z&se=2023-12-31T14:40:51Z&spr=https&sv=2022-11-02&sr=c&sig=%2B7oZvaaZMrPa%2FV7G8DZKKUdHdzpqtR9J79bsD4KT7pY%3D',
                                    endpoint_suffix='core.chinacloudapi.cn')
    def get_blobs(self, path):
        """获取指定容器一级目录下的blobs名称"""
        blobs = list(self.container.list_blobs(name_starts_with=(path + '/')))
        blobs_list = []
        for i in blobs:
            i = i.name
            blobs_list.append(i)
        return blobs_list



    def json_hand(self,json_data):
        with open(json_data,'r') as f:
            json_data = json.load(f)['info']
        total = json_data['total']
        data_list = json_data['list']
        df = pd.DataFrame(data_list)
        return total,df 

    def get_DF(self, blob_name):
        """读取合并指定经销商下本月和上月月初、月末的数据变为DF"""
        # 判断是否空
        if blob_name == []:
            print('=========经销商未上传数据=============')
            return '3'
        
        total_set = set()
        df_list = []

        for b in blob_name:
            last_index = b.rfind('/')
            uplt = blobContainName + b[:last_index]
            blob = b[last_index + 1:]
            # print(uplt)

            blobDirName = os.path.dirname(blob) # loacl downloadPath
            newBlobDirName = os.path.join(uplt, blobDirName)
            if not os.path.exists(newBlobDirName):
                os.makedirs(newBlobDirName)
            localFileName = os.path.join(uplt, blob)
            blob_client = container.get_blob_client(b)
            with open(localFileName, 'wb') as local_file:
                download = blob_client.download_blob()
                local_file.write(download.readall())    # download 
            downloadPath = sys.path[0] + "/" + localFileName

            last_index = blob.rfind('.')    # file type
            blob_end = blob[last_index + 1:]
            # print(blob_end)
            if blob_end == 'csv':
                table = pd.read_csv(downloadPath)
                df_list.append(table)
      

        
        merge_df = pd.concat(df_list)
        df_merge = merge_df.fillna('')
        col = list(df_merge.columns)
        print(col)
        schema = ' string,'.join(map(str, col))+' string'
        for col_name in col:
            df_merge[col_name] = df_merge[col_name].astype(str)
        df_merge.rename(columns=str.lower, inplace=True)

        print('接入数据：',len(df_merge))
        df_merge = df_merge.drop_duplicates()
        print('去重后数据：',len(df_merge))
        print('正确数据量：',total_set)    
        return df_merge,schema

    def data_need(self,blobs):
        """获取指定经销商的本月blobs和上月月初、月末blobs"""

        need_blobs = []
        for i in blobs:
            blob_name = i.split('/')[-1]
            if  blob_name == 'address_distance_valuable.csv':
                need_blobs.append(i)

        return need_blobs

    def insert_target_table(self, df, sparkConn, database, table, schema,date):
        """ 
         * @ message : 插入hive
         * @ param2   [type] self: 
         * @ param2   [type] df: pands DataFrame
         * @ param2   [type] sparkConn: spark实例
         * @ param2   [type] database: 数据库名
         * @ param2   [type] table: 表名
         * @ param2   [type] schema: 建表sql schema
         * @ return   [type] None
        """
        # drop table sql
        sql1 = """
        DROP TABLE IF EXISTS {0}.{1}
        """.format(database, table)

        # create table sql
        sql2 = """
         create table if not exists {0}.{1} (
            {2}
        )
        partitioned by (ds string) stored as parquet 
        location "boschfs://boschfs/warehouse/{0}.{1}"
        """.format(database, table, schema)

        # get hive table column sql
        test = """
        select * from {0}.{1} limit 1
        """.format(database, table, schema)

        # excuter
        # sparkConn.sql(sql1) # drop table
        sparkConn.sql(sql2)
        t = sparkConn.sql(test)
        df_col = [x.lower() for x in list(df.columns)]  # lower columns str
        t_col = [x.lower() for x in list(t.columns)]
        for col in df_col:  # alignment hive table and current dataframe
            if col not in t_col and col != 'ds':
                # add hive column sql
                sql3 = """ 
                       ALTER TABLE {0}.{1} add columns ({2} STRING comment '')
                 """.format(database, table, col)
                sparkConn.sql(sql3)
        for col in t_col:
            if col not in df_col and col != 'ds':
                df[col] = 'NULL'
        print(df.columns)
        print(t.columns)

        t = sparkConn.sql(test)
        schema1 = ','.join(t.columns).replace(',ds', '').lower()
         # insert overwrite table sql
        tmp_table = table + '_tmp'
        sql4 = """
        INSERT into TABLE {0}.{1} PARTITION (ds={4})
        select {2} from {3}
        """.format(database, table, schema1, tmp_table,date)
        df = sparkConn.createDataFrame(df)
        df.show()
        df.createOrReplaceTempView(tmp_table)
        sparkConn.sql(sql4)
        sparkConn.catalog.dropTempView(tmp_table)



if __name__ == '__main__':
    # TODO 0: set
    formatted_endtime = datetime.now()
    formatted_endtime -= timedelta(days=1)
    year = formatted_endtime.strftime("%Y")
    # month = str(int(formatted_endtime.strftime("%m")))
    # day = str(int(formatted_endtime.strftime("%d")))
    month = '12'
    day = 18

    data_list = ['sellout', 'stock']
    blobContainName = 'test-data/'
    database = 'boschpro'
    table = 'tmp_workshop_address_distance'
    print(f'=====当前读取日期为：{month}月{day}日========')
    path = 'test/WZX/03-workshop'

    # TODO 1：连接
    container = get_blob_clib(blobContainName)
    model = Azure_blob(path, container)
    sparkConn = model.get_spark_connection()
    sparkConn.sparkContext.setLogLevel("Error")

    # TODO 2:读取blob
    blobs = model.get_blobs(path)
    need_blob = model.data_need(blobs)
    print(need_blob)
    df,schema = model.get_DF(need_blob)
    
    # 切分
    num_splits = 10
    df_list = np.array_split(df,num_splits)


    # TODO 3: 插入hive
    ds = year+month+str(day).zfill(2)
    print(f'===========插入分区：{ds}=============')
    for i, df in enumerate(df_list):
        print(f'========第{i+1}个 DataFrame')
        model.insert_target_table(df, sparkConn, database, table, schema,ds)

    


