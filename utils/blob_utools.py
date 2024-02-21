#!/usr/bin/env python
# coding=utf-8
"""
 -- @ Creater      : sadkga sadkga@88.com
 -- @ Since        : 2023-12-10 00:06:53
 -- @ LastAuthor   : sadkga sadkga@88.com
 -- @ LastTime     : 2023-12-10 22:29:02
 -- @ Location     : \\work2023\\utils\\blob_utools.py
 -- @ Message      : Blob相关处理工具
 -- @ Copyright (c) 2023 by sadkga@88.com, All Rights Reserved. 
 """
from numpy import dtype
import xlrd
from azure.storage.blob import ContainerClient
import os
import sys
import io
import json
import pandas as pd
import pyarrow.parquet as pq
from datetime import datetime, timedelta


class BlobTools:
    """
        给定container对象，以应用blob工具
    """
    def __init__(self, blobContainName):
        self.blobContainName = blobContainName
        self.container = self.get_blob_clib(blobContainName)

    @staticmethod
    def get_blob_clib(blobContainName):
        connection_string = "DefaultEndpointsProtocol=https;AccountName=proddataplatcn3blob01;AccountKey=ScGueSagWl9s5XDCJeE6xOD8CupGFi5Jp0m9ZVi1Ri812p2GtXD5AXQ/       zsVFIcUrNRE2zrIZlWLCjORJZyZHbQ==;EndpointSuffix=core.chinacloudapi.cn"

        container = ContainerClient.from_connection_string(
            conn_str=connection_string,
            container_name=blobContainName)
        return container

    def get_blobs(self, path):
        """获取指定目录下的blob

        Args:
            path (str): blob目录地址

        Returns:
            list: 该目录下所有blobs
        """
        blobs = list(self.container.list_blobs(name_starts_with=(path + '/')))
        blobs_list = []
        for i in blobs:
            i = i.name
            blobs_list.append(i)
        return blobs_list

    def download_blob(self, b):
        """ 
         * @ message : download blob 
         * @ param2   [type] self: 
         * @ param2   [str] b: 下载的blob
         * @ return   [str] 下载地址
        """
        last_index = b.rfind('/')
        uplt = self.blobContainName + '/' + b[:last_index]
        blob = b[last_index + 1:]
        blobDirName = os.path.dirname(blob)
        newBlobDirName = os.path.join(uplt, blobDirName)
        if not os.path.exists(newBlobDirName):
            os.makedirs(newBlobDirName)
        localFileName = os.path.join(uplt, blob)
        downloadPath = sys.path[0].replace('\\', '/') + "/" + localFileName
        blob_client = self.container.get_blob_client(b)
        with open(downloadPath, 'wb') as local_file:
            download = blob_client.download_blob()
            local_file.write(download.readall())
        return downloadPath

    def get_path(self, blob_name):
        """ 
         * @ message : 下载blob并返回本地地址
         * @ param2   [type] self: 
         * @ param2   [list] blob_name: blob名字列表
         * @ return   [type] pd:DataFrame sql: schema
        """
        blobs_json = []
        blobs_parquet = []
        for blob in blob_name:
            downloadPath = self.download_blob(blob)
            last_index = blob.rfind('.')
            blob_end = blob[last_index + 1:]  # 后缀名
            if blob_end == 'json':
                try:
                    with open(downloadPath, encoding='utf8') as f:
                        data = f.read()
                        data = data.replace('\\', '\\\\')
                        json_load = json.loads(data)
                        for i in json_load:
                            blobs_json.append(i)
                except Exception as e:
                    print(f"============文件损坏：{downloadPath}=============")
                    continue

            elif blob_end == 'parquet':  # 处理parquet格式blob
                try:
                    table = pd.read_parquet(downloadPath)
                    blobs_parquet.append(downloadPath)
                except Exception as e:
                    print(f"============文件损坏：{downloadPath}=============")
                    continue

        json_str = json.dumps(blobs_json)  # 合并json数据
        if blobs_parquet != []:
            for parquet_path in blobs_parquet:  # only one
                schema = pq.read_schema(parquet_path)
                col = list(schema.names)
                print(col)
                schema = ' string,'.join(col)+' string'
                df = pd.read_parquet(parquet_path, dtype=str)
                df = df.fillna('')
                df.columns = col
                df.rename(columns=str.lower, inplace=True)
                return df, schema

        elif blobs_json != []:
            df_json = pd.read_json(json_str)
            df_json = df_json.fillna('')
            col = list(df_json.columns)
            print(col)
            schema = ' string,'.join(map(str, col))+' string'
            for col_name in col:
                df_json[col_name] = df_json[col_name].astype(str)
            df_json.rename(columns=str.lower, inplace=True)
            return df_json, schema
        else:
            print('=======没有读取到文件=======')

    def read_excel(self, path):
        """read excel and count rows

        Args:
            path (str): local file path

        Returns:
            int: excel rows num
        """
        workbook = xlrd.open_workbook(path)
        worksheet = workbook.sheet_by_index(0)
        total_rows = worksheet.nrows
        return total_rows

    def get_month(self, blobs, num):
        """read blobs list and dir_index,provide corresponding information

        Args:
            blobs (list): blob name list
            num (int): dir index

        Returns:
            list: Slice list
        """
        kehu = [a.split('/')[num] for a in blobs]
        if not kehu:
            return 1
        else:
            return kehu

    def insert_target_table(self, df, sparkConn, database, table, schema):
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

        # submit
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
        # insert overwrte table sql
        tmp_table = table + '_tmp'
        sql4 = """
        INSERT overwrite TABLE {0}.{1} PARTITION (ds=${bdp.system.bizdate})
        select {2} from {3}
        """.format(database, table, schema1, tmp_table)
        df = sparkConn.createDataFrame(df)
        df.show()
        df.createOrReplaceTempView(tmp_table)
        sparkConn.sql(sql4)
        sparkConn.catalog.dropTempView(tmp_table)
