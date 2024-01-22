import datetime
import calendar
import time

import pandas as pd
import pyarrow.parquet as pq
from azure.storage.blob.blockblobservice import BlockBlobService
from azure.storage.blob import ContainerClient
import json
import os
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, HiveContext


def get_blob_clib(blobContainName):
    """获取blob客户端以及所需目录下的blob名称"""
    connection_string = "DefaultEndpointsProtocol=https;AccountName=proddataplatcn3blob01;AccountKey=ScGueSagWl9s5XDCJeE6xOD8CupGFi5Jp0m9ZVi1Ri812p2GtXD5AXQ/zsVFIcUrNRE2zrIZlWLCjORJZyZHbQ==;EndpointSuffix=core.chinacloudapi.cn"

    container = ContainerClient.from_connection_string(
        conn_str=connection_string,
        container_name=blobContainName)
    return container


class Azure_blob():
    """数据模型化"""

    def __init__(self, data_list, container):
        self.data_list = data_list
        self.container = container
        # self.blobContainName =blobContainName

    def get_spark_connection(self):
        spark = SparkSession.builder.config(conf=SparkConf().setAppName("pyspark—to-hive").set(
            "spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false").set("spark.driver.memory", "4g").set("spark.executor.memory", "4g")).enableHiveSupport().getOrCreate()
        sc = spark.sparkContext
        hc = HiveContext(sc)
        hc.setConf("hive.metastore.warehouse.dir", "/boschfs/warehouse/azure_blob/")
        return spark

    def get_blob_service(self):
        """下载连接"""
        return BlockBlobService(account_name='proddataplatcn3blob01',
                                account_key='ScGueSagWl9s5XDCJeE6xOD8CupGFi5Jp0m9ZVi1Ri812p2GtXD5AXQ/zsVFIcUrNRE2zrIZlWLCjORJZyZHbQ==',
                                endpoint_suffix='core.chinacloudapi.cn')

    def get_blobs(self, num):
        """获取指定容器一级目录下的csv——blobs"""
        blobs = list(self.container.list_blobs(name_starts_with=(self.data_list[num] + '/')))
        blobs_list = []
        for i in blobs:
            i = i.name
            if i[-3:] == 'csv':
                blobs_list.append(i)

        return blobs_list




    def get_DF(self, b,blobContainName):
        """读取blob数据"""

        # 获取blob
        
        blobs_json = []
        df_list = []

        
        # 判断是否空
        if b == '':
            print('=========经销商未上传数据=============')
            return '3'

        last_index = b.rfind('/')
        last_d = b.rfind('.')
        # 父级目录信息
        uplt = blobContainName + b[:last_index]
        print(uplt)
        # blob名字
        blob = b[last_index + 1:]
        tmp_kehu = b[last_index+1:last_d]
        kehu = tmp_kehu.split('_')[-1]

        # print(blob)

        # 创建本地文件路径
        blobDirName = os.path.dirname(blob)
        newBlobDirName = os.path.join(uplt, blobDirName)
        if not os.path.exists(newBlobDirName):
            os.makedirs(newBlobDirName)
        localFileName = os.path.join(uplt, blob)

        # 下载
        model.get_blob_service().get_blob_to_path(uplt, blob, localFileName)
        downloadPath = sys.path[0] + "/" + localFileName
        # print(downloadPath)

        # 后缀名
        last_index = blob.rfind('.')

        # blob_end
        blob_end = blob[last_index + 1:]
        # print(blob_end)
        if blob_end == 'json':
            try:
                table = pd.read_json(downloadPath)
                table.rename(columns=str.lower, inplace=True)
                df_list.append(table)
            except Exception as e:
                print(f"=========文件损坏：{downloadPath}==========")
                return '3',kehu

        # if blob_end == 'parquet':
        elif blob_end == 'parquet':
            try:
                table = pd.read_parquet(downloadPath)
                table.rename(columns=str.lower, inplace=True)
                df_list.append(table)
            except Exception as e:
                print(f"=========文件损坏：{downloadPath}==========")
                return '3',kehu
        elif blob_end == 'csv':
            try:
                table = pd.read_csv(downloadPath)
                table.rename(columns = str.lower, inplace = True)
                df_list.append(table)

            except Exception as e:
                print(f"=========文件损坏：{downloadPath}==========")
                return '3',kehu

        else:
            print('======经销商上传文件格式错误=======')
            return '4',kehu

        
        merge_df = pd.concat(df_list)
            

        df_merge = merge_df.fillna('')
        return df_merge,kehu



    def insert_target_table(self, df, database, data, tmp_table, schema):


        # 删表
        sql1 = """
        DROP TABLE IF EXISTS {0}.ods_dealer_product_code_mapping_{1}_df
        """.format(database, data)

        sql = """
        ALTER TABLE {0}.ods_dealer_product_code_mapping_{1}_df DROP IF EXISTS PARTITION (ds=${bdp.system.bizdate})
        """.format(database, data)


        # 建表
        sql2= """
         create table if not exists {0}.ods_dealer_product_code_mapping_{1}_df (
            {2}
        )
        partitioned by (ds string) stored as parquet 
        location "boschfs://boschfs/warehouse/{0}.ods_dealer_product_code_mapping_{1}_df"
        """.format(database, data,schema)

        sparkConn.sql(sql1)
        
        sparkConn.sql(sql2)


        # 测试上传列与hive表已有列是否匹配
        test = """
        select * from {0}.ods_dealer_product_code_mapping_{1}_df limit 1
        """.format(database, data)

        t=sparkConn.sql(test)
        
        for col in list(df.columns):
            if col not in list(t.columns) and col != 'ds' and col != 'id':
                print('1')
                sql3=""" 
                ALTER TABLE {0}ods_dealer_product_code_mapping_{1}_df add columns ({2} STRING comment '')
                """.format(database, data,col)
                sparkConn.sql(sql3)
                

            

        
        for col in list(t.columns):
            if col not in list(df.columns) and col != 'ds' and col != 'id':
                df[col] = 'NULL'
        print(df.columns)        
        print(t.columns) 

        t=sparkConn.sql(test)

        schema1 = ','.join(t.columns).replace(',ds', '')   
                    



        df = sparkConn.createDataFrame(df)
        df.createOrReplaceTempView(tmp_table)


        sql4 = """
        INSERT overwrite TABLE {0}.ods_dealer_product_code_mapping_{1}_df PARTITION (ds=${bdp.system.bizdate})
        select {2} from {3}
        """.format(database, data,schema1,tmp_table)

        sql5 = """
        alter table {0}.ods_dealer_product_code_mapping_{1}_df drop partition(ds < ${yyyyMMdd, -7d});
        """.format(database, data)


        sparkConn.sql(sql4)
        # sparkConn.sql(sql5)
        sparkConn.catalog.dropTempView("tmp_table")

    def finish_etl(self,blobs_list):
        """根据给的一级目录名，读取完成对应的json&parquet数据文件导入"""
        # 临时表
        
        df_list = []
        for blob in blobs_list:
            # 读取合并处理过的json和parquet下载地址列表
            fin_merge,kehu = model.get_DF(blob,blobContainName)
            if type(fin_merge) != str:
                
                print(fin_merge)

                col = list(fin_merge.columns)
                print(col)
                for col_name in col:
                    fin_merge[col_name] = fin_merge[col_name].astype(str)

                schema = ' string,'.join(col) + ' string'
                
                tmp_table = kehu + '_df_tmp'
                model.insert_target_table(fin_merge, database, kehu, tmp_table, schema)
                time.sleep(2)
            else:
                print(f"========{kehu}数据异常，请检查===========")

if __name__ == '__main__':
    data_list1 = ['manual/AA_SCN/Dealer_ERP_PN_mapping']
    blobContainName = 'bosch-dw-integration-layer/'
    database = 'boschpro'

    # 读取列表参数
    b2 = 0

    # todo 0：连接blob
    container = get_blob_clib(blobContainName)
    model = Azure_blob(data_list1, container)

    # 创建sparksession
    sparkConn = model.get_spark_connection()
    sparkConn.sparkContext.setLogLevel("Error")

    # 　todo 2:指定经销商数据blob名读取
    kehu_business = model.get_blobs(b2)
    # print(kehu_business)
    # 获取当中csv文件
    # for kehu_blob in kehu_business:
        
        # kehu = '118002298'
    model.finish_etl(kehu_business)
