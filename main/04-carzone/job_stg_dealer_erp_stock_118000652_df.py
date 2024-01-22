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

# @resource_reference{"/STOCK/kangzong23.csv"}
# @resource_reference{"/STOCK/kangzong23.csv"}

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
        spark = SparkSession \
            .builder \
            .config(conf=SparkConf().setAppName("stock_repair_data") 
            .set("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs","false") 
            .set("spark.driver.memory", "8g")
            .set("spark.executor.memory", "8g")
            .set("spark.executor.instances", "4")
            .set("spark.executor.cores", '6')) \
            .enableHiveSupport() \
            .getOrCreate()
        sc = spark.sparkContext
        hc = HiveContext(sc)
        hc.setConf("hive.metastore.warehouse.dir", "/boschfs/warehouse/azure_blob/")
        return spark

    def get_blob_service(self):
            """下载连接"""
            return BlockBlobService(account_name='proddataplatcn3blob01',
                                    account_key='ScGueSagWl9s5XDCJeE6xOD8CupGFi5Jp0m9ZVi1Ri812p2GtXD5AXQ/zsVFIcUrNRE2zrIZlWLCjORJZyZHbQ==',
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
            model.get_blob_service().get_blob_to_path(uplt, blob, localFileName)    # download 
            downloadPath = sys.path[0] + "/" + localFileName

            last_index = blob.rfind('.')    # file type
            blob_end = blob[last_index + 1:]
            # print(blob_end)
            if blob_end == 'json':
                table = self.json_hand(downloadPath)
                try:
                    total,table = self.json_hand(downloadPath)
                    table["customercode"] = '118000652'
                    table.rename(columns=str.lower, inplace=True)
                    df_list.append(table)
                    total_set.add(total)
                except Exception as e:
                    print(f"=========文件损坏：{downloadPath}==========")
                    continue
            else:
                print('======经销商上传文件格式错误=======')
                return '4'

        
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
            if  blob_name == 'meta.json':
                continue
            need_blobs.append(i)

        return need_blobs

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



if __name__ == '__main__':
    # TODO 0: set
    formatted_endtime = datetime.now()
    formatted_endtime -= timedelta(days=1)
    year = formatted_endtime.strftime("%Y")
    month = str(int(formatted_endtime.strftime("%m")))
    day = str(int(formatted_endtime.strftime("%d")))

    data_list = ['sellout', 'stock']
    blobContainName = 'bosch-dw-integration-layer/'
    database = 'boschpro'
    path = 'Dealer_ERP/carzone/'+data_list[1]+'/'+year +'/'+ month +'/'+ day
    table = 'stg_dealer_erp_stock_118000652_df'

    # TODO 1：连接
    container = get_blob_clib(blobContainName)
    model = Azure_blob(path, container)
    sparkConn = model.get_spark_connection()
    sparkConn.sparkContext.setLogLevel("Error")

    # TODO 2:读取blob
    blobs = model.get_blobs(path)
    need_blob = model.data_need(blobs)
    df,schema = model.get_DF(need_blob)

    # TODO 3: 插入hive
    model.insert_target_table(df, sparkConn, database, table, schema)

    


