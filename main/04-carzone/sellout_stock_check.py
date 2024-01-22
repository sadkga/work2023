from datetime import datetime,timedelta 
import calendar
import time

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



    def get_new_date(self, num, kehu_code):
        """获取指定经销商最新日期和当前的一级目录名"""
        blobs = list(self.container.list_blobs(name_starts_with=(self.data_list[num] + '/' + kehu_code + '/')))

        da = {a.name.split('/')[2] for a in blobs}
        da2 = []
        print(da)
        for i in da:
            try:
                b = int(i)
                da2.append(b)
            except Exception as e:
                b = 0
                da2.append(b)
        date = max(da2)
        if date == 0 or da == {}:
            print("==========经销商数据未上传============")
            return 2
        else:
            return date


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
                    table["kehu"] = '11800xxxx'
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
        print('接入数据：',len(df_merge))
        # df_merge = df_merge.drop_duplicates()
        # print('去重后数据：',len(df_merge))
        print('正确数据量：',total_set)    
        return df_merge

    def data_need(self,blobs):
        """获取指定经销商的本月blobs和上月月初、月末blobs"""

        need_blobs = []
        for i in blobs:
            blob_name = i.split('/')[-1]
            if  blob_name == 'meta.json':
                continue
            need_blobs.append(i)

        return need_blobs

  


if __name__ == '__main__':
    # todo 0: set
    formatted_endtime = datetime.now()
    formatted_endtime -= timedelta(days=1)
    year = formatted_endtime.strftime("%Y")
    month = formatted_endtime.strftime("%m")
    day = formatted_endtime.strftime("%d")

    data_list = ['sellout', 'stock']
    blobContainName = 'bosch-dw-integration-layer/'
    database = 'boschpro'
    path = 'Dealer_ERP/carzone/'+data_list[0]+'/'+year +'/'+ month +'/23'
    blob_name = 'test/kangzong23_sellout_dup.csv'

    # todo 1：连接
    container = get_blob_clib(blobContainName)
    model = Azure_blob(path, container)
    # sparkConn = model.get_spark_connection()
    # sparkConn.sparkContext.setLogLevel("Error")

    # todo 2:读取blob
    blobs = model.get_blobs(path)
    need_blob = model.data_need(blobs)
    df = model.get_DF(need_blob)
    print(df)
    duplicate_rows = df.duplicated()
    df['IsDuplicate'] = df.duplicated()
    print(df)
    f = df.to_csv(header=True, index=False)

    container.upload_blob(name=blob_name, data=f,overwrite=True)
    


