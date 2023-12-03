import datetime
import calendar
import time

import pandas as pd
# import pyarrow.parquet as pq
# from azure.storage.blob.blockblobservice import BlockBlobService
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
        spark = SparkSession \
            .builder \
            .config(conf=SparkConf().setAppName("pyspark—to-sqlserver") 
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

    def get_blobs(self, num):
        """获取指定容器一级目录下的blobs名称"""
        blobs = list(self.container.list_blobs(name_starts_with=(self.data_list[num] + '/')))
        blobs_list = []
        for i in blobs:
            i = i.name
            blobs_list.append(i)
        return blobs_list

    def get_kehu_code(self, blobs):
        """获取经销商代号"""
        kehu = {a.split('/')[1] for a in blobs}
        if kehu == {}:
            print("==========无经销商============")
            return 1
        else:
            return kehu

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

    def get_DF(self, blob_name,kehu, blobContainName):
        """读取合并指定经销商下本月和上月月初、月末的数据变为DF"""

        # 获取blob
        
        blobs_json = []
        df_list = []

        for b in blob_name:
            # 判断是否空
            if b == '':
                print('=========经销商未上传数据=============')
                return '3'

            last_index = b.rfind('/')
            # 父级目录信息
            uplt = blobContainName + b[:last_index]
            print(uplt)
            # blob名字
            blob = b[last_index + 1:]
            kehu_code = b.split('/')[1]

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
            # blob名字
            blob_end = blob[last_index + 1:]
            # print(blob_end)
            if blob_end == 'json':
                try:
                    table = pd.read_json(downloadPath)
                    table["kehu"] = str(kehu)
                    table.rename(columns=str.lower, inplace=True)
                    df_list.append(table)
                except Exception as e:
                    print(f"=========文件损坏：{downloadPath}==========")
                    continue

            # if blob_end == 'parquet':
            elif blob_end == 'parquet':
                try:
                    table = pd.read_parquet(downloadPath)
                    table["kehu"] = str(kehu)
                    table.rename(columns=str.lower, inplace=True)
                    df_list.append(table)
                except Exception as e:
                    print(f"=========文件损坏：{downloadPath}==========")
                    continue



            else:
                print('======经销商上传文件格式错误=======')
                return '4'

        
        merge_df = pd.concat(df_list)
            

        df_merge = merge_df.fillna('')
        return df_merge


    def data_need(self,kehu_code):
        """获取指定经销商的本月blobs和上月月初、月末blobs"""
        blobs = list(container.list_blobs(name_starts_with=('Dealer_stock/' + kehu_code + '/')))



        blobs_month = []

        for i in blobs:
            da = i.name
            # 过滤模板
            try:
                b = int(da.split('/')[2])
            except Exception as e:
                continue

            if int(da.split('/')[2]) > 20230609: #and int(da.split('/')[2]) <= 20230609 :
                blobs_month.append(da)
            else:
                continue

            blobs_month.append(da)

        return blobs_month



    def insert_target_table(self, df, database, data, tmp_table, schema):


        # 删表
        sql1 = """
        DROP TABLE IF EXISTS {0}.dwd_azure_blob_auto_{1}_df
        """.format(database, data)

        sql = """
        ALTER TABLE {0}.dwd_azure_blob_auto_{1}_df DROP IF EXISTS PARTITION (ds=${bdp.system.bizdate})
        """.format(database, data)


        # 建表
        sql2= """
         create table if not exists {0}.dwd_azure_blob_auto_{1}_df (
            {2}
        )
        partitioned by (ds string) stored as parquet 
        location "boschfs://boschfs/warehouse/{0}.dwd_azure_blob_auto_{1}_df"
        """.format(database, data,schema)

        global a
        if a == 0:
            sparkConn.sql(sql1)
            a = 1
        
        
        sparkConn.sql(sql2)


        # 测试上传列与hive表已有列是否匹配
        test = """
        select * from {0}.dwd_azure_blob_auto_{1}_df limit 1
        """.format(database, data)

        t=sparkConn.sql(test)
        
        for i in list(df.columns):
            if i not in list(t.columns) and i != 'ds' and i != 'id':
                print(i)
                sql3=""" 
                ALTER TABLE {0}.dwd_azure_blob_auto_{1}_df add columns ({2} STRING comment '')
                """.format(database, data,i)
                sparkConn.sql(sql3)
                

            

        
        for col in list(t.columns):
            if col not in list(df.columns) and col != 'ds' and col != 'id':
                df[col] = 'NULL'



        t=sparkConn.sql(test)

        print(df.columns)        
        print(t.columns) 

        schema1 = ','.join(t.columns).replace(',ds', '')   
                    







        df = sparkConn.createDataFrame(df)
        df.createOrReplaceTempView(tmp_table)


        sql4 = """
        INSERT into TABLE {0}.dwd_azure_blob_auto_{1}_df PARTITION (ds=${bdp.system.bizdate})
        select {2} from {3}
        """.format(database, data,schema1,tmp_table)

        sql5 = """
        alter table {0}.dwd_azure_blob_auto_{1}_df drop partition(ds < ${yyyyMMdd, -7d});
        """.format(database, data)


        sparkConn.sql(sql4)
        # sparkConn.sql(sql5)
        sparkConn.catalog.dropTempView("tmp_table")

    def finish_etl(self,num, kehu_logo):
        """根据给的一级目录名，读取完成对应的json&parquet数据文件导入"""
        # 临时表
        tmp_table = data_list1[num] + '_df_tmp'
        df_list = []

        for kehu_code in kehu_logo:
            # 读取合并处理过的json和parquet下载地址列表
            blobs_month = model.data_need(kehu_code)
            merge_df = model.get_DF(blobs_month, kehu_code,blobContainName)
            if type(merge_df) != str:
                df_list.append(merge_df)
        # 整合所有经销商数据
        fin_merge = pd.concat(df_list)
        print(fin_merge)
        
        
        col = list(fin_merge.columns)
        print(col)
        for col_name in col:
            fin_merge[col_name] = fin_merge[col_name].astype(str)
           
        schema = ' string,'.join(col) + ' string'
        
        model.insert_target_table(fin_merge, database, data_list1[num], tmp_table, schema)
        time.sleep(2)

if __name__ == '__main__':
    data_list1 = ['Dealer_forecast', 'Dealer_stock']
    blobContainName = 'bosch-data-warehouse/'
    database = 'boschpro'

    # 读取列表参数
    b2 = 1

    # todo 0：连接blob
    container = get_blob_clib(blobContainName)
    model = Azure_blob(data_list1, container)

    # 创建sparksession
    # sparkConn = model.get_spark_connection()
    # sparkConn.sparkContext.setLogLevel("Error")

    # 　todo 2:指定经销商数据blob名读取
    kehu_business = model.get_blobs(b2)

    # print(kehu_business)

    # todo 3: 获取经销商代号列表
    kehu_business_code = model.get_kehu_code(kehu_business)
    print(kehu_business_code)
    # kehu_business_code = ['118000583','118000623','118000664','118000712','118000843']
    # end_list = ['118000131','118005361','118000021','118000411','118002298'
    # 
    # '118000478','118000524'
    # ]
    # for i in kehu_business_code:
    #     if i in end_list:
    #         kehu_business_code.remove(i)
    # print(kehu_business_code)
    # 为0则第一次运行删除当前分区
    # a = 1

    # # 切分供应商，解决内存溢出
    # chunk_size = 1
    # result = [kehu_business_code[i:i+chunk_size] for i in range(0, len(kehu_business_code), chunk_size)] 
    # jishu = [] 
    # for kehu_code in result:
        
    #     # todo 4：读取每月信息
    #     try:
    #         data_need_month = model.finish_etl(1, kehu_code)
    #     except Exception as e:
    #         jishu.append(kehu_code)
    #         print(e)
    #         continue

    # print(f"报错{len(jishu)}个：{jishu}")