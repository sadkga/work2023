import pandas as pd
from azure.storage.blob.blockblobservice import BlockBlobService
import os
import sys
from datetime import datetime
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, HiveContext


class doctor_azure():

    def __init__(self,blobContainName,blob,database,table,tmp_table):
        self.blobContainName = blobContainName
        self.blob = blob
        self.database = database
        self.table = table
        self.tmp_table = tmp_table

    def get_spark_connection(self):
        conf = SparkConf()\
                .setAppName("pyspark—to-hive")\
                .set("spark.sql.catalogImplementation",'hive')\
                .set("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs","false")
  
        spark = SparkSession(SparkContext('local', 'azure', conf=conf))
        return spark


    def get_blob_service(self):
        return BlockBlobService(account_name='proddataplatcn3blob01', account_key='ScGueSagWl9s5XDCJeE6xOD8CupGFi5Jp0m9ZVi1Ri812p2GtXD5AXQ/zsVFIcUrNRE2zrIZlWLCjORJZyZHbQ==', endpoint_suffix='core.chinacloudapi.cn')

    def insert_target_table(self,df):
        df.createOrReplaceTempView(tmp_table) 
        sql = """
        INSERT OVERWRITE TABLE {0}.{1} PARTITION (ds=${bdp.system.bizdate})
        SELECT
            seq,
            regexp_str,
            case 
                when replace_str is null then ''
                else replace_str
            end as replace_str
        FROM {2}
        """.format(database,table,tmp_table)
        sparkConn.sql(sql)

    def downloadBlobFiles(self,blob):
        blobDirName =  os.path.dirname(blob)
        newBlobDirName = os.path.join(blobContainName, blobDirName)
        if not os.path.exists(newBlobDirName):
            os.makedirs(newBlobDirName)
        localFileName = os.path.join(blobContainName, blob)
        doctor.get_blob_service().get_blob_to_path(blobContainName, blob, localFileName)
        return localFileName

if __name__ == '__main__':
     # 获取当前任务开始时间
    formatted_starttime = datetime.now()
    start_time = formatted_starttime.strftime( "%Y-%m-%d %H:%M:%S" )
    print('开始时间'+ ":" + start_time)

    blobContainName = 'bosch-dw-integration-layer/manual/AA_SCN/Dealer_ERP_PN_regular_expression'
    blob = 'regexp_rule.csv'
    database = 'boschpro'
    table = 'ods_azure_blob_regular_expression_df'
    tmp_table = table + '_tmp_${bdp.system.bizdate}'
    
    doctor = doctor_azure(blobContainName,blob,database,table,tmp_table)
    localFileName = doctor.downloadBlobFiles(blob)
    downloadPath = sys.path[0] + "/" + localFileName
    sparkConn = doctor.get_spark_connection()
    df = sparkConn.read.format("com.databricks.spark.csv").option("delimiter", ",").option("header", "true").load('file://'+downloadPath)

    sparkConn.sparkContext.setLogLevel("Info")
    doctor.insert_target_table(df)
    print("程序执行成功结束！！！")