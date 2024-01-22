from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, BinaryType
from azure.storage.blob import ContainerClient
import hashlib
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad


class TOBLOB():
    def __init__(self,blob_name,table_name):
        self.blob_name = blob_name
        self.table_name = table_name

    def get_blob_clib(self,blob_name,f):
        """获取blob客户端,上传数据"""
        connection_string = "https://stintermediatestgn3.blob.core.chinacloudapi.cn/bosch-aa"
        sas_token = 'sp=racwdl&st=2023-11-22T16:00:00Z&se=2024-11-23T16:00:00Z&spr=https&sv=2022-11-02&sr=c&sig=uNmWUE7MTJA0Z2F2AYa7j4DuEd4CO1BylFtzveeepxU%3D'
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
        sql = f"select * from boschpro.{self.table_name} where pday=${bdp.system.bizdate}"
        df = spark.sql(sql)
        df.show()

        def AES256(text):
            if text == '' or text is None:
                return text

            key = b'|I?=BNJtQpR1bc2"'   
            padded_data = pad(text.encode(), AES.block_size)     # 使用PKCS7填充
            cipher = AES.new(key, AES.MODE_ECB)
            encrypted_bytes = cipher.encrypt(padded_data)
            return encrypted_bytes

        AES_udf = udf(AES256, StringType())
        df = df.withColumn("phone9", AES_udf(df['phone9'])) \
                .withColumn('address9', AES_udf(df['address9']))
        df.show()
        f = df.toPandas().to_csv(sep='|',index=False,encoding='utf-8')
        return f
    

if __name__ == '__main__':

    # assignment
    table_name = 'dwd_car_driver_mini_program_order_df'
    blob_name = f'{table_name}/date=${yyyyMMdd}/{table_name}_${yyyyMMdd}.csv'
    md5_name =f'{table_name}/date=${yyyyMMdd}/{table_name}_${yyyyMMdd}.md5'

    # get_mode
    Blob = TOBLOB(blob_name,table_name)
    # spark
    spark = Blob.get_spark()
    # to_csv
    f = Blob.hive_data_to_csv(spark)
    md5_hash = hashlib.md5(f.encode()).hexdigest()

    # # upload
    Blob.get_blob_clib(blob_name,f)
    Blob.get_blob_clib(md5_name,md5_hash)



# https://proddataplatcn3blob01.blob.core.chinacloudapi.cn/ads-wks-bss-parts-sales