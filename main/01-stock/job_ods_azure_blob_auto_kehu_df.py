"""
--********************************************************************--
--所属主题: 库存域
--功能描述: 经销商库存&预测数据，Auto ETL
--创建者: 王兆翔
--创建日期:2023-05-12
--********************************************************************--
"""
import pandas as pd
import pyarrow.parquet as pq
from azure.storage.blob import ContainerClient
import json
import os
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, HiveContext
from datetime import datetime,timedelta


def get_blob_clib(blobContainName):
    """ 
     * @ function message: 
     * @ param2 [type] blobContainName: 
     * @ return [type]
    """
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
            "spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")).enableHiveSupport().getOrCreate()
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

        # 获取当前时间
        now = datetime.now()

        # 格式化输出年月日
        now_date = now.strftime("%Y%m%d")

        da = {a.name.split('/')[2] for a in blobs}
        da2 = []
        print(da)
        for i in da:
            try:
                b = int(i)
                if i != now_date:
                    da2.append(b)
                else:
                    continue
            except Exception as e:
                b = 0
                da2.append(b)
        date = max(da2)
        if date == 0 or da == {}:
            print("==========经销商数据未上传============")
            return 2
        else:
            return date

    def get_file(self, num, kehu, date, blobContainName):
        """读取合并指定日期下所有json数据 或者读取 单个parquet文件"""
        blob_name = list(self.container.list_blobs(
            name_starts_with=(self.data_list[num] + '/' + kehu + '/' + str(date) + '/')))
        print(self.data_list[num] + '/' + kehu + '/' + str(date) + '/')

        # 获取blob
        blobs_json = []
        blobs_parquet = []
        for b in blob_name:
            b = b.name
            # 判断是否空
            if b == '':
                print('=========经销商未上传数据=============')
                return '3','3'

            last_index = b.rfind('/')
            # 父级目录信息
            uplt = blobContainName + b[:last_index]
            print(uplt)
            # blob名字
            blob = b[last_index + 1:]
            print(blob)

            # 创建本地文件路径
            blobDirName = os.path.dirname(blob)
            newBlobDirName = os.path.join(uplt, blobDirName)
            if not os.path.exists(newBlobDirName):
                os.makedirs(newBlobDirName)
            localFileName = os.path.join(uplt, blob)

            # 下载
            model.get_blob_service().get_blob_to_path(uplt, blob, localFileName)
            downloadPath = sys.path[0] + "/" + localFileName

            # 后缀名
            last_index = blob.rfind('.')
            # blob名字
            blob_end = blob[last_index + 1:]
            # print(blob_end)
            if blob_end == 'json':
                try:
                    # 打开JSON文件并解析数据
                    with open(downloadPath, encoding='utf8') as f:
                        data = f.read()

                        # # 处理反斜杠
                        data = data.replace('\\', '\\\\')

                        json_load = json.loads(data)
                        for i in json_load:
                            blobs_json.append(i)
                    # 将解析后的数据合并到merged_data中关闭
                    f.close()
                except Exception as e:
                    print(f"============文件损坏：{downloadPath}=============")
                    continue

            elif blob_end == 'parquet':
                try:
                    table = pd.read_parquet(downloadPath)    
                    blobs_parquet.append(downloadPath)
                except Exception as e:
                    print(f"============文件损坏：{downloadPath}=============")
                    continue

            else:
                print('======经销商上传文件格式错误=======')
                return '4','4'

        # 列表转json
        json_str = json.dumps(blobs_json)

        # 单个地址返回
        if blobs_parquet != []:
            for i in blobs_parquet:
                return '', i

        elif blobs_json != [] :
            return json_str,''
        else:
            return '',''


    def insert_target_table(self, df, database, data, kehu, tmp_table, schema):
        """建表与插入"""

        # 删表
        
        sql1 = """
        DROP TABLE IF EXISTS {0}.ods_azure_blob_auto_{1}_{2}_df
        """.format(database, data, kehu)


        # 建表
        sql2= """
         create table if not exists {0}.ods_azure_blob_auto_{1}_{2}_df (
            {3}
        )
        partitioned by (ds string) stored as parquet 
        location "boschfs://boschfs/warehouse/{0}.ods_azure_blob_auto_{1}_{2}_df"
        """.format(database, data, kehu, schema)

      

        # sparkConn.sql(sql1)
        sparkConn.sql(sql2)


        # 测试上传列与hive表已有列是否匹配
        test = """
        select * from {0}.ods_azure_blob_auto_{1}_{2}_df limit 1
        """.format(database, data, kehu, schema)

        t=sparkConn.sql(test)
        
        # 兼容hive大写字段名
        df_col = [x.lower() for x in list(df.columns)]
        t_col = [x.lower() for x in list(t.columns)]


        for col in df_col:
            if col not in t_col and col != 'ds':
                sql3=""" 
                ALTER TABLE {0}.ods_azure_blob_auto_{1}_{2}_df add columns ({3} STRING comment '')
                """.format(database, data, kehu,col)
                sparkConn.sql(sql3)
                
        
        for col in t_col:
            if col not in df_col and col != 'ds':
                df[col] = 'NULL'
        print(df.columns)        
        print(t.columns)        
        
        t=sparkConn.sql(test)
        schema1 = ','.join(t.columns).replace(',ds', '').lower()


        # 插入数据
        df = sparkConn.createDataFrame(df)
        df.show()
        df.createOrReplaceTempView(tmp_table)

        

        sql4 = """
        INSERT OVERWRITE TABLE {0}.ods_azure_blob_auto_{1}_{2}_df PARTITION (ds=${bdp.system.bizdate})
        select {3} from {4}
        """.format(database, data, kehu,schema1,tmp_table)


        sparkConn.sql(sql4)
        sparkConn.catalog.dropTempView(tmp_table)


    def finish_etl(self, num, kehu_logo):
        """根据给的一级目录名，读取完成对应的json&parquet数据文件导入"""
        for kehu_code in kehu_logo:
            date_kehu = model.get_new_date(num, kehu_code)
            # 临时表
            tmp_table = data_list1[num] + '_' + kehu_code + '_df_tmp'

            # 读取合并处理过的json和parquet下载地址列表
            json_str, parquet_str = model.get_file(num, kehu_code, date_kehu, blobContainName)
            print(parquet_str)

            # 如果有危机
            if json_str == '3' or json_str == '4':
                continue

            elif json_str != '':

                # 将JSON字符串转换为DataFrame
                df_json = pd.read_json(json_str)


                # # df.columns = col
                df_json = df_json.fillna('')

                # change col_type
                df_json.rename(columns=str.lower, inplace=True)
                col = list(df_json.columns)
                print(col)
                for col_name in col:
                    df_json[col_name] = df_json[col_name].astype(str)
    
                keys = list(df_json.keys())
                # print(keys)
                schema = ' string,'.join(keys)+' string'
                     
                

                model.insert_target_table(df_json, database, data_list1[num], kehu_code, tmp_table, schema)
                # df.stop()

            elif parquet_str != '':
                # 读取Parquet文件的架构
                schema = pq.read_schema(parquet_str)
                # print(schema)

                # 获取列名
                col = list(schema.names)
                schema = ' string,'.join(col)+' string'

                df = pd.read_parquet(parquet_str)
              

                df.columns = col
                for col_name in col:
                    df[col_name] = df[col_name].astype(str)
                # print(df)
            


                model.insert_target_table(df, database, data_list1[num], kehu_code, tmp_table, schema)
            else:
                print('========未读取到json和parquet文件，请检查=========')


if __name__ == '__main__':
    data_list1 = ['Dealer_forecast', 'Dealer_stock']
    blobContainName = 'bosch-data-warehouse/'
    database = 'boschpro'

    # 读取列表参数
    p1 = 0
    b2 = 1

    # todo 0：连接blob
    container = get_blob_clib(blobContainName)
    model = Azure_blob(data_list1, container)

    # 创建sparksession
    sparkConn = model.get_spark_connection()
    sparkConn.sparkContext.setLogLevel("Error")

    # 　todo 2:指定经销商数据blob名读取
    kehu_predict = model.get_blobs(p1)
    kehu_business = model.get_blobs(b2)
    


    # todo 3: 获取经销商代号列表
    kehu_predict_code = model.get_kehu_code(kehu_predict)
    kehu_business_code = model.get_kehu_code(kehu_business)

    # print(kehu_predict_code)

    # todo 4:进行etl
    predict = model.finish_etl(p1,kehu_predict_code)
    business = model.finish_etl(b2,kehu_business_code)

