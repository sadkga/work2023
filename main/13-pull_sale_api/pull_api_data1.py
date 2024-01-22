# -*- coding:utf-8 -*-
import hashlib
import requests
import time
from datetime import date
import json
import pymysql
import decimal
from decimal import Decimal
import pandas as pd
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, HiveContext

# rise class
class ResponseError(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)

# api
class OAuth2Client:  
    def __init__(self, client_id, client_secret, redirect_uri, username, password):  
        self.client_id = client_id  
        self.client_secret = client_secret  
        self.redirect_uri = redirect_uri  
        self.username = username  
        self.password = password  

  
    def get_token(self):  
        payload = {  
            'grant_type': 'password',  
            'client_id': self.client_id,  
            'client_secret': self.client_secret,  
            'redirect_uri': self.redirect_uri,  
            'username': self.username,  
            'password': self.password  
        }  
        try:  
            response = requests.post(AUTH_API_URL, data=payload)  
            if response.status_code == 200:  
                print('token响应：',response.json())
                return response.json()['access_token']  
            else:  
                raise requests.HTTPError(f"OAuth2 token request failed with status {response.status_code}")  
        except requests.exceptions.HTTPError as e:  
            print(f"OAuth2 token request failed with error {str(e)}")  
            return None  
        except Exception as e:  
            print(f"Unexpected error occurred: {str(e)}")  
            return None  

    def send_request(self):    
        bearer_token = self.get_token()    
        if bearer_token is not None:   
            data = {
               'xoql' : """select
                                   customer__c.accountName accountName
                                 , customer__c.salesOrg__c SalesOrg
                                 , customer__c.distnibutionChannel__c DistributionChannel
                                 , dimDepart.departName Division,customer__c.soldToShipTo__c SoldtoParty
                                 , sales__c.userID__c SalesEmployeeCode
                                 , sales__c.name SalesEmployeeName
                                 , startDate__c from_date
                                 , endDate__c to_date
                                 , sales__c.personalEmail Email 
                            from salesAccMapping__c
                        """
            }
            headers = {  
                'Authorization': f'Bearer {bearer_token}',  
                'Content-Type':'application/x-www-form-urlencoded'  
            } 
            response = requests.post(API_URL, data = data, headers=headers)
            response1 = response.text.decode('utf-8') if isinstance(response.text, bytes) else response.text
            response2 = json.loads(response1)
            # # status_code = str(response2['code'])
            print('Data响应:', response2)
            if response2['code'] != str(200):
                 raise ResponseError("接口调用出错")
            return response2
        
    def handle_data(self):
        response2 = self.send_request()
        data = response2['data']
        print('TotalSize: ',data['totalSize'])
        print('Count: ',data['count'])
        df_json = pd.DataFrame(data['records'])
        df_json = df_json.fillna('')
        col = list(df_json.columns)
        print('COl:',col)
        schema = ' string,'.join(map(str, col))+' string'
        for col_name in col:
            df_json[col_name] = df_json[col_name].astype(str)
        df_json.rename(columns=str.lower, inplace=True)
        return df_json, schema
        

    def get_spark_connection(self):
        spark = SparkSession \
            .builder \
            .config(conf=SparkConf().setAppName("kangzong_data") 
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
    print('-------------Program Start-------------')
    # token parameter
    client_id = '2c3876f84b0af6e9254043291bbff111'
    client_secret = '77edfd88de07d8067a3dfd0e0de74dc2'
    redirect_uri = 'https://api-sandbox.xiaoshouyi.com'
    username = 'honghao.wei@cn.bosch.com'
    password = 'honghao.wei@cn.bosch.com4G5dvymy'
    AUTH_API_URL = "https://api-sandbox.xiaoshouyi.com/oauth2/token.action"

    # data parameter
    API_URL='https://api-sandbox.xiaoshouyi.com/rest/data/v2.0/query/xoql'
    RETRY_COUNT = 3
    RETRY_INTERVAL = 5

    # database
    database = 'boschpro'
    table = 'stg_aa_salesaccmapping_df'

    # example
    model =  OAuth2Client(client_id,client_secret,redirect_uri,username,password) 
    sparkConn = model.get_spark_connection()
    sparkConn.sparkContext.setLogLevel("Error")

    # logic
    df,schema = model.handle_data()
    model.insert_target_table(df ,sparkConn, database, table ,schema)
    print('-------------Program End-------------')
   

