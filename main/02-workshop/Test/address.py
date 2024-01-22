"""
--********************************************************************--
--所属主题: 库存域
--功能描述: extra与erp的workshop数据
--创建者: 王兆翔
--创建日期:2023-05-10
--********************************************************************--
"""
# -*- coding: utf-8 -*-
import requests
import json
import datetime
import pandas as pd
import difflib
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, HiveContext, Window
from pyspark.sql.functions import lit,row_number
from pyspark.sql.types import *

def get_spark_connection():
    """创建spark对象"""
    spark = SparkSession.builder.config(conf=SparkConf()
            .setAppName("dwd_workshop_api_di")
            .set("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
            .set("spark.driver.memory", "8")
            .set("spark.executor.memory", "6g")
            .set("spark.executor.instances", "3")
            .set("spark.executor.cores", '8')
            .set('spark.sql.session.timeZone','Asia/Shanghai')) \
            .enableHiveSupport() \
            .getOrCreate()
    sc = spark.sparkContext
    hc = HiveContext(sc)
    hc.setConf("hive.metastore.warehouse.dir", "/boschfs/warehouse/azure_blob/")
    return spark


def process_addr_sfe(df,addr):
    """去除特殊字符"""
    print(df.shape)
    df[addr] = df[addr] \
            .replace("`", "", regex=True) \
            .replace("#", "", regex=True) \
            .replace("/n",'', regex=True) \
            .replace("-",'', regex=True) \
            .replace("0",'', regex=True)

    addr_sfe = list(df[addr])
    return df, addr_sfe

def formatted_addr(addr, df):
    all_data = []
    if len(addr) > 0:
        for i in range(len(addr)):
            addr_key = '1f206129ec240f1cc12be917b9615c0b'
            _url = 'https://restapi.amap.com/v3/geocode/geo?address={}&output=JSON&key={}'.format(addr[i], addr_key)
            respon = requests.get(_url)
            response = json.loads(respon.content)
            status = response['status']
            info = response['info']
            if status == '1' and info == 'OK':
                count = response['count']
                if count == '1':
                    data = response["geocodes"][0]
                    formatted_address = data["formatted_address"]
                    country = data["country"]
                    province = data["province"]
                    city = data["city"]
                    district = data["district"]
                    location = data["location"]
                    addr_data = str(formatted_address) + "&" + str(country) + "&" + str(province) + "&" + str(
                        city) + "&" + str(district) + "&" + str(location)
                    all_data.append(addr_data)
                elif count == '0':
                    all_data.append('' + "&" + '' + "&" + '' + "&" + '' + "&" + '' + "&" + '0.0,0.0')
            elif status == '0' and info != 'OK':
                all_data.append('' + "&" + '' + "&" + '' + "&" + '' + "&" + '' + "&" + '0.0,0.0')

        all1 = pd.DataFrame(all_data, columns=['name'])

        r0 = all1['name'].str.split('&', expand=True)[0]
        r1 = all1['name'].str.split('&', expand=True)[1]
        r2 = all1['name'].str.split('&', expand=True)[2]
        r3 = all1['name'].str.split('&', expand=True)[3]
        r4 = all1['name'].str.split('&', expand=True)[4]
        r5 = all1['name'].str.split('&', expand=True)[5]

        r22 = r5.str.split(',', expand=True)
        # df['lon'] = r22[0]
        # df['lat'] = r22[1]

        # df[f'{source}_workshop_address'] = r0
        # df[f'{source}_workshop_country'] = r1
        df[f'standard_province'] = r2
        df[f'standard_city'] = r3
        df[f'standard_district'] = r4
    else:
        return df
    return df

# 自定义函数，根据条件创建新列
def condition(row):
    if row['extra_workshop_contact_mobile'] == row['erp_workshop_contact_mobile']:
        return 'y'
    else:
        return 'n'

if __name__ == '__main__':
    df = pd.DataFrame([])
    addr = ['台北市中正區','彰化縣大村鄉']
    formatted_addr(addr, df)
    print(df)

    sys.exit()

