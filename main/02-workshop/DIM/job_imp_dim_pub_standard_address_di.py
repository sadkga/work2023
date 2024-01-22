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
    # spark
    sparkConn = get_spark_connection()
    sparkConn.sparkContext.setLogLevel("Error")
    sparkConn.sql('set spark.sql.hive.convertMetastoreParquet=false')               # 序列化parquet
    sparkConn.conf.set('spark.sql.session.timeZone','Asia/Shanghai')                # 时区
    sparkConn.sql('set mapreduce.input.fileinputformat.input.dir.recursive=true')   # 文件递归读取

    # defind
    database = 'boschpro'
    target_table = 'dim_pub_standard_address_di'
    erp_table = 'dwd_del_dealer_erp_workshop_master_df'
    extra_table = 'dim_wks_extra_workshop_master_data'
    extra_source = 'extra'
    erp_select = {
                   'workshop_address'   :  'address',
                   'erp_workshopid'     :  'reference_id',
                   'from_app'           :  'data_source'
                   }
                   
    extra_select = {
                    'detail_address': 'address',
                    'client_id'     : 'reference_id'
                    }

    """todo0: 获取每日增量 """
    # ERP
    key_list = list(erp_select.keys())
    key1 = ','.join(key_list)
    df = sparkConn.sql(f'select {key1} from {database}.{erp_table} where pday = ${bdp.system.bizdate}')
    df2 = sparkConn.sql(f'select {key1} from {database}.{erp_table} where pday = ${yyyyMMdd, -2d}')
    
    erp_di = df.join(df2,
                        (df[key_list[0]]  ==  df2[key_list[0]]) &  
                        (df[key_list[1]]  ==  df2[key_list[1]]) &
                        (df[key_list[2]]  ==  df2[key_list[2]])   
                        ,"left_anti")
                        # .where(df2[key_list[0]].isNull()).select(df["*"]))

    # Extra
    key_list2 = list(extra_select.keys())
    key2 = ','.join(key_list2)
    df3 = sparkConn.sql(f'select {key2} from {database}.{extra_table} where ds = ${bdp.system.bizdate}')
    df4 = sparkConn.sql(f'select {key2} from {database}.{extra_table} where ds = ${yyyyMMdd, -2d}')
    extra_di = df3.join(df4,
                        (df3[key_list2[0]]  ==  df4[key_list2[0]]) &  
                        (df3[key_list2[1]]  ==  df4[key_list2[1]])  
                        ,"left_anti")
                        # .where(df4[key_list[0]].isNull()).select(df3["*"]))
     
     
    """todo1: 检查数据分区 """
    if df.count() == 0 or df2.count() == 0 or df3.count() == 0 or df4.count() == 0:
        print('=========数据分区异常，请检查数据后补数据==========')
    elif erp_di.count() == 0 and extra_di.count() == 0:
        print('=========数据没有变化，请确认===============')
    else:
        print('==============分区检查完毕=================')


    """todo2: 调用高德api"""
    # ERP
    erp_di = erp_di.toPandas()
    erp_addr = erp_select['workshop_address']
    df = erp_di.rename(columns=erp_select)
    df,addr = process_addr_sfe(df,erp_addr)             # 清洗
    df_erp = formatted_addr(addr,df)      # 地区认证
    erp_col = list(df_erp.columns)
    for col_name in erp_col:
        df_erp[col_name] = df_erp[col_name].astype(str)

    
    # Extra
    extra_di = extra_di.toPandas()
    df2 = extra_di.rename(columns=extra_select)
    df2['data_source'] = extra_source
    extra_addr = extra_select['detail_address']
    df2,addr2 = process_addr_sfe(df2,extra_addr)             # 清洗
    df_extra = formatted_addr(addr2,df2)      # 地区认证
    extra_list = list(df_extra.columns)
    for col_name in extra_list:
        df_extra[col_name] = df_extra[col_name].astype(str)

    
    """todo3: 插入增量表"""
    df_merge = pd.concat([df_extra, df_erp], ignore_index=True) 
    # time
    current_time = datetime.datetime.now()
    formatted_time = current_time.strftime("%Y-%m-%d %H:%M:%S")
    df_merge['etl_load_time'] = formatted_time

    df = sparkConn.createDataFrame(df_merge)
    df.createOrReplaceTempView('df_rn')

    # 添加递增标识符列及字段排序
    sql = """
       select
        row_number() over(order by address) address_num        
        ,reference_id      
        ,data_source       
        ,address           
        ,standard_province 
        ,standard_city     
        ,standard_district 
        ,etl_load_time      
       from {}
    """.format('df_rn')
    result = sparkConn.sql(sql)
    result =result.dropDuplicates()
    result.write \
            .mode('overwrite') \
            .insertInto(f"{database}.{target_table}",True)
    # result.show(result.count(),truncate=False)