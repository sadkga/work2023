#!/usr/bin/env python
# coding=utf-8
"""
 -- @ Creater      : sadkga sadkga@88.com
 -- @ Since        : 2023-12-05 14:20:37
 -- @ LastAuthor   : sadkga sadkga@88.com
 -- @ LastTime     : 2023-12-09 23:54:01
 -- @ Location     : \\code\\main\\03-CDP_push\\push.py
 -- @ Message      : dafda dsafdsfadfsaf
 -- @ Copyright (c) 2023 by sadkga@88.com, All Rights Reserved. 
 """

from pyspark import SparkConf
from pyspark.sql import SparkSession
import pandas as pd
from azure.storage.blob import ContainerClient
import hashlib


class TOBLOB():
    def __init__(self,blob_name,table_name):
        """ 
         * @ message : dsfadfadf afafadfad
         * @ param2   [type] self: dsafdadad
         * @ param2   [string] blob_name: sadfadfas
         * @ param2   [blob] table_name: dsafdsaf
         * @ return   [type]
        """
        self.blob_name = blob_name
        self.table_name = table_name

    def get_blob_clib(self,blob_name,f):
        """获取blob客户端,上传数据"""
        connection_string = "https://stintermediatestgn3.blob.core.chinacloudapi.cn/bosch-aa"
        sas_token = 'sp=racwdl&st=2023-11-22T16:00:00Z&se=2024-11-23T16:00:00Z&spr=https&sv=2022-11-02&sr=c&sig=uNmWUE7MTJA0Z2F2AYa7j4DuEd4CO1BylFtzveeepxU%3D'
        container = ContainerClient.from_container_url(f'{connection_string}?{sas_token}')
    
        container.upload_blob(name=blob_name, data=f,overwrite=True)

    

if __name__ == '__main__':
    

    # assignment
    blob_name = f'activateCustomerListSync/20231204/test.csv'
    table_name = 'ads_battery_o2o_region'
    md5_name =f'activateCustomerListSync/20231204/activateCustomerListSyncByActiveTime_20231204.md5'

    # get_mode
    Blob = TOBLOB(blob_name,table_name)
    # to_csv
    loacl_path = '../../datas/CDP/sample_data.csv'
    d = pd.read_csv(loacl_path,encoding='ISO-8859-1',dtype=str)
    f = d.to_csv(header=True,index=False,sep='|',encoding='utf8')
    print(d['vehicle_model_id'])
    md5_hash = hashlib.md5(f.encode()).hexdigest()

    # # # upload
    # Blob.get_blob_clib(blob_name,f)
    # Blob.get_blob_clib(md5_name,md5_hash)

    

# https://proddataplatcn3blob01.blob.core.chinacloudapi.cn/ads-wks-bss-parts-sales