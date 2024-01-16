#!/usr/bin/env python
# coding=utf-8
"""
 -- @ Creater      : sadkga sadkga@88.com
 -- @ Since        : 2023-12-08 09:35:39
 -- @ LastAuthor   : sadkga sadkga@88.com
 -- @ LastTime     : 2024-01-11 17:05:23
 -- @ Location     : \\code\\utils\\test.py
 -- @ Message      : 
 -- @ Copyright (c) 2024 by sadkga@88.com, All Rights Reserved. 
 """
import json
import requests
import hashlib
import time
import pandas as pd
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, HiveContext
from datetime import datetime, timedelta


def md5_handle(app_secret, nonce, timestamp):
    sign_str = f"key1=value1&key2=&key3=value3{app_secret}{nonce}{timestamp}"
    # 创建md5对象
    md5 = hashlib.md5()
    # 将字符串传入md5对象中
    md5.update(sign_str.encode('utf-8'))
    # # 获取编码后的结果
    sign = md5.hexdigest().upper()
    return sign


if __name__ == '__main__':

    formatted_endtime = datetime.now()
    formatted_endtime += timedelta(hours=8)
    etl_load_time = formatted_endtime.strftime("%Y-%m-%d %H:%M:%S")
    # 请求地址
    url = "https://dataplat.boschaftermarket.com.cn:2443/SimbaData/getadsuncoveredOElistmf"

    # 请求头
    headers = {'Content-type': 'application/x-www-form-urlencoded;charset=utf-8'}

    # 输入app_secret、app_key
    app_secret = '90a45a42261b56880a39c45c38b75db5'
    Timestamp = etl_load_time
    Nonce = '772fa07de4254e6dad6f9ae9beb692f5'
    appId = 'MTU6MTcwNDk0NjA3ODc3Mw=='
    appKey = 'MTU6MTcwNDk0NjA3ODc3Mw=='
    groupId = '919458739955101697'
    serviceId = '919458739928707073'
    serviceVersion = '2.0.0'
    requestId = 'b4680d5b42de4aceac0c311cc59e5b7f'
    encryptMethod = 'MD5'
    Sign = md5_handle(app_secret, Nonce, Timestamp)

    # 签名认证
    headers = {
        'Sign': Sign, 'Timestamp': etl_load_time, 'Nonce': Nonce, 'appId': appId, 'appKey': appKey, 'groupId': groupId, 'serviceId': serviceId, 'serviceVersion': serviceVersion, 'requestId': requestId, 'encryptMethod': encryptMethod
    }

    # 发送请求
    response = requests.get(url, headers=headers, params={
                            'pageSize': 999, 'pageNum': 1})
    print(headers)

    # print(response.text)
    if response.status_code == 200:
        json_cangzong = response.json()

        # 列表转json
        json_str = json.dumps(json_cangzong['info'])
        pd_json = pd.read_json(json_str)
        print(pd_json)

        # 处理获取到的数据
    else:
        print("请求失败：", response.status_code)
