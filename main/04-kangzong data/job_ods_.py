#!/usr/bin/env python
# coding=utf-8
"""
 -- @ Creater      : sadkga sadkga@88.com
 -- @ Since        : 2023-12-10 12:14:08
 -- @ LastAuthor   : sadkga sadkga@88.com
 -- @ LastTime     : 2023-12-10 15:53:01
 -- @ Location     : \\work2023\\main\\04-kangzong data\\job_ods_.py
 -- @ Message      : 
 -- @ Copyright (c) 2023 by sadkga@88.com, All Rights Reserved. 
 """

from msilib import schema
import string
from utils.blob_utools import blob_tools
from datetime import datetime, timedelta
import sys
import os


def data_need(blobs):
    '''
    message : 
    param2   [type] blobs: 
    return   [type]
    '''
    """ 
     * @ message : 获取需要的blob
     * @ param2   [type] self: 
     * @ param2   [type] blobs: blobs
     * @ return   [type]
    """
    need_blobs = []
    for i in blobs:
        blob_name = i.split('/')[-1]
        if blob_name == 'meta.json':
            continue
        need_blobs.append(i)
    return need_blobs


if __name__ == '__main__':
    # BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    # # __file__获取执行文件相对路径，整行为取上一级的上一级目录
    # sys.path.append(BASE_DIR)

    # todo 0: set
    formatted_endtime = datetime.now()
    formatted_endtime -= timedelta(days=1)
    year = formatted_endtime.strftime("%Y")
    month = formatted_endtime.strftime("%m")
    day = formatted_endtime.strftime("%d")

    data_list = ['sellout', 'stock']
    blobContainName = 'bosch-dw-integration-layer'
    database = 'boschpro'
    path = 'Dealer_ERP/carzone/'+data_list[0]+'/'+year + '/' + '11' + '/23'
    blob_name = 'test/kangzong1123_sellout_dup.csv'

    # todo 1：连接
    model = blob_tools(blobContainName)
    # sparkConn = model.get_spark_connection()
    # sparkConn.sparkContext.setLogLevel("Error")

    # todo 2:读取blob
    blobs = model.get_blobs(path)
    need_blob = data_need(blobs)
    df, schema = model.get_path(need_blob)  # type: ignore

    print(df)
    print(schema)
    # duplicate_rows = df.duplicated()
    # df['IsDuplicate'] = df.duplicated()
    # print(df)
