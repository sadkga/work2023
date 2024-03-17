# coding=utf-8
"""
 -- @ Creater      : sadkga sadkga@88.com
 -- @ Since        : 2024-03-16 11:42:33
 -- @ LastAuthor   : sadkga sadkga@88.com
 -- @ LastTime     : 2024-03-17 21:09:40
 -- @ Location     : \\work2023\\pytest\\kehu_test2.py
 -- @ Message      : 
 -- @ Copyright (c) 2024 by sadkga@88.com, All Rights Reserved. 
 """
import pandas as pd
import json
from utils.blob_utools import BlobTools
import sys
import os
print(sys.path)
# sys.path.append("D:\material\Bosch_code\work2023")


# todo 1: set
pd.set_option('display.max_columns', None)
blobContainName = 'bosch-data-warehouse'
path = 'Dealer_stock/118000478/20240316'

moder = BlobTools(blobContainName)
blobs = moder.get_blobs(path)
print(blobs)

# todo 2: down file
blobs_json = []
for blob in blobs:
    downLoadPath = moder.download_blob(blob)
    # 打开JSON文件并解析数据
    with open(downLoadPath, encoding='utf8') as f:
        data = f.read()
        # # 处理反斜杠
        data = data.replace('\\', '\\\\')
        json_load = json.loads(data)
        for i in json_load:
            blobs_json.append(i)
    # 将解析后的数据合并到merged_data中关闭
    f.close()

# todo 3: check
# 列表转json
lowercase_json_list = [
    {key.lower(): value for key, value in item.items()} for item in blobs_json]
json_str = json.dumps(lowercase_json_list)
print(pd.read_json(json_str))
