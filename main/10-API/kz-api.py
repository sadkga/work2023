import json
import requests
import hashlib
import time
import pandas as pd
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, HiveContext



class API_DATA():
    """康众数据模型"""
    def __init__(self, app_key, app_secret,headers,url):
        self.app_key = app_key
        self.app_secret = app_secret
        self.headers = headers
        self.url = url

    def get_data_cangzong(self):
        """
        根据康众给定的参数，返回API的DataFrame数据
        :param app_key:
        :param app_secret:
        :param headers:
        :param url:
        :return:
        """

        # 获取时间戳
        timestamp = int(time.time() * 1000)

        # 签名认证
        data = {
         "app_key": app_key,
         "timestamp":timestamp,
         "carId":0
        }

        hader_list = []

        # 连接字符
        for x,y in data.items():
            hader_list.append(str(x)+str(y))

        # 对列表中的元素进行排序
        hader_list.sort(key=lambda x: x[0])

        # 列表转字符串
        hader_str = ''.join(hader_list)

        sign_str = app_secret+hader_str+app_secret

        # 创建md5对象
        md5 = hashlib.md5()

        # 将字符串传入md5对象中
        md5.update(sign_str.encode('utf-8'))

        # # 获取编码后的结果
        sign = md5.hexdigest().lower()

        data['sign'] = sign

        # 发送请求
        response = requests.get(url, headers=headers, params=data)
        print(data)

        # print(response.text)
        if response.status_code == 200:
            json_cangzong = response.json()

            # 列表转json
            json_str = json.dumps(json_cangzong['info'])
            pd_json = pd.read_json(json_str)
            print(pd_json)

            return pd_json

        # 处理获取到的数据
        else:
            print("请求失败：", response.status_code)


if __name__ == '__main__':
    # 请求地址
    url = "https://opensandbox.ncarzone.com/api/searchprovider/getTmCarModelTree"

    # 请求头
    headers = {'Content-type': 'application/x-www-form-urlencoded;charset=utf-8'}

    # 输入app_secret、app_key
    app_secret = '767213d676ec835add4b89a2dc55b7ce0d402090'
    app_key = '2022083012101'

    # todo 1.读取数据
    cangzong = API_DATA(app_key,app_secret,headers,url)
    df = cangzong.get_data_cangzong()