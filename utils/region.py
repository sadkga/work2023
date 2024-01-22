# coding=utf-8
"""
 -- @ Creater      : sadkga sadkga@88.com
 -- @ Since        : 2024-01-22 15:55:26
 -- @ LastAuthor   : sadkga sadkga@88.com
 -- @ LastTime     : 2024-01-22 15:55:26
 -- @ Location     : \\code\\utils\\region.py
 -- @ Message      : 
 -- @ Copyright (c) 2024 by sadkga@88.com, All Rights Reserved. 
 """
import requests
from bs4 import BeautifulSoup
import time
from datetime import datetime
import pyspark.sql.types as T
import pandas as pd
 
""" 
从国家统计局爬取省市区数据
"""
 
 
class Administrative(object):
    def main(self):

        # 年份
        current_date = datetime.now()
        year = current_date.year
        base_url = 'http://www.stats.gov.cn/sj/tjbz/tjyqhdmhcxhfdm/%s/' % year
        trs = self.get_response(base_url, 'provincetr')

        result = []
        for tr in trs:  # 循环每一行
            for td in tr:  # 循环每个省
                if td.a is None:
                    continue
                href_url = td.a.get('href')
                province_name = td.a.get_text()
                province_code = str(href_url.split(".")[0]) + "0000000000";
                province_url = base_url + href_url

                # print(province_code)
                # print(province_name)
                # print(province_url)
 
                # 插入省份数据并获取主键
                province_data = {'area_code' : province_code,'area_name' : province_name,'parent_code' : '0','area_level' : '1'}
                result.append(province_data)

                trs = self.get_response(province_url, None)
                for tr in trs[1:]:  # 循环每个市

                    city_code = tr.find_all('td')[0].string
                    city_name = tr.find_all('td')[1].string
 
                    # 插入城市数据并获取主键
                    city_data = {'area_code' : city_code,'area_name' : city_name,'parent_code' : province_code,'area_level' : '2'}
                    result.append(city_data)
                    
                    if tr.find_all('td')[1].a.get('href') is not None:
                        city_url = base_url + tr.find_all('td')[1].a.get('href')
                        trs = self.get_response(city_url, None)
                        for tr in trs[1:]:  # 循环每个区县
                            county_code = tr.find_all('td')[0].string
                            county_name = tr.find_all('td')[1].string
    
                            # 插入区县数据并获取主键
                            county_data = {'area_code' : county_code,'area_name' : county_name,'parent_code' : city_code,'area_level' : '3'}
                            result.append(county_data)
 
                    # time.sleep(1)
                time.sleep(1)
            time.sleep(1)

        df = pd.DataFrame(result)
        return df
 
    @staticmethod
    def get_response(url, attr):
        response = requests.get(url)
        response.encoding = 'utf8'  # 编码转换
        soup = BeautifulSoup(response.text, features="html.parser")
        table = soup.find_all('tbody')[1].tbody.tbody.table

        if attr:
            trs = table.find_all('tr', attrs={'class': attr})
        else:
            trs = table.find_all('tr')
        return trs

 
if __name__ == '__main__':
    df = Administrative().main()
    display(df)
