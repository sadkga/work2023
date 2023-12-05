import requests
from bs4 import BeautifulSoup
import time
from datetime import datetime
import pyspark.sql.types as T
import pandas as pd
from IPython.display import display

# todo 1: 从国家统计局爬取省市区数据


class Administrative(object):
    def main(self):
        # 年份
        current_date = datetime.now()
        year = current_date.year
        base_url = 'http://www.stats.gov.cn/sj/tjbz/tjyqhdmhcxhfdm/%s/' % year
        trs = self.get_response(base_url, 'provincetr')
        direct_province = ['北京市', '天津市', '重庆市', '上海市']
        direct_district = ['省直辖县级行政区划', '自治区直辖县级行政区划']

        result = []  # 数据列表
        for tr in trs:  # 循环每一行
            for td in tr:  # 循环每个省
                if td.a is None:
                    continue
                href_url = td.a.get('href')
                province_name = td.a.get_text()
                province_code = str(href_url.split(".")[0]) + "0000000000"
                province_url = base_url + href_url

                if province_name in direct_province:
                    data = self.result_schema(
                        province_code, province_name, province_code, province_name, 1)
                    result.append(data)
                    continue

                trs = self.get_response(province_url, None)
                for tr in trs[1:]:  # 循环每个市
                    city_code = tr.find_all('td')[0].string
                    city_name = tr.find_all('td')[1].string

                    if (city_name in direct_district) and tr.find_all('td')[1].a.get('href') is not None:
                        city_url = base_url + \
                            tr.find_all('td')[1].a.get('href')
                        trs = self.get_response(city_url, None)
                        for tr in trs[1:]:  # 循环每个区县
                            county_code = tr.find_all('td')[0].string
                            county_name = tr.find_all('td')[1].string
                            data = self.result_schema(
                                county_code, county_name, province_code, province_name, 3)
                            result.append(data)
                        continue
                    data = self.result_schema(
                        city_code, city_name, province_code, province_name, 2)
                    result.append(data)
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

    @staticmethod
    def result_schema(code, name, parent_code, parent_name, level):
        data = {'city_code_external': code, 'city_name_local_language': name, 'city_name_English': '', 'province_code': parent_code, 'province_name_local_language': parent_name, 'province_name_English': ''                # ,'area_level' : level
                , 'ISO_country_code': 'CN', 'country_name_local_language': '中国', 'country_name_English': 'China'
                }
        return data


if __name__ == '__main__':
    df = Administrative().main()
    print(df)

    # todo 2: 读取航空国外数据
    # read_path = 'abfss://test-data@dlsaaddpnorth3001.dfs.core.chinacloudapi.cn/travel expense/DIM/  dim_aviation_city_code_mf.csv'
    # loacl path
    read_path = 'C:/Users/WZH8SGH/OneDrive - Bosch Group/code/main/Travel expense/datas/DIM/dim_aviation_city_code_mf.csv'
    df_aviation = pd.read_csv(read_path).query('country_region_name !="中国"')

    df_aviation['Province code'] = ''
    df_aviation['Province_name_local_language'] = ''
    df_aviation['Province_name_English'] = ''
    df_aviation['country_name_English'] = ''

    new_index = {'aviation_city_code':   'city_code_external', 'city_name_local_language':      'city_name_local_language', 'city_name_EN':   'city_name_English', 'Province code':    'province_code', 'Province_name_local_language':   'province_name_local_language',  'Province_name_English':   'province_name_English', 'country_region_code':   'ISO_country_code',  'country_region_name':   'country_name_local_language', 'country_name_English':      'country_name_English'
                 }
    df_aviation = df_aviation.rename(columns=new_index)
    group_col = list(new_index.values())
    group_col.remove('city_code_external')
    print(group_col)
    df_aviation = df_aviation.groupby(group_col)['city_code_external'].agg(lambda x: '/'.join(map(str,x))).reset_index()
    df_aviation['city_name_local_language'] = df_aviation['city_name_local_language'].str.strip() # 去除行首空格
    df_aviation = df_aviation[new_index.values()]
    

    # todo 3: 聚合
    df_result = pd.concat([df, df_aviation])
    display(df_aviation)
