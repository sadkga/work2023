# coding=utf-8
"""
 -- @ Creater      : sadkga sadkga@88.com
 -- @ Since        : 2024-01-22 15:23:17
 -- @ LastAuthor   : sadkga sadkga@88.com
 -- @ LastTime     : 2024-01-22 15:23:17
 -- @ Location     : \\code\\main\\05-travel expense\\synchronize_data.py
 -- @ Message      : 
 -- @ Copyright (c) 2024 by sadkga@88.com, All Rights Reserved. 
 """
# path
from notebookutils import mssparkutils
dws = 'abfss://data-warehouse-dws@dlsaaddpnorth3001.dfs.core.chinacloudapi.cn/dws_fi_te_travel_expense.csv'
dim_aviation = 'abfss://data-warehouse-dim@dlsaaddpnorth3001.dfs.core.chinacloudapi.cn/dim_aviation_city_code_mf.csv'
dim_city = 'abfss://data-warehouse-dim@dlsaaddpnorth3001.dfs.core.chinacloudapi.cn/dim_city_mf.csv'
dim_employee = 'abfss://data-warehouse-dim@dlsaaddpnorth3001.dfs.core.chinacloudapi.cn/dim_employee_mf.csv'
dim_expense_type = 'abfss://data-warehouse-dim@dlsaaddpnorth3001.dfs.core.chinacloudapi.cn/dim_fi_te_expense_type.csv'
dwd_air = 'abfss://data-warehouse-dwd@dlsaaddpnorth3001.dfs.core.chinacloudapi.cn/dwd_fi_te_air_ticket_cn.csv'
dwd_reimburer_exp_detail = 'abfss://data-warehouse-dwd@dlsaaddpnorth3001.dfs.core.chinacloudapi.cn/dwd_fi_te_reimburse_exp_detail.csv'
dwd_reimburer_header = 'abfss://data-warehouse-dwd@dlsaaddpnorth3001.dfs.core.chinacloudapi.cn/dwd_fi_te_reimburse_header.csv'
dwd_travel_app_header = 'abfss://data-warehouse-dwd@dlsaaddpnorth3001.dfs.core.chinacloudapi.cn/dwd_fi_te_travel_app_header_cn.csv'
dwd_travel_app_item = 'abfss://data-warehouse-dwd@dlsaaddpnorth3001.dfs.core.chinacloudapi.cn/dwd_fi_te_travel_app_item_cn.csv'


# target
target_dws = 'abfss://data-warehouse-ads@dlsaaddpnorth3001.dfs.core.chinacloudapi.cn/travel expense/dws_fi_te_travel_expense.csv'
target_dim_aviation = 'abfss://data-warehouse-ads@dlsaaddpnorth3001.dfs.core.chinacloudapi.cn/travel expense/dim_aviation_city_code_mf.csv'
target_dim_city = 'abfss://data-warehouse-ads@dlsaaddpnorth3001.dfs.core.chinacloudapi.cn/travel expense/dim_city_mf.csv'
target_dim_employee = 'abfss://data-warehouse-ads@dlsaaddpnorth3001.dfs.core.chinacloudapi.cn/travel expense/dim_employee_mf.csv'
target_dim_expense_type = 'abfss://data-warehouse-ads@dlsaaddpnorth3001.dfs.core.chinacloudapi.cn/travel expense/dim_fi_te_expense_type.csv'
target_dwd_air = 'abfss://data-warehouse-ads@dlsaaddpnorth3001.dfs.core.chinacloudapi.cn/travel expense/dwd_fi_te_air_ticket_cn.csv'
target_dwd_reimburer_exp_detail = 'abfss://data-warehouse-ads@dlsaaddpnorth3001.dfs.core.chinacloudapi.cn/travel expense/dwd_fi_te_reimburse_exp_detail.csv'
target_dwd_reimburer_header = 'abfss://data-warehouse-ads@dlsaaddpnorth3001.dfs.core.chinacloudapi.cn/travel expense/dwd_fi_te_reimburse_header.csv'
target_dwd_travel_app_header = 'abfss://data-warehouse-ads@dlsaaddpnorth3001.dfs.core.chinacloudapi.cn/travel expense/dwd_fi_te_travel_app_header_cn.csv'
target_dwd_travel_app_item = 'abfss://data-warehouse-ads@dlsaaddpnorth3001.dfs.core.chinacloudapi.cn/travel expense/dwd_fi_te_travel_app_item_cn.csv'


# copy
mssparkutils.fs.cp(dws, target_dws, True)
mssparkutils.fs.cp(dim_aviation, target_dim_aviation, True)
mssparkutils.fs.cp(dim_city, target_dim_city, True)
mssparkutils.fs.cp(dim_employee, target_dim_employee, True)
mssparkutils.fs.cp(dim_expense_type, target_dim_expense_type, True)
mssparkutils.fs.cp(dwd_air, target_dwd_air, True)
mssparkutils.fs.cp(dwd_reimburer_exp_detail,
                   target_dwd_reimburer_exp_detail, True)
mssparkutils.fs.cp(dwd_reimburer_header, target_dwd_reimburer_header, True)
mssparkutils.fs.cp(dwd_travel_app_header, target_dwd_travel_app_header, True)
mssparkutils.fs.cp(dwd_travel_app_item, target_dwd_travel_app_item, True)
