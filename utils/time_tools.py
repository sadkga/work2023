#!/usr/bin/env python
# coding=utf-8
"""
 -- @ Creater      : error: error: git config user.name & please set dead value or install git && error: git config user.email & please set dead value or install git & please set dead value or install git
 -- @ Since        : 2023-12-12 10:23:39
 -- @ LastAuthor   : error: error: git config user.name & please set dead value or install git && error: git config user.email & please set dead value or install git & please set dead value or install git
 -- @ LastTime     : 2024-01-16 13:23:25
 -- @ Location     : \\code\\utils\\time_tools.py
 -- @ Message      : 
 -- @ Copyright (c) 2024 by sadkga@88.com, All Rights Reserved. 
 """
from datetime import datetime

def get_last_month():
    """ 
     * @ message : 获取上个月月份
     * @ return   [int] 月份
    """
    current_date = datetime.now()
    current_year = current_date.year
    current_month = current_date.month
    previous_month = current_month - 1 if current_month > 1 else 12
    previous_year = current_year if current_month > 1 else current_year -1
    last_month = str(previous_year) + str(previous_month).zfill(2)
    return last_month


    