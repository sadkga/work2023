#!/usr/bin/env python
# coding=utf-8
"""
 -- @ Creater      : sadkga sadkga@88.com
 -- @ Since        : 2023-12-10 00:06:53
 -- @ LastAuthor   : sadkga sadkga@88.com
 -- @ LastTime     : 2023-12-10 17:19:36
 -- @ Location     : \\work2023\\utils\\test.py
 -- @ Message      : 
 -- @ Copyright (c) 2023 by sadkga@88.com, All Rights Reserved. 
 """
from blob_utools import blob_tools
from datetime import datetime, timedelta

if __name__ == '__main__':
    formatted_endtime = datetime.now()
    formatted_endtime -= timedelta(days=99)
    year = formatted_endtime.strftime("%Y")
    month = formatted_endtime.strftime("%m")
    day = formatted_endtime.strftime("%d")
    print(month, day)
