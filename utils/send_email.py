#!/usr/bin/env python
# coding=utf-8
"""
 -- @ Creater      : sadkga sadkga@88.com
 -- @ Since        : 2024-01-10 13:23:21
 -- @ LastAuthor   : sadkga sadkga@88.com
 -- @ LastTime     : 2024-01-10 13:23:59
 -- @ Location     : \\code\\utils\\send_email.py
 -- @ Message      : 
 -- @ Copyright (c) 2024 by sadkga@88.com, All Rights Reserved. 
 """
import smtplib
from email.mime.text import MIMEText


def send_to_email(receiver_email, subject, body):
    """请设置receiver_email、subject、body"""

    # 设置发件人和收件人
    sender_email = 'zhaoxiang_wang@leansight.cn'

    # 创建 MIMEText 对象
    msg = MIMEText(body, 'plain')
    msg['Subject'] = subject
    msg['From'] = sender_email
    msg['To'] = ','.join(receiver_email)

    # 设置SMTP服务器
    smtp_server = 'smtp.exmail.qq.com'  # 以Gmail为例

    # 登录邮箱
    username = 'zhaoxiang_wang@leansight.cn'
    password = '8EadfD8k4TEB7De6'

    # 连接SMTP服务器并发送邮件
    try:
        server = smtplib.SMTP(smtp_server, 465)
        server.starttls()
        server.login(username, password)
        server.sendmail(sender_email, receiver_email, msg.as_string())
        print('邮件发送成功')
    except Exception as e:
        print(f'邮件发送失败: {e}')
    finally:
        server.quit()


print('请设置receiver_email、subject、body，以发送邮件')
send_to_email(['external.Zhaoxiang.Wang@cn.bosch.com',
              'external.huajie.guo@cn.bosch.com'], 'simba send email', 'Hello, all')
