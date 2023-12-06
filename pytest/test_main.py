'''
Author: wangzhaoxiang
Date: 2023-12-02 10:40:07
LastEditTime: 2023-12-02 10:52:26
FilePath: \code\pytest\test_main.py
Description: 
'''
def add(y,x):
    return x +y



def test_add():
    assert add(1,3) == 4
    
def test_ad():
    assert add(1,3) == 3
    

# ======================邮件测试============================================

import smtplib
from email.mime.text import MIMEText

def send_to_email(receiver_email,subject,body):
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
    
    server = smtplib.SMTP(smtp_server, 465)
    server.starttls()
    server.login(username, password)
    server.sendmail(sender_email, receiver_email, msg.as_string())
    print('邮件发送成功')

    # 连接SMTP服务器并发送邮件
    # try:
    #     server = smtplib.SMTP(smtp_server, 587)
    #     server.starttls()
    #     server.login(username, password)
    #     server.sendmail(sender_email, receiver_email, msg.as_string())
    #     print('邮件发送成功')
    # except Exception as e:
    #     print(f'邮件发送失败: {e}')
    # finally:
    #     server.quit()



def test_send():
     send_to_email(['external.Zhaoxiang.Wang@cn.bosch.com'],'test','test')

# =====================python语法测试======================
def test_yufa():
    print('overseas'.upper())