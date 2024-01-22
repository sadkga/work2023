
# coding=utf-8
"""
 -- @ Creater      : sadkga sadkga@88.com
 -- @ Since        : 2024-01-16 16:40:02
 -- @ LastAuthor   : sadkga sadkga@88.com
 -- @ LastTime     : 2024-01-18 10:41:37
 -- @ Location     : \\code\\main\\03-CDP_push\\mi2.py
 -- @ Message      : 
 -- @ Copyright (c) 2024 by sadkga@88.com, All Rights Reserved. 
 """
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad


def AES256(text):
    key = b'|I?=BNJtQpR1bc2"'
    # 使用PKCS7填充
    padded_data = pad(text.encode(), AES.block_size)
    cipher = AES.new(key, AES.MODE_ECB)
    encrypted_bytes = cipher.encrypt(padded_data)
    return encrypted_bytes


a = AES256("博世车联常熟康顺店").decode('utf-8')
print(a)
