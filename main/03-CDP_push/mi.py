#!/usr/bin/env python
# coding=utf-8
"""
 -- @ Creater      : sadkga sadkga@88.com
 -- @ Since        : 2024-01-16 14:48:54
 -- @ LastAuthor   : sadkga sadkga@88.com
 -- @ LastTime     : 2024-01-18 14:31:20
 -- @ Location     : \\code\\main\\03-CDP_push\\mi.py
 -- @ Message      : 
 -- @ Copyright (c) 2024 by sadkga@88.com, All Rights Reserved. 
 """

# Step 3: Apply the encryption function to specified column
# df = df.withColumn('column_name', encrypt(df['Bob']))

# Step 4: Write the result to new file or other storage location
# df.show()
# df.write.format('csv').mode('overwrite').save('/output/path')
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad


# 假设密钥为key，明文数据为text
key = b'|I?=BNJtQpR1bc2"'
text = '博世车联常熟康顺店'
text = text.encode()

# 使用PKCS7填充
padded_data = pad(text, AES.block_size)

# 创建AES加密对象
cipher = AES.new(key, AES.MODE_ECB)

# 加密
encrypted_bytes = cipher.encrypt(padded_data)

# 解密时，记得要先解密，再移除填充
decrypted_bytes = unpad(cipher.decrypt(encrypted_bytes), AES.block_size)

# 输出加密结果和解密结果
print("Encrypted:", encrypted_bytes)
print("Decrypted:", decrypted_bytes.decode('utf-8'))


def AES256(text):
    key = b'|I?=BNJtQpR1bc2"'
    # 使用PKCS7填充
    padded_data = pad(text.encode(), AES.block_size)
    cipher = AES.new(key, AES.MODE_ECB)
    encrypted_bytes = cipher.encrypt(padded_data)
    return encrypted_bytes


def AES256_1(text):
    key = b'|I?=BNJtQpR1bc2"'
    # 使用PKCS7填充
    padded_data = pad(text, AES.block_size)
    cipher = AES.new(key, AES.MODE_ECB)
    decrypted_bytes = unpad(cipher.decrypt(text), AES.block_size)
    return decrypted_bytes.decode('utf-8')


a = AES256("博世车联常熟康顺店")
b = AES256_1(a)
print(a)
print(b)


def hive_data_to_csv(self,spark):
    """hive数据转csv"""
    sql = f"select * from boschpro.{self.table_name} where pday=${bdp.system.bizdate}"
    df = spark.sql(sql)
    df.show()
    def AES256(text):
           if text == '' or text is None:
               return text
           key = b'|I?=BNJtQpR1bc2"'
           # 使用PKCS7填充
           padded_data = pad(text.encode(), AES.block_size)
           cipher = AES.new(key, AES.MODE_ECB)
           encrypted_bytes = cipher.encrypt(padded_data)
           return encrypted_bytes
    def AES256_1(text):
        if text == '' or text is None:
            return text
        key = b'|I?=BNJtQpR1bc2"'
        # 使用PKCS7填充
        padded_data = pad(text, AES.block_size)
        cipher = AES.new(key, AES.MODE_ECB)
        decrypted_bytes = unpad(cipher.decrypt(text), AES.block_size)
        return decrypted_bytes.decode('utf-8')
    AES_udf = udf(AES256, StringType())
    AES1_udf = udf(AES256_1, StringType())
    df = df.withColumn("show_name9", AES_udf(df['show_name9']))
    df.show()
    df = df.withColumn("show_name9", AES1_udf(df['show_name9']))
    df.show()
    f = df.toPandas().to_csv(sep='|',index=False,encoding='utf-8')
    return f