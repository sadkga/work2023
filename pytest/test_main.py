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