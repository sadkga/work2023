/*
 * @ Creater      : sadkga sadkga@88.com
 * @ Since        : 2024-01-19 11:36:05
 * @ LastAuthor   : sadkga sadkga@88.com
 * @ LastTime     : 2024-01-19 11:36:50
 * @ Location     : \\code\\main\\03-CDP_push\\oe_api_sigh.js
 * @ message2     : 
 * Copyright (c) 2024 by sadkga@88.com, All Rights Reserved. 
 */
function md5_handle(app_secret, nonce, timestamp) {
    var sign_str = "key1=value1&key2=&key3=value3" + app_secret + nonce + timestamp;
    
    // 创建一个 MD5 对象
    var md5 = new Hashes.MD5;

    // 计算 MD5 哈希值
    var sign = md5.hex(sign_str).toUpperCase();
    
    return sign;
}

// 示例调用
var app_secret = "your_app_secret";
var nonce = "your_nonce";
var timestamp = "your_timestamp";

var result = md5_handle(app_secret, nonce, timestamp);
console.log(result);
