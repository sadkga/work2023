--********************************************************************--
--所属主题: 库存域
--功能描述: EXTRA与ERP系统Workshop高德api数据匹配
--创建者: 王兆翔
--创建日期:2023-06-07
--********************************************************************--
DROP TABLE IF EXISTS dim_pub_standard_address_mf;
CREATE TABLE IF NOT EXISTS dim_pub_standard_address_mf (
    address_number                STRING  COMMENT '主键编码'
    ,reference_id                 STRING  COMMENT '维修站客户端id'
    ,data_source                  STRING  COMMENT '数据来源系统'
    ,address                      STRING  COMMENT '维修站联系地址'
    ,standard_province            STRING  COMMENT '维修站所在省份'
    ,standard_city                STRING  COMMENT '维修站所在城市'
    ,standard_district            STRING  COMMENT '维修站所在区'
    ,etl_load_time                STRING  COMMENT '数据加载时间'
) COMMENT '维修站主数据匹配表'
STORED AS parquet
LOCATION 'boschfs://boschfs/warehouse/dim/dim_pub_standard_address_mf';
