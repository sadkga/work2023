--********************************************************************--
--所属主题: 库存域
--功能描述: EXTRA与ERP系统Workshop主数据的匹配
--创建者: 王兆翔
--创建日期:2023-06-07
--********************************************************************--
insert overwrite table dim_pub_standard_address_mf
select
    md5(concat(reference_id,standard_district)) address_number,
   *
from
    (
        select
            reference_id,
            data_source,
            address,
            standard_province,
            standard_city,
            standard_district,
            etl_load_time
        from
            dim_pub_standard_address_mf
        union
        select
            reference_id,
            data_source,
            address,
            standard_province,
            standard_city,
            standard_district,
            etl_load_time
        from
            dim_pub_standard_address_di
        where address rlike '[\u4e00-\u9fa5]+'
    ) a