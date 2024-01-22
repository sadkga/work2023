--********************************************************************--
--所属主题: 车主域
--功能描述: 小程序客户数据
--创建者: 王兆翔
--创建日期:2023-12-15
--********************************************************************--
WITH t1 AS (
    SELECT
        distinct id,
        mobile as m9,
        open_id,
        union_id,
        nick_name,
        real_name,
        avatar,
        gender,
        country,
        province,
        city,
        birthday,
        last_auth_at,
        last_login_at,
        register_from,
        is_logout
    FROM
        ods_car_driver_mini_program_customer_df
    WHERE
        ds = ${bdp.system.bizdate}
)
,t2 AS (
    SELECT
        distinct customer_id,
        vehicle_brand_id,
        brand_name,
        brand_logo,
        model_class,
        vehicle_model_id,
        model_name,
        sell_name,
        vehicle_name,
        vehicle_id,
        product_year,
        output_volume,
        manufacturer,
        engine_code,
        license_plate,
        vin as vin9,
        remark,
        is_default
    FROM
        ods_car_driver_mini_program_customer_vehicle_df
    where ds = ${bdp.system.bizdate} 
)
insert overwrite table dwd_car_driver_mini_program_customer_df partition(pday=${bdp.system.bizdate})
select
    row_number() over() id
    ,t1.id customer_id
    ,t1.m9 as phone9
    ,t1.open_id
    ,t1.union_id
    ,t1.nick_name as nick_name9
    ,t1.real_name as real_name9
    ,t1.avatar
    ,t1.gender
    ,t1.country
    ,t1.province
    ,t1.city
    ,t1.birthday
    ,t1.last_auth_at
    ,t1.last_login_at
    ,t1.register_from
    ,t1.is_logout
    ,t2.vehicle_brand_id
    ,t2.brand_name
    ,t2.brand_logo
    ,t2.model_class
    ,t2.vehicle_model_id
    ,t2.model_name
    ,t2.sell_name as sell_name9
    ,t2.vehicle_name
    ,t2.vehicle_id
    ,t2.product_year
    ,t2.output_volume
    ,t2.manufacturer
    ,t2.engine_code
    ,t2.license_plate as license_plate9
    ,t2.vin9
    ,t2.remark
    ,t2.is_default
    ,store.store_no
    ,date_format(current_timestamp(),'yyyy-MM-dd HH:mm:ss') etl_load_time
from t1 
left join t2 on t1.id = t2.customer_id
left join 
    (select distinct customer_id,concat_ws('/', collect_list(store_no) over(partition by customer_id)) store_no
     from ods_car_driver_mini_program_customer_store_df where ds = ${bdp.system.bizdate}) store
on t1.id = store.customer_id

-- select * from dwd_car_driver_mini_program_customer_df 