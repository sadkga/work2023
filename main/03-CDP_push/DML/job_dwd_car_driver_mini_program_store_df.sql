--********************************************************************--
--所属主题: 车主域
--功能描述: 小程序门店数据
--创建者: 王兆翔
--创建日期:2023-12-15
--********************************************************************--
insert overwrite table dwd_car_driver_mini_program_store_df partition(pday=${bdp.system.bizdate})
select 
    distinct id          
    ,name       
    ,store_no   
    ,show_name  
    ,address as address9
    ,latitude   
    ,longitude  
    ,province   
    ,city       
    ,area       
    ,city_index 
    ,status     
    ,created_at 
    ,updated_at 
    ,deleted_at 
    ,date_format(current_timestamp(),'yyyy-MM-dd HH:mm:ss') etl_load_time
from 
    ods_car_driver_mini_program_store_df
where ds = ${bdp.system.bizdate}


