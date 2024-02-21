--********************************************************************--
--所属主题: 产品域
--功能描述: sellout——金额计算
--创建者:王兆翔
--创建日期:2023/08/22
--修改日期  修改人  修改内容
--yyyymmdd  name  comment
--********************************************************************--
insert overwrite table ods_fepaa_currency_gc_df partition(pday=${bdp.system.bizdate})
select * ,date_format(CURRENT_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss') etl_load_time from (
select
    material_number         
    ,calendar_year_month    
    ,year                   
    ,month                  
    ,fepaa                  
    ,is_valid               
from
    ods_fepaa_currency_gc_df
where
    pday in (select max(pday) from ods_fepaa_currency_gc_df) and pday !=${bdp.system.bizdate} 
union all
select 
    material_number         
    ,calendar_year_month    
    ,year                   
    ,month                  
    ,fepaa                  
    ,is_valid
from 
    stg_fepaa_currency_gc_di
where 
    pday in (select max(pday) from stg_fepaa_currency_gc_di)
) a
group by  material_number         
    ,calendar_year_month    
    ,year                   
    ,month                  
    ,fepaa                   
    ,is_valid