--********************************************************************--
--所属主题: 库存域
--功能描述: 进销商库存本月数据及其他月月初月末数据
--创建者: 王兆翔
--创建日期:2023-06-07
--********************************************************************--
set hive.tez.container.size = 4096;

with t1 as (
    select
        customercode,
        warehousename,
        warehouseno,
        boschpartno,
        boschpartno13,
        productname,
        productcategory,
        stockqty,
        unit,
        concat_ws('-' ,substring(loaddate,1,4),substring(loaddate,5,2),substring(loaddate,7,2)) loaddate,
        loadtime,
        boschpartno_verified 
    from 
        dwd_dealer_stock_history_df
    where ds =  ${bdp.system.bizdate}
) insert overwrite table ads_stock_visualization_dealer_stock_history_df partition(ds = ${bdp.system.bizdate})
select
    customercode,
    warehousename,
    warehouseno,
    boschpartno,
    boschpartno13,
    productname,
    productcategory,
    stockqty,
    unit,
    date_format(loaddate,'yyyyMMdd') loaddate,
    loadtime,
    boschpartno_verified
from t1
where (month(loaddate) != month(current_date()) and (loaddate = last_day(loaddate) or day(loaddate) = 1))
   or month(loaddate) = month(current_date())     
