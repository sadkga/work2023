--********************************************************************--
--所属主题: 产品域
--功能描述: 蓄电池O2O
--创建者:王兆翔
--创建日期:2023-04-19
--修改日期  修改人  修改内容
--yyyymmdd  name  comment
--********************************************************************--
-- Map join
set hive.auto.convert.join=true
set hive.auto.convert.join.noconditionaltask.size=512000000

-- 计算引擎Spark
set hive.execution.engine=spark;

with t1 as (
    -- 关联到的仓库地区表
    select
        distinct id,
        ckmc, 
        address,
        ty,
        a1.region_name province,
        a2.region_name city,
        a3.region_name district
    from (select province_id,city_id,district, id,ckmc,address,ty from dwd_battery_o2o_cangku where id != 109 ) c
left join (select region_id,region_name,region_type from dwd_battery_o2o_region) a1 on c.province_id = a1.region_id and a1.region_type =1
left join (select region_id,region_name,region_type from dwd_battery_o2o_region) a2 on c.city_id = a2.region_id and a2.region_type = 2
left join (select region_id,region_name,region_type from dwd_battery_o2o_region) a3 on c.district = a3.region_id and a3.region_type = 3
)
, t2_1 as (
select
    -- 部分事实
    distinct sn.id,
    sn.orderinfoid, -- 非指标：按订单号去重
    sn.OrderNumber,
    year(from_unixtime(o.add_time,'yyyy-MM-dd HH:mm:ss')) year,
    month(from_unixtime(o.add_time,'yyyy-MM-dd HH:mm:ss')) month,
    date(from_unixtime(o.add_time,'yyyy-MM-dd HH:mm:ss')) day,
    from_unixtime(o.add_time,'yyyyMM') yearmonth,
    scn.scn_region,
    t1.province,
    t1.city,
    t1.district,
    t1.id netpoint_id,
    t1.ckmc netpoint_name,
    t1.ty,
    t1.address,
    sn.status,
    so.return_order_status,
    so.relating_order_id,
    o.is_jj,
    o.order_sn,
    cd.netpointwo_id expedite_id,

    -- 时间准备          
    dispatchtime,
    handletime,
    appointtime,        
    o.add_time addtime,        -- 下单时间
    completetime,
    sn.stime,
    from_unixtime(o.add_time,'yyyy-MM-dd HH:mm:ss') add_time,
    min(from_unixtime(add_time, 'yyyy-MM-dd HH:mm:ss')) over(partition by
        scn.scn_region,
        t1.province,
        t1.city,
        t1.district,
        sn.netpointid,
        t1.ckmc,
        t1.address
        ) min_addtime,
    min(stime) over(partition by
        scn.scn_region,
        t1.province,
        t1.city,
        t1.district,
        sn.netpointid,
        t1.ckmc,
        t1.address
        ) min_stime
                                                
from (select 
        distinct id
        ,date_format(dispatchtime,'yyyy-MM-dd HH:mm:ss') stime   
        ,unix_timestamp(date_format(dispatchtime,'yyyy-MM-dd HH:mm:ss')) dispatchtime -- 派单时间
        ,unix_timestamp(date_format(handletime,'yyyy-MM-dd HH:mm:ss')) handletime -- 处理时间
        ,unix_timestamp(date_format(completetime,'yyyy-MM-dd HH:mm:ss')) completetime   -- 完成时间
        ,unix_timestamp(date_format(appointtime,'yyyy-MM-dd HH:mm:ss')) appointtime -- 预约时间
        ,netpointid 
        ,orderinfoid
        ,OrderNumber 
        ,ServiceInterval  
        ,status
       
    from dwd_battery_o2o_sxonsale_netpointinstallwo) sn
right join t1 on t1.id = sn.netpointid    -- 过滤掉停用仓库订单
left join (select order_id,order_sn, is_jj,unix_timestamp(from_unixtime(add_time, 'yyyy-MM-dd HH:mm:ss')) add_time from dwd_battery_o2o_order_info) o on sn.OrderNumber = o.order_sn
left join (select relating_order_id, return_order_status from dwd_battery_o2o_order_return) so on o.order_id = so.relating_order_id 
left join (select id,fhck_id,netpointwo_id,cd_time from dwd_battery_o2o_bosch_cd ) cd on cd.fhck_id = sn.netpointid and cd.netpointwo_id = sn.id
left join (select province,scn_region from ads_battery_o2o_scn_region) scn on t1.province = scn.province

) 
, t2 as (
select 
    id,
    orderinfoid,
    OrderNumber,
    year,
    month,
    day,
    yearmonth,
    scn_region,
    province,
    city,
    district,
    netpoint_id,
    netpoint_name,
    ty,
    address,
    status,
    return_order_status,
    relating_order_id,
    is_jj,
    order_sn,
    expedite_id,

    -- 时间准备          
    max(dispatchtime) dispatchtime,
    max(handletime) handletime,
    max(appointtime) appointtime,        
    max(addtime) addtime,        -- 下单时间
    max(completetime) completetime,
    max(stime) stime,
    max(add_time) add_time,
    min_addtime,
    min_stime
from t2_1
group by id,orderinfoid, OrderNumber, year, month, day,yearmonth, scn_region, province, city, district, netpoint_id, netpoint_name,
ty,address, status, return_order_status, relating_order_id,is_jj, order_sn, expedite_id,min_addtime, min_stime
)
, tyear as (
    -- 年月表
   select
   distinct netpoint_id,
   netpoint_name,
   cast(date_format(d, 'yyyy') as int) year,
   cast(date_format(d, 'MM') as int) month,
   cast(date_format(d, 'yyyyMM') as int) yearmonth
from (
  select add_months('2022-01-01', m) as d, netpoint_id, netpoint_name
  from (
    select posexplode(split(space(datediff(current_date(), '2022-01-01')), ' ')) as (m, dummy)
  ) t
cross join
  (select distinct netpoint_id, netpoint_name from t2) as netpoint -- 网点
) t
distribute by netpoint_id,netpoint_name
sort by year, month
)
-- select * from t2
, t3 as (
select 
    distinct province,
    city,
    netpoint_id,
    netpoint_name,
    year,
    month
from t2
) 
, t4 as (
select 
    distinct a.province,
    a.city,
    tyear.netpoint_id,
    tyear.netpoint_name,
    tyear.yearmonth,
    if(a.mon_num is not null,1,0) is_active_netpoint
from (select * from tyear where year <= year(current_date)) tyear
left join (
    select 
        province,
        city,
        netpoint_id,
        netpoint_name,
        year,
        month, 
        yearmonth,
        count(distinct order_sn) mon_num
    from t2
    group by province, city, netpoint_id, netpoint_name, year, month, yearmonth
    ) a
 on tyear.year = a.year and tyear.month = a.month and tyear.netpoint_id = a.netpoint_id and tyear.netpoint_name = a.netpoint_name 
)

, t5 as (
select
    distinct t3.province,
    t3.city,
    t4.netpoint_id,
    t4.netpoint_name,
    t4.yearmonth,
    t4.is_active_netpoint 
from t4
left join t3 on t3.netpoint_id =t4.netpoint_id 
order by t4.netpoint_id,t4.yearmonth
)
insert overwrite table ads_battery_o2o_is_active
select 
    *
    ,if(lag(is_active_netpoint,1,0) over(partition by province,city,netpoint_id,netpoint_name order by yearmonth) = 1
        or
        lag(is_active_netpoint,2,0) over(partition by province,city,netpoint_id,netpoint_name order by yearmonth) = 1
        or is_active_netpoint = 1 ,1,0) is_order_netpoint
from t5;