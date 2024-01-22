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
    case when kh.khdm = 'TM01' then '天猫'
        when kh.khdm = 'PACP001' then '平安'
        when kh.khdm = 'Baisoon001' then '百顺'
        when kh.khdm = 'JD01' or kh.khdm = 'JD02' then '京东'
        else '' 
        end channel,

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
left join (select sd_id,order_id,order_sn, is_jj,unix_timestamp(from_unixtime(add_time, 'yyyy-MM-dd HH:mm:ss')) add_time from dwd_battery_o2o_order_info) o on sn.OrderNumber = o.order_sn
left join (select id,khdm from dwd_battery_o2o_kehu where khdm in ('TM01','JD01','JD02','PACP001','Baisoon001')) kh on kh.id = o.sd_id
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
    channel,

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
group by id,orderinfoid, OrderNumber, year, month, day, scn_region, province, city, district, channel, netpoint_id, netpoint_name,ty,
address, status, return_order_status, relating_order_id,is_jj, order_sn, expedite_id,min_addtime, min_stime
)
, tyear as (
    -- 年月表
   select
   distinct netpoint_id,
   netpoint_name,
   cast(date_format(d, 'yyyy') as int) year,
   cast(date_format(d, 'MM') as int) month
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
    distinct netpoint_id,
    netpoint_name,
    year,
    month,
    count(distinct date_format(day,'yyyyMM')) over (partition by scn_region, province,city,district,netpoint_id,netpoint_name,address order by year,month,netpoint_name ) result,
    count(if(date_add(min_addtime,90) >= add_time, 1,null)) over(partition by netpoint_id, netpoint_name order by year,month,netpoint_name) as first_3_mon_dispatch_order                                  -- 首三个月派单量
from t2
) 
, t4 as (
select 
    distinct tyear.netpoint_id,
    tyear.netpoint_name,
    tyear.year year,
    tyear.month month,
    a.mon_num,
    coalesce(a.mon_num,0) + coalesce(lag(mon_num,1) over(partition by tyear.netpoint_id, tyear.netpoint_name order by tyear.year, tyear.month),0) 
    + coalesce(lag(mon_num,2) over(partition by tyear.netpoint_id, tyear.netpoint_name order by tyear.year, tyear.month), 0) last_3_mon_dispatch_order
from (select * from tyear where year <= year(current_date)) tyear
left join (
    select 
        netpoint_id,
        netpoint_name,
        year,
        month, 
        count(distinct order_sn) mon_num
    from t2
    group by netpoint_id, netpoint_name, year, month
    ) a
 on tyear.year = a.year and tyear.month = a.month and tyear.netpoint_id = a.netpoint_id and tyear.netpoint_name = a.netpoint_name 
order by tyear.netpoint_id,year,month
)
insert overwrite table ads_battery_o2o_service_quality_month partition(ds = ${bdp.system.bizdate})
select
    row_number() over() id, -- 主键ID
    coalesce(t2.year, 0) year,                   -- 年
    coalesce(t2.month, 0) month,                  -- 月
    -- day,
    scn_region,                 -- scn大区
    t2.province,                -- 省
    t2.city,                    -- 市
    district,                   -- 区
    channel,                    -- 线上渠道
    t2.netpoint_id,             -- 网点ID
    t2.netpoint_name,           -- 网点名称
    t2.ty,                      -- 是否启用
    address,                    -- 联系地址 
    
    coalesce(nt.netpoint_type, '无订单网点') netpoint_type,                                                                 -- 网点类型
    if(datediff(if(t2.year = year(current_date) and t2.month = month(current_date), date(current_date), last_day(concat_ws('-',cast(year as string), cast(t2.month as string),'01'))) ,min(min_stime)) <= 90, 1, 0) is_new_netpoint,     -- 是否新网点
    
    case when  if(datediff(if(t2.year = year(current_date) and t2.month = month(current_date), date(current_date), last_day(concat_ws('-',cast(year as string), cast(t2.month as string),'01'))) ,min(min_stime)) <= 90, 1, 0) = 1 
            and t4.last_3_mon_dispatch_order >= 15 then 1
         when  if(datediff(if(t2.year = year(current_date) and t2.month = month(current_date), date(current_date), last_day(concat_ws('-',cast(year as string), cast(t2.month as string),'01'))) ,min(min_stime)) <= 90, 1, 0) = 0 
            and t4.last_3_mon_dispatch_order >= 90 then 1
        else 0 end is_high_quality_netpoint,                                                                                -- 是否为优质网点
    
    case
        when max(add_time) >=  date_sub(current_date,30) then '1'
        when max(add_time) <   date_sub(current_date,30) then '0'
        else '' end is_active_netpoint,                                                                                    -- 是否为活跃网点
         
    date_format(min(min_stime),'yyyyMMdd') register_date,                                                                   -- 上线日期
    date_format(max(stime),'yyyyMMdd') latest_date,                                                                         -- 最近一次派单日期
    (t2.year- year(min(min_addtime))) * 12 + (t2.month - month(min(min_addtime))) + 1 running_month_num,                    -- 运营月份数
    t3.result  dispatch_month_num,                                                                                          -- 实际派单月份数
    t3.first_3_mon_dispatch_order,
    t4.last_3_mon_dispatch_order,                            


    count(distinct id) dispatch_order_num,   -- 派单量
    count(case when return_order_status != 3 then 1 else null end) return_order_num, -- 退单量
    coalesce(round(count(case when return_order_status != 3 then 1 else null end)/count(distinct id),2),0) return_order_rate, -- 退单率

    coalesce(round(
        sum(if((cast(substring(add_time,12,2) as int) >9 and cast(substring(add_time,12,2) as int) < 18),((handletime - dispatchtime) /3600),null)) /
        count(distinct if((cast(substring(add_time,12,2) as int) >9 and cast(substring(add_time,12,2) as int) < 18),id,null))
        , 2), 0) avg_order_time,   -- 平均接单时效
      
    coalesce(round(
        sum(if(completetime is not null and completetime > handletime and (cast(substring(add_time,12,2) as int) >9 and cast(substring(add_time,12,2) as int) < 18) ,(completetime - handletime) / 3600 ,null )) /
        count(distinct if((cast(substring(add_time,12,2) as int) >9 and cast(substring(add_time,12,2) as int) < 18),id,null))
        , 2), 0) avg_install_time,   -- 平均服务时效

    coalesce(round(
        count(if(completetime is not null and (completetime > appointtime) and (cast(substring(add_time,12,2) as int) >9 and cast(substring(add_time,12,2) as int) < 18), 1,null)) /
        count(distinct if((cast(substring(add_time,12,2) as int) >9 and cast(substring(add_time,12,2) as int) < 18),id,null))
        , 2), 0) ontime_service_rate,    -- 及时服务率

    coalesce(round(
        sum(if((cast(substring(add_time,12,2) as int) >9 and cast(substring(add_time,12,2) as int) < 18),((appointtime - dispatchtime) / 3600 ),null)) /
        count(distinct if((cast(substring(add_time,12,2) as int) >9 and cast(substring(add_time,12,2) as int) < 18),id,null))
        , 2), 0) ontime_appointment_time,   -- 平均及时预约率

    coalesce(count(if(completetime is not null and ((completetime - addtime) / 3600) < 2 and (cast(substring(add_time,12,2) as int) >9 and cast(substring(add_time,12,2) as int) < 18)
                    , 1, null)), 0) within_2_hour_order_num,     -- 2小时安装时效达成订单数

    coalesce(round(
        count(if(completetime is not null and (completetime - addtime) / 3600 < 2 and (cast(substring(add_time,12,2) as int) >9 and cast(substring(add_time,12,2) as int) < 18), 1, null)) / 
        count(distinct if((cast(substring(add_time,12,2) as int) >9 and cast(substring(add_time,12,2) as int) < 18),id,null))
        ,2), 0) within_2_hour_rate, -- 2小时安装时效达成率
    
    coalesce(count(if(completetime is not null and ((completetime - addtime) / 3600) < 4 and (cast(substring(add_time,12,2) as int) >9 and cast(substring(add_time,12,2) as int) < 18)
                    , 1, null)), 0) within_4_hour_order_num,     -- 4小时安装时效达成订单数

    coalesce(round(
        count(if(completetime is not null and (completetime - addtime) / 3600 < 4 and (cast(substring(add_time,12,2) as int) >9 and cast(substring(add_time,12,2) as int) < 18), 1, null)) / 
        count(distinct if((cast(substring(add_time,12,2) as int) >9 and cast(substring(add_time,12,2) as int) < 18),id,null))
        ,2), 0) within_4_hour_rate, -- 4小时安装时效达成率

    coalesce(count(if(status = 8 or status = 10 , 1, null)), 0) reject_order_num,                        -- 拒单量
    coalesce(round(count(if(status = 8 or status = 10 , 1, null)) / count(distinct id), 2), 0) reject_order_rate, -- 拒单率
    coalesce(cast((1- round(count(if(status = 8 or status = 10 , 1, null)) / count(distinct id), 2)) as decimal(18,2)) * 100 , 0) reject_order_score,  -- 拒单分

    coalesce(count(distinct expedite_id), 0) expedite_order_num,            -- 催单量
    coalesce(round(count(distinct expedite_id) / count(distinct id) , 2), 0) expedite_order_rate,      -- 催单率
    coalesce(cast((1- round(count(distinct expedite_id) / count(distinct id), 2)) as decimal(18,2)) * 100, 0) expedite_order_score,  -- 催单分
                 
    coalesce(cast((1- round(count(if(return_order_status != 3 , 1, null)) / count(distinct id), 2)) as decimal(18, 2)) * 100, 0) return_order_score, -- 退单分
    coalesce(count(if(((handletime - dispatchtime) / 60) <= 10 and (cast(substring(add_time,12,2) as int) >9 and cast(substring(add_time,12,2) as int) < 18) 
                    , 1, null)), 0) within_10_min_order_num,  -- 10分钟接单量

    coalesce(round(
        count(if(((handletime - dispatchtime) / 60) <= 10  and (cast(substring(add_time,12,2) as int) >9 and cast(substring(add_time,12,2) as int) < 18), 1, null)) / 
        count(distinct if((cast(substring(add_time,12,2) as int) >9 and cast(substring(add_time,12,2) as int) < 18),id,null))
        , 2) , 0) response_rate,      -- 10分钟响应率

    coalesce(round(
        count(if(((handletime - dispatchtime) / 60) <= 10  and (cast(substring(add_time,12,2) as int) >9 and cast(substring(add_time,12,2) as int) < 18), 1, null)) / 
        count(distinct if((cast(substring(add_time,12,2) as int) >9 and cast(substring(add_time,12,2) as int) < 18),id,null))
        , 2) * 100, 0) response_score,    -- 时效分
    cast(coalesce(cast((1- round(count(distinct expedite_id) / count(distinct id), 2)) as decimal(18,2)) * 100, 0) * 0.3 + coalesce(cast((1- round(count(if(return_order_status != 3 , 1, null)) / count(distinct id), 2)) as decimal(18, 2)) * 100, 0) * 0.5 + coalesce(cast((1- round(count(if(status = 8 or status = 10 , 1, null)) / count(distinct id), 2)) as decimal(18,2)) * 100 , 0) * 0.2 as decimal(18,2)) total_score,    -- 综合评分    
    cast(
        -- 订单分
        coalesce(if(t4.mon_num / if(t2.year = year(current_date) and t2.month = month(current_date), day(current_date), day(last_day(concat_ws('-',cast(year as string), cast(t2.month as string),'01')))) * 100 > 100, 100,
                    t4.mon_num / if(t2.year = year(current_date) and t2.month = month(current_date), day(current_date), day(last_day(concat_ws('-',cast(year as string), cast(t2.month as string),'01')))) * 100), 0) * 0.3
        +
        -- 近期活跃分
        coalesce((
           datediff((if(t2.year = year(current_date) and t2.month = month(current_date), date(current_date), last_day(concat_ws('-',cast(year as string), cast(t2.month as string),'01')))), min(min_addtime))
         - datediff(if(t2.year = year(current_date) and t2.month = month(current_date), date(current_date), last_day(concat_ws('-',cast(year as string), cast(t2.month as string),'01'))), max(add_time))) 
         / datediff(if(t2.year = year(current_date) and t2.month = month(current_date), date(current_date), last_day(concat_ws('-',cast(year as string), cast(t2.month as string),'01'))), min(min_addtime)) * 100, 0) * 0.1
        +
        -- 退单分
        coalesce(cast((1- round(count(if(return_order_status != 3 , 1, null)) / count(distinct id), 2)) as decimal(18, 2)) * 100, 0) * 0.2
        +
        -- 拒单分
        coalesce(cast((1- round(count(if(status = 8 or status = 10 , 1, null)) / count(distinct id), 2)) as decimal(18,2)) * 100, 0) * 0.15
        +
        -- 催单分
        coalesce(cast((1- round(count(distinct expedite_id) / count(distinct id), 2)) as decimal(18,2)) * 100, 0) * 0.15
        +
        -- 2小时达成分
        coalesce(round(
            count(if(completetime is not null and (completetime - addtime) / 3600 < 2 and (cast(substring(add_time,12,2) as int) >9 and cast(substring(add_time,12,2) as int) < 18), 1, null)) / 
            count(distinct if((cast(substring(add_time,12,2) as int) >9 and cast(substring(add_time,12,2) as int) < 18),id,null))
            ,2), 0) * 100 * 0.1

        as decimal(18,2)) total_health_score,  -- 综合健康评分
    max('${bdp.system.bizdate}') update_date    -- 数据更新时间

from t2
left join (select netpoint_id, netpoint_name, netpoint_type from dim_battery_o2o_netpoint_type_df where ds = ${bdp.system.bizdate}) nt on t2.netpoint_id = nt.netpoint_id and t2.netpoint_name = nt.netpoint_name
left join t3 on t2.year = t3.year and t3.month = t2.month and t2.netpoint_id = t3.netpoint_id and t3.netpoint_name = t2.netpoint_name
left join t4 on t2.year = t4.year and t4.month = t2.month and t2.netpoint_id = t4.netpoint_id and t4.netpoint_name = t2.netpoint_name
group by t2.year,t2.month,scn_region, t2.province,t2.city,district,channel,t2.netpoint_id,t2.netpoint_name,t2.ty, address,nt.netpoint_type,t3.result,t3.first_3_mon_dispatch_order,t4.last_3_mon_dispatch_order,t4.mon_num                           

union all   -- 添加全渠道汇总

select
    row_number() over() id, -- 主键ID
    coalesce(t2.year, 0) year,                   -- 年
    coalesce(t2.month, 0) month,                  -- 月
    -- day,
    scn_region,                 -- scn大区
    t2.province,                -- 省
    t2.city,                    -- 市
    district,                   -- 区
    'all_channel' channel,      -- 线上渠道
    t2.netpoint_id,             -- 网点ID
    t2.netpoint_name,           -- 网点名称
    t2.ty,                      -- 是否启用
    address,                    -- 联系地址 
    
    coalesce(nt.netpoint_type, '无订单网点') netpoint_type,                                                                 -- 网点类型
    if(datediff(if(t2.year = year(current_date) and t2.month = month(current_date), date(current_date), last_day(concat_ws('-',cast(year as string), cast(t2.month as string),'01'))) ,min(min_stime)) <= 90, 1, 0) is_new_netpoint,     -- 是否新网点
    
    case when  if(datediff(if(t2.year = year(current_date) and t2.month = month(current_date), date(current_date), last_day(concat_ws('-',cast(year as string), cast(t2.month as string),'01'))) ,min(min_stime)) <= 90, 1, 0) = 1 
            and t4.last_3_mon_dispatch_order >= 15 then 1
         when  if(datediff(if(t2.year = year(current_date) and t2.month = month(current_date), date(current_date), last_day(concat_ws('-',cast(year as string), cast(t2.month as string),'01'))) ,min(min_stime)) <= 90, 1, 0) = 0 
            and t4.last_3_mon_dispatch_order >= 90 then 1
        else 0 end is_high_quality_netpoint,                                                                                -- 是否为优质网点
    
    case
        when max(add_time) >=  date_sub(current_date,30) then '1'
        when max(add_time) <   date_sub(current_date,30) then '0'
        else '' end is_active_netpoint,                                                                                    -- 是否为活跃网点
         
    date_format(min(min_stime),'yyyyMMdd') register_date,                                                                   -- 上线日期
    date_format(max(stime),'yyyyMMdd') latest_date,                                                                         -- 最近一次派单日期
    (t2.year- year(min(min_addtime))) * 12 + (t2.month - month(min(min_addtime))) + 1 running_month_num,                    -- 运营月份数
    t3.result  dispatch_month_num,                                                                                          -- 实际派单月份数
    t3.first_3_mon_dispatch_order,
    t4.last_3_mon_dispatch_order,                            


    count(distinct id) dispatch_order_num,   -- 派单量
    count(case when return_order_status != 3 then 1 else null end) return_order_num, -- 退单量
    coalesce(round(count(case when return_order_status != 3 then 1 else null end)/count(distinct id),2),0) return_order_rate, -- 退单率

    coalesce(round(
        sum(if((cast(substring(add_time,12,2) as int) >9 and cast(substring(add_time,12,2) as int) < 18),((handletime - dispatchtime) /3600),null)) /
        count(distinct if((cast(substring(add_time,12,2) as int) >9 and cast(substring(add_time,12,2) as int) < 18),id,null))
        , 2), 0) avg_order_time,   -- 平均接单时效
      
    coalesce(round(
        sum(if(completetime is not null and completetime > handletime and (cast(substring(add_time,12,2) as int) >9 and cast(substring(add_time,12,2) as int) < 18) ,(completetime - handletime) / 3600 ,null )) /
        count(distinct if((cast(substring(add_time,12,2) as int) >9 and cast(substring(add_time,12,2) as int) < 18),id,null))
        , 2), 0) avg_install_time,   -- 平均服务时效

    coalesce(round(
        count(if(completetime is not null and (completetime > appointtime) and (cast(substring(add_time,12,2) as int) >9 and cast(substring(add_time,12,2) as int) < 18), 1,null)) /
        count(distinct if((cast(substring(add_time,12,2) as int) >9 and cast(substring(add_time,12,2) as int) < 18),id,null))
        , 2), 0) ontime_service_rate,    -- 及时服务率

    coalesce(round(
        sum(if((cast(substring(add_time,12,2) as int) >9 and cast(substring(add_time,12,2) as int) < 18),((appointtime - dispatchtime) / 3600 ),null)) /
        count(distinct if((cast(substring(add_time,12,2) as int) >9 and cast(substring(add_time,12,2) as int) < 18),id,null))
        , 2), 0) ontime_appointment_time,   -- 平均及时预约率

    coalesce(count(if(completetime is not null and ((completetime - addtime) / 3600) < 2 and (cast(substring(add_time,12,2) as int) >9 and cast(substring(add_time,12,2) as int) < 18)
                    , 1, null)), 0) within_2_hour_order_num,     -- 2小时安装时效达成订单数

    coalesce(round(
        count(if(completetime is not null and (completetime - addtime) / 3600 < 2 and (cast(substring(add_time,12,2) as int) >9 and cast(substring(add_time,12,2) as int) < 18), 1, null)) / 
        count(distinct if((cast(substring(add_time,12,2) as int) >9 and cast(substring(add_time,12,2) as int) < 18),id,null))
        ,2), 0) within_2_hour_rate, -- 2小时安装时效达成率
    
    coalesce(count(if(completetime is not null and ((completetime - addtime) / 3600) < 4 and (cast(substring(add_time,12,2) as int) >9 and cast(substring(add_time,12,2) as int) < 18)
                    , 1, null)), 0) within_4_hour_order_num,     -- 4小时安装时效达成订单数

    coalesce(round(
        count(if(completetime is not null and (completetime - addtime) / 3600 < 4 and (cast(substring(add_time,12,2) as int) >9 and cast(substring(add_time,12,2) as int) < 18), 1, null)) / 
        count(distinct if((cast(substring(add_time,12,2) as int) >9 and cast(substring(add_time,12,2) as int) < 18),id,null))
        ,2), 0) within_4_hour_rate, -- 4小时安装时效达成率

    coalesce(count(if(status = 8 or status = 10 , 1, null)), 0) reject_order_num,                        -- 拒单量
    coalesce(round(count(if(status = 8 or status = 10 , 1, null)) / count(distinct id), 2), 0) reject_order_rate, -- 拒单率
    coalesce(cast((1- round(count(if(status = 8 or status = 10 , 1, null)) / count(distinct id), 2)) as decimal(18,2)) * 100 , 0) reject_order_score,  -- 拒单分

    coalesce(count(distinct expedite_id), 0) expedite_order_num,            -- 催单量
    coalesce(round(count(distinct expedite_id) / count(distinct id) , 2), 0) expedite_order_rate,      -- 催单率
    coalesce(cast((1- round(count(distinct expedite_id) / count(distinct id), 2)) as decimal(18,2)) * 100, 0) expedite_order_score,  -- 催单分
                 
    coalesce(cast((1- round(count(if(return_order_status != 3 , 1, null)) / count(distinct id), 2)) as decimal(18, 2)) * 100, 0) return_order_score, -- 退单分
    coalesce(count(if(((handletime - dispatchtime) / 60) <= 10 and (cast(substring(add_time,12,2) as int) >9 and cast(substring(add_time,12,2) as int) < 18) 
                    , 1, null)), 0) within_10_min_order_num,  -- 10分钟接单量

    coalesce(round(
        count(if(((handletime - dispatchtime) / 60) <= 10  and (cast(substring(add_time,12,2) as int) >9 and cast(substring(add_time,12,2) as int) < 18), 1, null)) / 
        count(distinct if((cast(substring(add_time,12,2) as int) >9 and cast(substring(add_time,12,2) as int) < 18),id,null))
        , 2) , 0) response_rate,      -- 10分钟响应率

    coalesce(round(
        count(if(((handletime - dispatchtime) / 60) <= 10  and (cast(substring(add_time,12,2) as int) >9 and cast(substring(add_time,12,2) as int) < 18), 1, null)) / 
        count(distinct if((cast(substring(add_time,12,2) as int) >9 and cast(substring(add_time,12,2) as int) < 18),id,null))
        , 2) * 100, 0) response_score,    -- 时效分
    cast(coalesce(cast((1- round(count(distinct expedite_id) / count(distinct id), 2)) as decimal(18,2)) * 100, 0) * 0.3 + coalesce(cast((1- round(count(if(return_order_status != 3 , 1, null)) / count(distinct id), 2)) as decimal(18, 2)) * 100, 0) * 0.5 + coalesce(cast((1- round(count(if(status = 8 or status = 10 , 1, null)) / count(distinct id), 2)) as decimal(18,2)) * 100 , 0) * 0.2 as decimal(18,2)) total_score,    -- 综合评分    
    cast(
        -- 订单分
        coalesce(if(t4.mon_num / if(t2.year = year(current_date) and t2.month = month(current_date), day(current_date), day(last_day(concat_ws('-',cast(year as string), cast(t2.month as string),'01')))) * 100 > 100, 100,
                    t4.mon_num / if(t2.year = year(current_date) and t2.month = month(current_date), day(current_date), day(last_day(concat_ws('-',cast(year as string), cast(t2.month as string),'01')))) * 100), 0) * 0.3
        +
        -- 近期活跃分
        coalesce((
           datediff((if(t2.year = year(current_date) and t2.month = month(current_date), date(current_date), last_day(concat_ws('-',cast(year as string), cast(t2.month as string),'01')))), min(min_addtime))
         - datediff(if(t2.year = year(current_date) and t2.month = month(current_date), date(current_date), last_day(concat_ws('-',cast(year as string), cast(t2.month as string),'01'))), max(add_time))) 
         / datediff(if(t2.year = year(current_date) and t2.month = month(current_date), date(current_date), last_day(concat_ws('-',cast(year as string), cast(t2.month as string),'01'))), min(min_addtime)) * 100, 0) * 0.1
        +
        -- 退单分
        coalesce(cast((1- round(count(if(return_order_status != 3 , 1, null)) / count(distinct id), 2)) as decimal(18, 2)) * 100, 0) * 0.2
        +
        -- 拒单分
        coalesce(cast((1- round(count(if(status = 8 or status = 10 , 1, null)) / count(distinct id), 2)) as decimal(18,2)) * 100, 0) * 0.15
        +
        -- 催单分
        coalesce(cast((1- round(count(distinct expedite_id) / count(distinct id), 2)) as decimal(18,2)) * 100, 0) * 0.15
        +
        -- 2小时达成分
        coalesce(round(
            count(if(completetime is not null and (completetime - addtime) / 3600 < 2 and (cast(substring(add_time,12,2) as int) >9 and cast(substring(add_time,12,2) as int) < 18), 1, null)) / 
            count(distinct if((cast(substring(add_time,12,2) as int) >9 and cast(substring(add_time,12,2) as int) < 18),id,null))
            ,2), 0) * 100 * 0.1

        as decimal(18,2)) total_health_score,  -- 综合健康评分
    max('${bdp.system.bizdate}') update_date    -- 数据更新时间

from t2
left join (select netpoint_id, netpoint_name, netpoint_type from dim_battery_o2o_netpoint_type_df where ds = ${bdp.system.bizdate}) nt on t2.netpoint_id = nt.netpoint_id and t2.netpoint_name = nt.netpoint_name
left join t3 on t2.year = t3.year and t3.month = t2.month and t2.netpoint_id = t3.netpoint_id and t3.netpoint_name = t2.netpoint_name
left join t4 on t2.year = t4.year and t4.month = t2.month and t2.netpoint_id = t4.netpoint_id and t4.netpoint_name = t2.netpoint_name
group by t2.year,t2.month,scn_region, t2.province,t2.city,district,t2.netpoint_id,t2.netpoint_name,t2.ty, address,nt.netpoint_type,t3.result,t3.first_3_mon_dispatch_order,t4.last_3_mon_dispatch_order,t4.mon_num                           

-- select * from ads_battery_o2o_service_quality where ds = 20230703 and running_month_num < 0

