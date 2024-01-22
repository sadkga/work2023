--********************************************************************--
--所属主题: 产品域
--功能描述: 蓄电池O2O
--创建者:王兆翔
--创建日期:2023-04-17
--修改日期  修改人  修改内容
--yyyymmdd  name  comment
--********************************************************************--
-- Map join
set hive.auto.convert.join=true
set hive.auto.convert.join.noconditionaltask.size=512000000

-- 计算引擎Spark
set hive.execution.engine=spark;

with t1 as (
select 
    case when kh.khdm = 'TM01' then '天猫'
         when kh.khdm = 'PACP001' then '平安'
         when kh.khdm = 'Baisoon001' then '百顺'
         else '京东' 
         end online_channel,
    year(add_time) year,
    month(add_time) month,
    weekofyear(add_time) week,
    hour(add_time) hour,
    add_time `date`,
    a1.region_name province,
    a2.region_name city,
    deal_code,
    order_sn,
    payment,
    i.receiver_province,
    i.receiver_city,
    sn.completetime,
    unix_timestamp(add_time) addtime,
    row_number() over(partition by deal_code order by i.order_status desc) rn
from (select id,khdm from dwd_battery_o2o_kehu where khdm in ('TM01','JD01','JD02','PACP001','Baisoon001')) kh
join (select sd_id, from_unixtime(add_time, 'yyyy-MM-dd HH:mm:ss') add_time,order_sn, deal_code, payment, order_status,receiver_province,receiver_city from dwd_battery_o2o_order_info where receiver_province != 0) i on kh.id = i.sd_id
left join (select region_id,region_name,region_type from dwd_battery_o2o_region) a1 on i.receiver_province = a1.region_id and a1.region_type =1
left join (select region_id,region_name,region_type from dwd_battery_o2o_region) a2 on i.receiver_city = a2.region_id 
left join (select 
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
    from dwd_battery_o2o_sxonsale_netpointinstallwo) sn on sn.OrderNumber = i.order_sn
)
select 
    row_number() over() id, -- 主键Id
    online_channel, -- 线上渠道
    province, -- 省
    city,   -- 市   
    year,   -- 年
    month,  -- 月
    week,   -- 周 
    count(deal_code) order_num,    -- 订单量
    coalesce(round(avg(if(completetime is not null and completetime > addtime ,(completetime - addtime) / 3600 ,null )), 2), 0) avg_install_time,   -- 平均服务时效
    max('${bdp.system.bizdate}') update_date    -- 数据更新点
from t1 
where rn = 1 and online_channel = '平安' and year = 2023 and month >=5 and month <= 7 and hour>=9 and hour<=18
group by online_channel,province,city, year, month, week


