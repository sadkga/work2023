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
    add_time `date`,
    a1.region_name province,
    a2.region_name city,
    deal_code,
    payment,
    i.receiver_province,
    i.receiver_city,
    row_number() over(partition by deal_code order by i.order_status desc) rn
from (select id,khdm from dwd_battery_o2o_kehu where khdm in ('TM01','JD01','JD02','PACP001','Baisoon001')) kh
join (select order_id, sd_id, from_unixtime(add_time, 'yyyy-MM-dd') add_time, deal_code, payment, order_status,receiver_province,receiver_city from dwd_battery_o2o_order_info ) i on kh.id = i.sd_id
left join (select region_id,region_name,region_type from dwd_battery_o2o_region) a1 on i.receiver_province = a1.region_id and a1.region_type =1
left join (select region_id,region_name,region_type from dwd_battery_o2o_region) a2 on i.receiver_city = a2.region_id 
left join (select distinct order_id, goods_id, goods_name from dwd_battery_o2o_order_goods where goods_name like '%风帆%' ) g on g.order_id = i.order_id
where g.order_id is null
)
, t2 as (
select 
    row_number() over() id, -- 主键Id
    online_channel, -- 线上渠道
    year,   -- 年
    month,  -- 月
    week,   -- 周
    `date`, -- 日期
    province, -- 省
    city,   -- 市    
    count(deal_code) order_num,    -- 订单量
    round(sum(payment),4) order_amt,    -- 订单金额
    max('${bdp.system.bizdate}') update_date    -- 数据更新点
from t1 
where rn = 1 
group by online_channel, year, month, week, `date`,province,city
) insert overwrite table ads_battery_o2o_online_sales partition(ds = ${bdp.system.bizdate})
select 
    t2.id, -- 主键Id
    t2.online_channel, -- 线上渠道
    t2.year,   -- 年
    t2.month,  -- 月
    t2.week,   -- 周
    t2.`date`, -- 日期 
    t2.province, -- 省
    t2.city,    -- 市
    tmp.target_order_num, -- 目标订单量（模拟）   
    t2.order_num,    -- 订单量
    t2.order_amt,    -- 订单金额
    t2.update_date    -- 数据更新点
from t2 
left join ads_battery_o2o_target_order_num_tmp tmp on tmp.online_channel = t2.online_channel and tmp.month = t2.month and tmp.year = t2.year
