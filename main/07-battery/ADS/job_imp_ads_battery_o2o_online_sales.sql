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
INSERT OVERWRITE TABLE ads_battery_o2o_sku PARTITION(ds = ${bdp.system.bizdate})
select
    row_number() over() id, -- ID
    year(i.add_time) year,  -- 年
    month(i.add_time) month,    -- 月
    weekofyear(i.add_time) week,  -- 周   
    i.add_time `date`,  -- 日期
    case when kh.khdm = 'TM01' then '天猫'
        when kh.khdm = 'PACP001' then '平安'
        when kh.khdm = 'Baisoon001' then '百顺'
        when kh.khdm = 'JD01' or kh.khdm = 'JD02' then '京东'
        else '' 
        end channel,    
    n.netpointid netpoint_id,   -- 网点ID
    c.ckmc netpoint_name,   -- 网点名称
    s.series_name series_type,  -- 商品系列
    g.goods_id,     -- 商品ID
    g.goods_name,   -- 商品名称
    g.goods_number sold_num,    -- 卖出数量
    g.payment sold_amt,     -- 卖出金额
    '${bdp.system.bizdate}' update_date    -- 数据更新时间点
from (select order_id, goods_id, goods_name, goods_number, payment from dwd_battery_o2o_order_goods) g 
left join (select sd_id, from_unixtime(add_time, 'yyyy-MM-dd') add_time,order_id,fhck_id from dwd_battery_o2o_order_info) i on g.order_id = i.order_id
left join (select distinct id,khdm from dwd_battery_o2o_kehu where khdm in ('TM01','JD01','JD02','PACP001','Baisoon001')) kh on kh.id = i.sd_id
left join (select distinct orderinfoid,netpointid from dwd_battery_o2o_sxonsale_netpointinstallwo) n on n.orderinfoid = i.order_id
left join (select distinct id,ckmc from dwd_battery_o2o_cangku) c on c.id = i.fhck_id
left join (select distinct goods_id,series_id from dwd_battery_o2o_goods) gs on gs.goods_id = g.goods_id
join (select distinct series_id,series_name from dwd_battery_o2o_series) s on gs.series_id = s.series_id


