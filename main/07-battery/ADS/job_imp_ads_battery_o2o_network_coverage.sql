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
        a1.region_name province,  -- 省
        a2.region_name city,    -- 市
        count(distinct a2.region_name) over(partition by a1.region_name) city_num, -- 网点覆盖城市数
        count(distinct c.id) netpoints  -- 网点数量
    from (select province_id,city_id,id from dwd_battery_o2o_cangku where ty = 0) c
    left join (select region_id,region_name,region_type from dwd_battery_o2o_region) a1 on c.province_id = a1.region_id and a1.region_type =1
    left join (select region_id,region_name,region_type from dwd_battery_o2o_region) a2 on c.city_id = a2.region_id and a2.region_type = 2
    group by a1.region_name, a2.region_name
)
, t2 as (
    select
        r.region_name province,
        a.city,
        sum(population_2020) vehicle_population
    from (
        select distinct province,city,population_2020 from dwd_prd_dmst_pc_population_df
        union all 
        select distinct province,city,number population_2020 from dwd_prd_impr_pc_population_df
        union all
        select distinct province,city,population_2020 from dwd_prd_lcv_population_df

    ) a
    left join dwd_battery_o2o_region r
    on r.region_name like concat('%',a.province,'%') and r.region_type  =1
    group by r.region_name, a.city
) 
, t3 as (
    select 
        service_province,
        service_city,
        count(distinct netpoint_id) netpoint_num    -- 履约网点数量
    from ads_battery_o2o_netpoint 
    where status = 0 and netpoint_id != 109
    group by service_province, service_city
)
, t4 as (
    -- 地区每月派单量统计表
  select
        distinct c.province,
        c.city,
        o.year,
        o.month,
        count(distinct sn.id) over(partition by c.province,c.city) order_num,
        count(if(o.year = year(current_date), 1, null)) over(partition by c.province,c.city) year_order_num,
        count(distinct sn.id) over(partition by  c.province,c.city,o.year,o.month order by o.year, o.month) mon_num,
        count(distinct sn.id) over(partition by c.province,c.city,o.year order by o.year, o.month) sum_mon_num
    from (select id,netpointid, ordernumber from dwd_battery_o2o_sxonsale_netpointinstallwo) sn
-- 仓库地区    
right join (
 select
        distinct id,
        ckmc, 
        address,
        a1.region_name province,
        a2.region_name city,
        a3.region_name district
    from (select province_id,city_id,district, id,ckmc,address from dwd_battery_o2o_cangku where id != 109 ) c
left join (select region_id,region_name,region_type from dwd_battery_o2o_region) a1 on c.province_id = a1.region_id and a1.region_type =1
left join (select region_id,region_name,region_type from dwd_battery_o2o_region) a2 on c.city_id = a2.region_id and a2.region_type = 2
left join (select region_id,region_name,region_type from dwd_battery_o2o_region) a3 on c.district = a3.region_id and a3.region_type = 3
) c
on c.id = sn.netpointid
-- 订单信息
left join (
    select
    order_id, order_sn, from_unixtime(max(add_time), 'yyyy-MM-dd HH:mm:ss') add_time 
    ,from_unixtime(max(add_time), 'yyyy') year
    ,from_unixtime(max(add_time), 'MM') month
    from dwd_battery_o2o_order_info
    group by order_id, order_sn
) o on sn.ordernumber = o.order_sn
order by province,city,year,month
-- group by c.province, c.city, o.year, o.month
)
, t5 as (
select
    COALESCE(t1.province, t2.province) province,
    COALESCE(t1.city, t2.city) city,
    year(current_date()) year,  -- 年
    month(current_date()) month,    -- 月
    weekofyear(current_date()) week,    -- 周
    case when t1.province is null and t1.city is null then 0
         else t1.city_num
         end city_num,
    case when t1.province is null and t1.city is null then 0
         else t1.netpoints
         end netpoints,
    '${bdp.system.bizdate}' update_date,    -- 数据更新时间
    COALESCE(t2.vehicle_population,0) vehicle_population   -- 汽车保有量
from t1
full join t2 on t1.province = t2.province and t1.city = t2.city
)
, t6 as (
select 
    distinct t5.province
    ,t5.city
    ,t5.year
    ,t5.month
    ,t5.week
    ,t5.city_num
    ,coalesce(t3.netpoint_num, 0) netpoint_num      -- 网点数量,履约到该地区的总数
    ,t5.update_date
    ,t5.vehicle_population
    ,scn.scn_region   -- scn大区   
    ,coalesce(t4.year_order_num, 0) order_num    -- 订单数量
    ,cast(t5.month * 30 - coalesce(t4.year_order_num / t5.netpoints, 0) as decimal(18,2)) order_potenial
    ,coalesce(cast((t4.sum_mon_num-t4.mon_num) / (vehicle_population * 0.1*0.25*(if(t5.month-1<=0, 12+t5.month-1,t5.month-1))) as decimal(18,6)), '0') soa
from t5
left join ads_battery_o2o_scn_region scn on t5.province = scn.province
left join t4 on t4.province = t5.province and t4.city = t5.city and t4.year = t5.year and t4.month = t5.month
left join t3 on t3.service_province = t5.province and t3.service_city = t5.city 
)
insert into table ads_battery_o2o_network_coverage 
select 
    row_number() over() id, -- ID
    *
from t6




