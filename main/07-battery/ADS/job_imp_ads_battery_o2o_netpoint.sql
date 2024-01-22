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
insert overwrite table ads_battery_o2o_netpoint partition(ds = ${bdp.system.bizdate})
select
    row_number() over() id,     -- 主键id
    scn.scn_region,             -- SCN大区
    a1.region_name province,    -- 省份
    a2.region_name city,        -- 城市
    a3.region_name district,    -- 地区
    id netpoint_id ,            -- 网点id
    ckmc netpoint_name,         -- 网点名
    case when a4.region_type=1 then a4.region_name
         when a4.region_type=2 and a6.region_type =1 then a6.region_name
         when a4.region_type=3 and a6.region_type =1 then a6.region_name
    else a7.region_name
    end service_province,       -- 可履约省份
    case when a4.region_type=2 then a4.region_name 
         when a6.region_type =2 then a6.region_name
         when a4.region_type=3 and a6.region_type = 1 then a4.region_name
    else null
    end service_city,           -- 可履约城市
    case when a4.region_type =3 then a5.region_name
    else null 
    end service_district,       -- 可履约区县
    ty status,                  -- 网点状态
    '${bdp.system.bizdate}' update_date     -- 更新时间
from (select province_id,city_id,district, id,ckmc,address,ty from dwd_battery_o2o_cangku) c
    -- 所属区域
    left join (select region_id,region_name,region_type from dwd_battery_o2o_region) a1 on c.province_id = a1.region_id and a1.region_type =1
    left join (select region_id,region_name,region_type from dwd_battery_o2o_region) a2 on c.city_id = a2.region_id and a2.region_type = 2
    left join (select region_id,region_name,region_type from dwd_battery_o2o_region) a3 on c.district = a3.region_id and a3.region_type = 3

    -- 可达区
    left join (select region_id,region_name,region_type,cangku_id from dwd_battery_o2o_cangku_in_area) a4 on c.id = a4.cangku_id 
    -- 可达区域(追溯)
    left join (select region_id,region_name,region_type, parent_id from dwd_battery_o2o_region) a5 on a4.region_id = a5.region_id
    left join (select region_id,region_name,region_type, parent_id from dwd_battery_o2o_region) a6 on a5.parent_id = a6.region_id 
    left join (select region_id,region_name,region_type from dwd_battery_o2o_region) a7 on a6.parent_id = a7.region_id and a7.region_type = 1
    -- SCN大区
    left join (select province,scn_region from ads_battery_o2o_scn_region) scn on a1.region_name = scn.province
group by a1.region_name, a2.region_name, a3.region_name, id, ckmc, a4.region_id, a4.region_name, ty, a7.region_name, a6.region_name, a5.region_name, a4.region_type, a6.region_type,scn.scn_region




