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
insert overwrite table ads_battery_o2o_region
select
    case 
        when a5.region_type=3 and a6.region_type =1 then a6.region_name
        else a7.region_name
        end province,       -- 省份
    case 
        when a6.region_type =2 then a6.region_name
        when a5.region_type=3 and a6.region_type = 1 then a5.region_name
        else null
        end city,   -- 城市
    case
        when a5.region_type =3 then a5.region_name
        else null 
        end district   -- 区县
from  (select region_id,region_name,region_type, parent_id from dwd_battery_o2o_region where region_type =3) a5 

    -- 父级区域(追溯)   
    left join (select region_id,region_name,region_type, parent_id from dwd_battery_o2o_region) a6 on a5.parent_id = a6.region_id 
    left join (select region_id,region_name,region_type from dwd_battery_o2o_region) a7 on a6.parent_id = a7.region_id and a7.region_type = 1

