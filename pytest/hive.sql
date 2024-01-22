--********************************************************************--
--所属主题: 车主域
--功能描述: 数据小程序_来源渠道_数据改造5
--创建者:王兆翔
--创建日期:2023-04-10
--修改日期  修改人  修改内容
--yyyymmdd  name  comment
--********************************************************************--
-- 设置动态分区 压缩等参数
-- 分区
SET hive.exec.dynamic.partition = TRUE;
SET hive.exec.dynamic.partition.mode = nonstrict;
-- 分区参数设置
SET hive.exec.max.dynamic.partitions.pernode = 10000;
SET hive.exec.max.dynamic.partitions = 100000;
SET hive.exec.max.created.files = 150000;
-- hive压缩
SET hive.exec.compress.intermediate = TRUE;
SET hive.exec.compress.output = TRUE;
-- 每个容器设置10G大小
SET hive.tez.container.size = 10240;
-- Map join
set hive.auto.convert.join=true
set hive.auto.convert.join.noconditionaltask.size=512000000
select version();
-- 计算引擎Spark
-- set hive.execution.engine=spark;
select count(*) from ods_logistics_cost_outbound_domestic_mf 

drop table ods_base_area_info_df;
drop table ods_extra_base_area_info_df;
select * from ods_extra_base_area_info_df



select * from ods_extra_dbo_client_info_df where ds = 20240114 and work_chain_id is not null
select * from dim_wks_extra_workshop_master_data where ds = 20240115 and workshop_chain_name is not null

select * from 
ods_logistics_cost_outbound_domestic_mf limit 10

alter table dim_wks_extra_workshop_master_data add columns (workshop_chain_name string comment '')

select  'broker_onforwarding_cal' as topic, collect_set(country) country
,count(*) `rows`  from ods_logistics_cost_broker_onforwarding_cal_mf where pmonth = 202312
union all
select  'inbound_intercontinental' as topic, collect_set(country) 
,count(*) `rows`  from ods_logistics_cost_inbound_intercontinental_mf where pmonth = 202312
union all
select  'inbound_domestics' as topic, collect_set(country) 
,count(*) `rows`  from ods_logistics_cost_inbound_domestics_mf where pmonth = 202312
union all
select  'outbound_domestic' as topic, collect_set(country) 
,count(*) `rows`  from ods_logistics_cost_outbound_domestic_mf where pmonth = 202312

select max(job_id) from
dim_pub_job_basics_info

select max(task_id) from
dim_pub_task_dependency_info

desc dim_pub_task_dependency_info
select * from ods_extra_dbo_client_info_df where ds = 20240111 and work_chain_id is not null
alter table ods_extra_dbo_client_info_df add columns (
    work_chain_id string comment '',double_show_tag_id string comment '')

select * from ods_extra_dbo_client_info_df where ds = 20240111 and work_chain_id is not null
select * from  ods_extra_dbo_work_chain_df  where ds = 20240111



1152	task_dim_bosch_coverage_lid_df
1153	task_dwd_kindle_coverage_ana_with_pcd_status_df
1154	task_dwd_wks_coverage_analysis_sum_df
1155	task_dws_covered_product_line_pcd_na_df
1156	task_dws_wks_coverage_analysis_uncovered_levelid_df
1157	task_ads_uncovered_OE_list_mf

insert into dim_pub_task_dependency_info  values (1157,'task_ads_uncovered_OE_list_mf',0,array('0/1156'),array('task_dws_wks_coverage_analysis_uncovered_levelid_df/task_imp_spiderb_d'),'ADS',20231211,'wangzhaoxiang',date_format(CURRENT_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss'));
insert into dim_pub_task_dependency_info  values (1158,'task_stg_dealer_erp_sellout_118000652_df',1,array('0'),array(''),'STG',20231211,'wangzhaoxiang',date_format(CURRENT_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss'));
insert into dim_pub_task_dependency_info  values (1159,'task_stg_dealer_erp_stock_118000652_df',1,array('0'),array(''),'STG',20231211,'wangzhaoxiang',date_format(CURRENT_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss'));
insert into dim_pub_task_dependency_info  values (1160,'task_ods_dealer_erp_sellout_118000652_df',1,array('1158'),array('task_stg_dealer_erp_sellout_118000652_df'),'ODS',20231211,'wangzhaoxiang',date_format(CURRENT_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss'));
insert into dim_pub_task_dependency_info  values (1161,'task_ods_dealer_erp_stock_118000652_df',1,array('1159'),array('task_stg_dealer_erp_stock_118000652_df'),'ODS',20231211,'wangzhaoxiang',date_format(CURRENT_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss'));


1158	task_stg_dealer_erp_sellout_118000652_df
1159	task_stg_dealer_erp_stock_118000652_df
1160	task_ods_dealer_erp_sellout_118000652_df
1161	task_ods_dealer_erp_stock_118000652_df

insert into dim_pub_job_basics_info  values (10186,'job_stg_dealer_erp_sellout_118000652_df',2,'接入康众售出数据',1,1158,'task_stg_dealer_erp_sellout_118000652_df','STG',20231211,'full','wangzhaoxiang',date_format(CURRENT_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss'));
insert into dim_pub_job_basics_info  values (10187,'job_stg_dealer_erp_stock_118000652_df',2,'接入康众库存数据',1,1159,'task_stg_dealer_erp_stock_118000652_df','STG',20231211,'full','wangzhaoxiang',date_format(CURRENT_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss'));
insert into dim_pub_job_basics_info  values (10188,'job_ods_dealer_erp_sellout_118000652_df',1,'康众售出数据merge',1,1160,'task_ods_dealer_erp_sellout_118000652_df','ODS',20231211,'full','wangzhaoxiang',date_format(CURRENT_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss'));
insert into dim_pub_job_basics_info  values (10189,'job_ods_dealer_erp_stock_118000652_df',1,'将康众每日数据拉到ODS',1,1161,'task_ods_dealer_erp_stock_118000652_df','ODS',20231211,'full','wangzhaoxiang',date_format(CURRENT_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss'));















select count(*) from stg_dealer_erp_stock_118000652_df where ds = 20231210
select count(*) from stg_dealer_erp_stock_118000652_df where ds = 20231210


show partitions ods_dealer_erp_stock_118000652_df

DROP TABLE IF EXISTS test_spark;
CREATE TABLE IF NOT EXISTS test_spark(
    id STRING COMMENT 'PK',
    record_id INT COMMENT '订单记录ID',
    order_number STRING COMMENT '开思订单号',
    create_date TIMESTAMP COMMENT '创建时间',
    order_time TIMESTAMP COMMENT '创建时间',
    from_app STRING COMMENT '来源app',
    shop_name string comment 'shop_name',
    short_name string comment 'short_name',
    client_id INT COMMENT '客户ID',
    detail_id INT COMMENT 'detail_id',
    product_code STRING COMMENT '产品料号',
    product_code_handle string comment '处理后产品料号',
    oe_no STRING COMMENT '商品OE号',
    product_name STRING COMMENT '产品名称',
    order_type int COMMENT 'order_type',
    product_qty INT COMMENT '商品数量',
    point_value INT COMMENT '奖励积分值',
    product_amount STRING COMMENT '商品实付小计金额',
    category1_name STRING COMMENT '一级品类名称',
    category2_name STRING COMMENT '二级品类名称',
    category3_name STRING COMMENT '三级品类名称',
    external_client_id String COMMENT '外部客户id',
    boschpartno STRING COMMENT '博世料号'
) COMMENT 'dws_del_iam_dealer_sellout_df_tmp03'
PARTITIONED BY (ds STRING COMMENT '时间分区') STORED AS parquet LOCATION 'boschfs://boschfs/warehouse/spark';

insert overwrite table test_spark partition(ds=20231105)
SELECT id, record_id, order_number, create_date, order_time, from_app, shop_name, short_name, client_id, detail_id, product_code, product_code_handle, oe_no, product_name, order_type, product_qty, point_value, product_amount, category1_name, category2_name, category3_name, external_client_id, boschpartno FROM dws_del_iam_dealer_sellout_df_tmp03SELECT id, record_id, order_number, create_date, order_time, from_app, shop_name, short_name, client_id, detail_id, product_code, product_code_handle, oe_no, product_name, order_type, product_qty, point_value, product_amount, category1_name, category2_name, category3_name, external_client_id, boschpartno FROM dws_del_iam_dealer_sellout_df_tmp03
limit 1


with t1 as (select vin as car
from dws_wks_service_record_smy_df 
where ds='20231205') select count(car) from t1

select count(*) from ads_battery_o2o_return_statistic_df where netpoint_id is null

select sku_name,pd_category,pd_unit,pd_brand,material_code,
              pack_date,customer_code,customer_name,delivery_number,
              delivery_item,out_date,out_pack_code,smart_code_rel_detail,mark
from ads_lgs_dealer_interface_d  where ds = 20231030 mark = \'N\'

select * from dwd_latest_dealer_stock_di where ds = 20231026 and customercode = 118000940
select substring('20230429',4,2)

INSERT OVERWRITE TABLE dwd_dealer_stock_history_df PARTITION (ds = '20231029')
SELECT customercode, warehousename, warehouseno, boschpartno, boschpartno13, productname, productcategory, stockqty, unit, loaddate, loadtime, boschpartno_verified FROM dwd_dealer_stock_history_df
where (customercode != '118000131' and ds = 20231029) or (customercode = '118000131' and ds = 20231029 and cast(substring(loaddate, 5,2) as int) not in (4,5,6,7,8,9))

insert into table dwd_dealer_stock_history_df partition(ds = '${bdp.system.bizdate}')
SELECT customercode, warehousename, warehouseno, boschpartno, boschpartno13, productname, productcategory, stockqty, unit, loaddate, loadtime, boschpartno_verified FROM dwd_dealer_stock_history_df
where customercode = '118000131' and ds = 20231028 and cast(loaddate as int) > 20230910 and cast(loaddate as int) <20231001



select count(*) from ods_azure_blob_dealer_stock_118000131_history_fill_df where cast(substring(substring(regexp_replace(loaddate,'[^0-9]',''),1,8), 5,2) as int) in (4,5,6,7,8,9)
select count(*) from dwd_dealer_stock_history_df where customercode='118000131' and cast(substring(substring(regexp_replace(loaddate,'[^0-9]',''),1,8), 5,2) as int) in (4,5,6,7,8,9)

drop table ods_extra_client_identity_config_df

CREATE EXTERNAL TABLE ods_extra_client_identity_config_df (
    `id` INT,
    `client_type` STRING COMMENT '客户类型',
    `typeof` STRING COMMENT '客户类别',
    `details_type` STRING COMMENT '细分类型',
    `details_typeof` STRING COMMENT '细分类别',
    `showtag` STRING COMMENT '展示标签',
    `power` STRING COMMENT '权限',
    `banner_service_ids_str` STRING COMMENT '展示BANNER',
    `button1_service_ids_str` STRING COMMENT '展示ICON',
    `button2_service_ids_str` STRING COMMENT '展示BUTTON',
    `identity_val` INT COMMENT '显示ICON设置',
    `sub_identity_val` INT COMMENT '1:GBB 2:GCP'
    
) COMMENT '客户信息配置表'
PARTITIONED BY (`ds` STRING COMMENT 'yyyymmdd') STORED AS PARQUET LOCATION 'hdfs://nameservice1/warehouse/tablespace/external/hive/boschpro.db/ods_extra_client_identity_config_df'





drop table dwd_lgs_turnover_df_tmp
create table dwd_lgs_turnover_df_tmp stored as parquet location 'boschfs://boschfs/warehouse/dwd_lgs_turnover_df_tmp' as
select * from dwd_lgs_turnover_df where ds = 20231012 and sales_org in('CN20', 'CN21') and  sales_chl = 'G1'


select * from ads_del_iam_dealer_sellout_price_df where client_id = 44810
-- return statistic
select * from ads_battery_o2o_return_statistic_df where netpoint_id = 212
select *
from (select 'abC' a) a
join (select 'ABC' a) b
on upper(a.a) = b.a
-- winodws test
with t1 as (
    select 1 a,2 b, 3 c,4 d
    union all
    select 2 a,2 b,2 c,2 d
    union all
    select 3 a,3 b,3 c,3 d
)

select 
    -- count(*),
    count(1),
    count(a) over(partition by c)
from t1
group by b


select count(*) from ads_dealer_erp_workshop_master_mapping_df
 select 
        -- count(*)
        client_name  
        ,client_code   
        ,contact_phone 
        ,detail_address
        ,client_id
        ,row_number() over(partition by client_name  
        ,client_code   
        ,contact_phone 
        ,detail_address
        ,client_id) rn
from dim_wks_extra_workshop_master_data
where ds = ${bdp.system.bizdate} and active_status=1 
-- and client_id not in (select distinct client_id c1 from ods_extra_dbo_client_external_entity_df where ds = ${bdp.system.bizdate}) 
) a 
where rn > 1
select 
*
from (select  
workshop_name,   
erp_workshopid,  
contact_mobile,  
workshop_address,
from_app  
from dwd_del_dealer_erp_workshop_master_df where pday = ${bdp.system.bizdate} and workshop_address rlike '[\u4e00-\u9fa5]+') a
left join 
        (select distinct external_entity,external_app from ods_extra_dbo_client_external_entity_df where ds = ${bdp.system.bizdate} and external_app like '11800%' ) b
on a.erp_workshopid = b.external_entity and a.from_app = b.external_app and a.erp_workshopid is not null
where b.external_entity is not null 

desc ods_extra_dbo_client_external_entity_df
select distinct external_app  from ods_extra_dbo_client_external_entity_df where ds =${bdp.system.bizdate}
select count(*) from (
select distinct external_entity_id,client_id,external_app  from ods_extra_dbo_client_external_entity_df where ds =${bdp.system.bizdate}
) a
select * from stg_pig_region_turnover_mid
select count(*) from ods_oms_order_tags
select count(*) from dwd_dealer_stock_history_df where ds = 20230830
select count(*) from ads_lgs_dealer_interface_d where ds = 20230826
select count(*) from stg_fepaa_currency_lc_di where pday = 20230907
select count(*) from stg_fepaa_currency_gc_di where pday = 20230907

select * from dim_pub_job_basics_info where job_name = 'job_stg_prd_product_suggested_retail_price_df'

-- sellout 
select * from dim_prd_product where ds = 20230824
select * from dim_prd_product_suggested_retail_price_df

select count(*) from ads_stock_visualization_dealer_stock_history_df where ds = 20230815

show tables in boschpro like 'ods_mau_dealer_erp_workshop_master_*_df'
select from_app from dwd_del_dealer_erp_workshop_master_df where pday = 20230924 group by from_app
-- smart code 

SELECT
    *
FROM dwd_lgs_smart_code_info_df
WHERE ds = '${bdp.system.bizdate}' 
AND (sernr_main like '0000000000000%' or sernr_main like 'HTTP%') --特定的规则，即为顶层码
or (select * from )
)

select * from dwd_lgs_smart_code_binding_rltnp_df_test where ds = 20230925 and min_smart_code_level > 3



select * from dwd_delivery_header_cn_df where ds = ${bdp.system.bizdate}
select * from  dwd_lgs_smart_code_info_df where ds = ${bdp.system.bizdate} and matnr_sub = '0092S47332WRE'
select * from  dwd_lgs_smart_code_info_df_test where ds = ${bdp.system.bizdate} and vbeln = '3051521117'
select * from  dwd_lgs_smart_code_binding_rltnp_df where ds = ${bdp.system.bizdate} and min_smart_code like 'HTTP%' and max_level =3
-- tmp_test
select * 
from dwd_lgs_smart_code_info_df
where ds = '20230925' and sernr_sub in ('0092S67919KZ8_1_20221029123437360_97955781_D1')

select * 
from dwd_lgs_smart_code_binding_rltnp_df_test
where ds = '20230925' and sernr_sub in ('0092S67919KZ8_1_20221029123437360_97955781_D1')


select *
from  dwd_lgs_smart_code_binding_rltnp_df_test
where ds = '20230925' and 
primary_code in  ('Q8YMK2FHR0LNV8VWCD','1023017002')



select *
from  temp_dwd_las_smart_code_first_code
where first_code in ('Q8YMK2FHR0LNV8VWCD','1023017002') 

select *
from  temp_dwd_lgs_smart_code_binding_rltnp_df_01
where min_smart_code in ('0092S67919KZ8_1_20221029123437360_97955781_D1') 

SELECT
    sernr_sub AS min_smart_code --最小层级编码
    ,sernr_sub AS primary_code --一级编码
    ,NULL AS secondary_code --二级编码
    ,NULL AS level_three_code --三级编码
    ,NULL AS level_four_code --四级编码
    ,NULL AS level_five_code --五级编码
    ,NULL AS level_six_code --六级编码
    ,NULL AS level_seven_code --七级编码
    ,NULL AS level_eight_code --八级编码
    ,'1' AS min_smart_code_level --所在层级
    ,NULL AS smart_code_flow --编码流（一级层级无编码流）
    ,matnr_sub AS material_code --物料编码
FROM dwd_lgs_smart_code_info_df
WHERE ds = '${bdp.system.bizdate}' 
AND sernr_main = 'HTTPS://SQR.BOSCH-EXTRA.COM.CN/0092S67919KZ8_1_202'

-- dwd_lgs_smart_code_info_df
select * 
from dwd_lgs_smart_code_info_df
where ds = '20230808' and sernr_sub in ('0092S47326WRE_9_20221118192041306_97955781_10','0092S47326WRE_1_20221118191924088_97955781_10','E6B23FE2-7BED-4367-9F78-A8F8943FF50C')

select * 
from dwd_lgs_smart_code_info_df
where ds = '20230808' and sernr_sub in ('0000000000000_9_022620221019090135_00000000_00','0092S47372WRT_1_20221114070701214_97955781_10','154DC757-970B-4C6E-AD48-D18B1BB6BA35')

select * 
from dwd_lgs_smart_code_info_df
where ds = '20230924' and sernr_sub in ('HTTPS://SQR.BOSCH-EXTRA.COM.CN/0092S67919KZ8_1_202','0092S67919KZ8_1_20221029123437360_97955781_D1','87FD6389-DC23-4EBF-8311-365B25C9FB30')

with RECURSIVE t1 as (
    select 1
)select * from t1

WITH RECURSIVE cte as(
  SELECT sernr_sub, sernr_main, 1 AS level
  FROM dwd_lgs_smart_code_info_df
  WHERE ds = '20230924' and sernr_sub = '0092S47372WRT_1_20221114070701214_97955781_10'

  UNION ALL

  SELECT sernr_sub, sernr_main, cte.level + 1
  FROM dwd_lgs_smart_code_info_df t
  JOIN cte ON t.sernr_sub = cte.sernr_main
  WHERE t.parent_code IS NOT NULL
)
SELECT sernr_sub, sernr_main, level
FROM cte
WHERE sernr_main IS NULL;  -- 找到顶层码



-- dwd_lgs_smart_code_binding_rltnp_df

select *
from  dwd_lgs_smart_code_binding_rltnp_df_test
where ds = '20230813' and min_smart_code in ('0092S47326WRE_9_20221118192041306_97955781_10','0092S47326WRE_1_20221118191924088_97955781_10','E6B23FE2-7BED-4367-9F78-A8F8943FF50C')

select *
from  dwd_lgs_smart_code_binding_rltnp_df_test
where ds = '20230813' and min_smart_code in ('0000000000000_9_022620221019090135_00000000_00','0092S47372WRT_1_20221114070701214_97955781_10','154DC757-970B-4C6E-AD48-D18B1BB6BA35')


select * 
from dwd_lgs_smart_code_binding_rltnp_df_test
where ds = '20230925' and min_smart_code in ('HTTPS://SQR.BOSCH-EXTRA.COM.CN/0092S67919KZ8_1_202','0092S67919KZ8_1_20221029123437360_97955781_D1','87FD6389-DC23-4EBF-8311-365B25C9FB30')

-- 0818
drop table smart_code_check
create table tmp_smart_code_check(
    smart_code string
)






select * from tmp_smart_code_check;



--0817

select * 
from dwd_lgs_smart_code_binding_rltnp_df_test
where ds = '20230813' and min_smart_code in ('HTTPS://QR.BOSCH-EXTRA.COM.CN/GTN380X8RNCNPFM8MD','0092S67920KZ8_1_20230511194522388_97955781_D1','8088B54B-5D35-4C0F-8D48-67AF02209885')


select * 
from dwd_lgs_smart_code_info_df
where ds = '20230816' and sernr_sub in ('339cd7dc-a336-4736-af43-8e7d02348498')

select * from ods_smartcode_rbi2_yw_srn_df where ds = '20230813' and vbeln='3051515597' and posnr = '000180'
select count(*) from ods_smartcode_rbi2_yw_srn_df where ds = '20230816'

select * 
from dwd_lgs_smart_code_binding_rltnp_df
where ds = '20230816' and min_smart_code in ('7e7e8bfd-076b-4888-84eb-df9abc94e02d')

select * from ads_lgs_extra_interface_d where ds = 20230816 and min_smart_code = '8f1164c4-b2ec-460f-95b1-53c7b7825787'





select * from ads_lgs_extra_interface_df_test where min_smart_code
in (
    '8088B54B-5D35-4C0F-8D48-67AF02209885'
)

select * from ads_lgs_extra_smart_code_packcode_d where ds = 20230816 and pack_code ='0000000000000_9_022620221019090135_00000000_00'


select * from (select * from dwd_lgs_smart_code_info_df where ds = 20230816) a
join (select upper(smart_code) smart_code from tmp_smart_code_check) b on a.sernr_sub=b.smart_code
union all
select * from (select * from dwd_lgs_smart_code_info_df where ds = 20230816) a
join (select smart_code from tmp_smart_code_check) b on a.sernr_sub=b.smart_code







select * from ads_lgs_extra_smart_code_packcode_d_test where pack_code ='0000000000000_9_022620221019090135_00000000_00'



-- dealer workshop master data
select * from ods_mau_dealer_erp_workshop_master_118000141_df
select * from ods_extra_dbo_client_external_entity_df where ds = 20230730 and external_app like '118%'

select * from ads_dealer_erp_workshop_master_mapping_df where pday = 20230924
alter table ads_dealer_erp_workshop_master_mapping_df drop partition(pday=20230719)
select * from dim_wks_extra_workshop_master_data where ds = 20230723 and active_status = 1
select count(distinct reference_id) from dim_pub_standard_address_mf
 
-- min 小程序
select * from ads_car_driver_mini_program_user_source_d;
select * from ods_oms_jingdong_order_df;
select * from ods_oms_taobao_trade_df;
select * from ads_car_driver_mini_program_channel_mapping_d;
select * from ads_car_driver_mini_program_channel_fission_table_d;
select * from ads_del_iam_dealer_sellout_df where ds = ${bdp.system.bizdate}
select count(customer_id) from ads_mini_user_source_d

drop table tmp_test;
create table tmp_test2(
    id int,
    name string,
    num int,
    rn int
)
regexp_replace(regexp_replace(name, '\\|', '/'),'[^a-zA-Z0-9/]','')
insert into tmp_test values (3,'add|bv*db/daad',1)

from tmp_test
insert overwrite table tmp_test2 
select id, name, num,row_number() over(partition by id,name,num) rn
-- lateral view explode(split(regexp_replace(regexp_replace(name, '\\|', '/'),'[^a-zA-Z0-9/]',''), '/')) exploded_table as a
from tmp_test
where id = 1

select * from tmp_test2;

select regexp_replace('ddsfs:|dfsfd/dfsff222','[^a-zA-Z0-9|/]','')
select (datediff('2023-03-31','2022-05-18') - datediff('2023-03-31','2023-03-21') ) / datediff('2023-03-31','2022-05-18') * 100 * 0.1

show databases

-- battery o2o
SELECT * FROM dwd_battery_o2o_cangku where province_id is null and city_id is not null
select * from ads_battery_o2o_online_sales
select * from ods_oms_cangku_in_area_df
select last_day(concat('2023-',cast(5 as string) , '-01'))
select date(current_date)
select * from ods_oms_order_info_df where deal_code = '274935201608'
select * from ods_o2o_sxonsale_netpointinstallwo_df where orderinfoid = 35179

select * from ads_battery_o2o_service_quality where ds = 20230710

select * from ads_battery_o2o_service_quality where netpoint_name = '速电' 


-- 网点覆盖
select * from ads_battery_o2o_network_coverage where update_date = 20230624
insert overwrite table ads_battery_o2o_network_coverage
select id, province, city, year, month, week, city_num, netpoint_num, update_date ,vehicle_population,null,null,null,null 
from ads_battery_o2o_network_coverage_tmp where update_date != 20230624
drop table ads_battery_o2o_network_coverage_tmp
alter table ads_battery_o2o_network_coverage change column soa soa string comment '订单渗透率'

select * from ads_battery_o2o_online_sales where city is null
select * from ads_battery_o2o_netpoint  
select * from ads_battery_o2o_sku where goods_id = 107
select * from ods_oms_region_df
select distinct goods_id, goods_name,series_id from ods_oms_goods_df
select * from ads_battery_o2o_target_order_num_tmp




SELECT handleinterval, appointinterval, serviceinterval, approveinterval FROM dwd_battery_o2o_sxonsale_netpointinstallwo

select address from dwd_battery_o2o_cangku where address like '%\n'

SELECT id, year, month, week, province, city, district, netpoint_id, netpoint_name,replace(address,'/n','') address, dispatch_order_num, return_order_num, return_order_rate, avg_order_time, avg_install_time, ontime_service_rate, ontime_appointment_time, within_2_hour_order_num, within_2_hour_rate, undate_date FROM ads_battery_o2o_service_quality where id = 2801

select * from dwd_battery_o2o_sxonsale_netpointinstallwo where netpointid =1
select province_id,city_id,district, id,ckmc,address from dwd_battery_o2o_cangku where id in (select netpointid from dwd_battery_o2o_sxonsale_netpointinstallwo ) and id =60
select * from dwd_battery_o2o_sxonsale_netpointinstallwo where netpointid != 1 and netpointid =60

SELECT bx_dlr_id, bx_dlr_name, is_dealers, parent_distributor_id 
FROM dim_del_bx_dealer
WHERE
ds = '${bdp.system.bizdate}';

set hive.fetch.task.conversion
set  hive.exec.dynamic.partition.mode;

show databases;
show tables in boschpro

show versions

truncate table dwd_azure_blob_auto_dealer_stock_df

drop table dwd_azure_blob_auto_dealer_stock_df

alter table ads_azure_blob_month_auto_dealer_stock_df rename to dwd_azure_blob_month_auto_dealer_stock_df_tmp

SELECT distinct shop_name from  ads_del_iam_dealer_sellout_df where ds = 20230521
-- py
select count(*) from dim_wks_vin_level_id where ds = 20230627

select * from ods_azure_blob_dealer_stock_118006073_df
select * from dwd_azure_blob_month_auto_dealer_stock_df_tmp

select * from ads_battery_o2o_network_coverage where province in ('重庆市','天津市','北京市','上海市')

select * from ods_azure_blob_auto_dealer_stock_118005361_df where ds = 20230517

select * from ads_azure_blob_month_auto_dealer_stock_df where tmpid != 'nan'

select * from dws_del_iam_dealer_sellout_df_tmp03 where ds = 20230523

select * from dws_del_iam_dealer_sellout_df_tmp03 where ds = 20230523

select * from ads_bx_core_order_status_percentage_df


select * from dwd_azure_blob_month_auto_dealer_stock_df_tmp where kehu like '%:%' or kehu like '%.%'

truncate table dwd_azure_blob_month_auto_dealer_stock_df_tmp 

drop table dwd_azure_blob_month_auto_dealer_stock_df_tmp_1
select count(*) from dwd_azure_blob_month_auto_dealer_stock_df_tmp
select count(*) from dwd_azure_blob_auto_dealer_stock_df

select * from dwd_azure_blob_auto_dealer_stock_df where ds = 20230529 and kehu = '118002143'

drop table dwd_azure_blob_auto_dealer_stock_df WHERE kehu = '118002143'
select * from dwd_azure_blob_month_auto_dealer_stock_df_tmp where kehu NOT IN (
select distinct kehu from dwd_azure_blob_month_auto_dealer_stock_df_tmp) 

with t1 as (
select 1 a,2 b,3 c,4 d
union all
select 2 a,9999 b,4 c,4 d
union all
select 2 a,3 b,4 c,5 d
union all
select 3 a,3 b,4 c,5 d
)
select 
count(distinct if(b != 3, a,null))
-- avg(a-b)
from t1 

drop table atest

select count(*) from dwd_azure_blob_auto_dealer_stock_df where ds = 20230529


-- 时间
select '2023-4' <'2023-04-30 00:00:00' < '2023-05'
select unix_timestamp(date_format('2023-05-06 01:02:22','yyyy-MM-dd HH:mm:ss')) - unix_timestamp(date_format('2023-05-06 00:02:22','yyyy-MM-dd HH:mm:ss'))
select add_months(current_date, -3)
select datediff(current_date,'2022-04-25 01:02:22')
select substring('2023-04-30 12:00:00',12,2)
select concat_ws('-' ,substring('20230506',1,4),substring('20230506',5,2),substring('20230506',7,2))


select regexp_replace(loaddate,'[^0-9]','') loaddate from dwd_azure_blob_auto_dealer_stock_df  group by regexp_replace(loaddate,'[^0-9]','') order by loaddate
select loaddate from dwd_azure_blob_auto_dealer_stock_df  group by loaddate order by loaddate
select regexp_replace(loaddate,'[^0-9]','') loaddate from dwd_azure_blob_auto_dealer_stock_df  group by regexp_replace(loaddate,'[^0-9]','')  order by loaddate


select loaddate from dwd_dealer_stock_history_df  group by loaddate order by loaddate


select count(*) from dwd_dealer_stock_history_df
select count(*) from dwd_azure_blob_auto_dealer_stock_df

select 
    count(distinct a.recycling_order_id)
from ads_bx_core_recycling_order_info a
join ads_bx_core_info b
on a.recycling_order_id = b.accessories_recycling_id

select 
    count(distinct recycling_order_id) from ads_bx_core_recycling_order_info 

select 
count(distinct accessories_recycling_id) from ads_bx_core_info 

select * from dwd_dealer_stock_history_df where ds = 20230615 and customercode = 118002298 and loaddate = 20230613

select count(*) from ods_wsms_tb_partscategory_df where ds = 20230615
select * from dim_battery_o2o_netpoint_type_df

alter table ods_extra_dbo_client_info_df add columns (
    client_identify_config_id string comment '用于识别客户的唯一编号',client_bss_identify_config_id string comment '用于识别客户的唯一编号')

select receiver_province, receiver_city from dwd_battery_o2o_order_info limit 10

select * from dws_wks_bss_seg_pd_temp_all

SELECT pd_line, pd_line_name, cbf, cbf_desc, city_name, final_seg, average_pd_qty FROM dws_wks_bss_seg_pd_temp_all
select lastchanged from dwd_battery_o2o_order_info

create table stresstest.tmp_test(
    id int,
    name string
)

show tables in stresstest
describe database boschpro;
describe extended tmp_test_01;
describe extended ads_del_sec_pltm_oe_inqr_m;

select a.order_number,a.shop_name, b.search_term from (select order_number, shop_name from dwd_del_casstime_order_df where ds = 20230711) a 
join (select distinct search_term from dim_del_dealer where ds = 20230711 and prtn_fnc = 'AG' and addr_ver = "C") b 
on a.shop_name LIKE concat('%', substring(b.search_term,1,2), '%') and a.shop_name LIKE concat('%', substring(b.search_term,-1,4), '%')


