
 -- @ Creater      : sadkga sadkga@88.com
 -- @ Since        : 2024-01-22 14:15:41
 -- @ LastAuthor   : sadkga sadkga@88.com
 -- @ LastTime     : 2024-01-22 14:16:16
 -- @ Location     : \\code\\main\\01-stock\\DIM\\job_trsf_dim_del_dealer_product_code_mapping_merge.py
 -- @ Message      : 
 -- @ Copyright (c) 2024 by sadkga@88.com, All Rights Reserved. 
set hive.auto.convert.join=false;
set hive.tez.container.size=9192;
set hive.tez.java.opts=-Xmx9192m;

DROP TABLE IF EXISTS tmp_aamm_gen_material_df;
CREATE TABLE IF NOT EXISTS tmp_aamm_gen_material_df AS
-- 全球物料匹配表
select
    material_10_digits,
    case
        when packaging_use = 'IAM' then 1
        when packaging_use = 'OES' then 2
        else 3
    end as packaging_use_order,
    case
        when cross_plant_material_status = '40' then 1
        when cross_plant_material_status = '49' then 2
        when cross_plant_material_status = '50' then 3
        else 4
    end as cross_plant_material_status_order
from (
        select
            rank() over(partition by material_10_digits, packaging_use, cross_plant_material_status) as group_distinct,
            material_10_digits,         -- 10位料号
            packaging_use,
            cross_plant_material_status
        from
            ods_mmhub_v_aamm_gen_material_df y -- 开思物料匹配表
            left semi join
            (select max(pday) pday from ods_mmhub_v_aamm_gen_material_df) o
        on y.pday = o.pday
        where y.material_10_digits is not null and length(trim(y.material_10_digits)) >= 10
        ) t
where
    group_distinct = 1;

DROP TABLE IF EXISTS tmp_ymtk00101_df;
CREATE TABLE IF NOT EXISTS tmp_ymtk00101_df AS
-- alternative number 通用料号匹配表
select
    t.material,
    REGEXP_REPLACE(t.comp_alt_no, '^0+|0+$', '') AS comp_alt_no,
    if(t1.packaging_use_order is null, 9, t1.packaging_use_order) as packaging_use_order,
    if(t1.cross_plant_material_status_order is null, 9, t1.cross_plant_material_status_order) as cross_plant_material_status_order
from
    (
        select
            material, --材料号
            comp_alt_no --比较选择料号
        from
            ods_azure_blob_v_ymtk00101_df y
            left semi join
            (select max(ds) ds from ods_azure_blob_v_ymtk00101_df) o
        on y.ds = o.ds
        where y.comp_alt_no <> '' and y.comp_alt_no is not null AND length(REGEXP_REPLACE(trim(y.comp_alt_no), '^0+|0+$', '')) > 5
            and y.material is not null and length(trim(y.material)) >= 10
        group by material, comp_alt_no
    ) t
left join tmp_aamm_gen_material_df t1
    on upper(t1.material_10_digits) = upper(t.material);


            
DROP TABLE IF EXISTS tmp_ymtk10001_cross_ref_info_df;
CREATE TABLE IF NOT EXISTS tmp_ymtk10001_cross_ref_info_df AS
-- cross reference 原厂编号匹配表
select
    distinct t.matnr,
                REGEXP_REPLACE(t.khnr_verd, '^0+|0+$', '') AS khnr_verd,
                if(t1.packaging_use_order is null, 9, t1.packaging_use_order) as packaging_use_order,
                if(t1.cross_plant_material_status_order is null, 9, t1.cross_plant_material_status_order) as cross_plant_material_status_order
from (
        select
            matnr, --料号
            khnr_verd   -- 原厂处理后
        from
            ods_azure_blob_ymtk10001_cross_ref_info_df y
            left semi join
            (select max(ds)+1 ds from ods_azure_blob_ymtk10001_cross_ref_info_df) o
        on y.ds = o.ds
        where y.khnr_verd <> '' and y.khnr_verd is not null AND length(REGEXP_REPLACE(trim(y.khnr_verd), '^0+|0+$', '')) > 5
            and y.matnr is not null and length(trim(y.matnr)) >= 10
        group by matnr, khnr_verd
    ) t
left join tmp_aamm_gen_material_df t1
    on upper(t1.material_10_digits) = upper(t.matnr);

with t4 as (
    select 
        distinct customercode,
        product_code,
        boschpartno
    from dwd_del_dealer_product_code_map_df
    where product_code <> '' and product_code is not null AND NOT product_code RLIKE '^[0]+$'
        and boschpartno is not null and length(trim(boschpartno)) >= 10
)

, t5 as(
    select 
        distinct
        t.customercode,
        t.product_code,
        product_code_spilt
    from (
        select customer_code as customercode, product_code 
        from dim_del_dealer_product_code_source_df 
        where pday='${bdp.system.bizdate}'
    ) t
    lateral view explode(split(regexp_replace(regexp_replace(t.product_code, '\\|', '/'),'[^a-zA-Z0-9/]',''), '/')) exploded_table as product_code_spilt
)

, t6 as (
    select *
    from (
        select 
            t5.product_code,
            t5.product_code_spilt,
            t5.customercode,
            case
                when t5.product_code_spilt regexp reg_rule.regexp_str then regexp_replace(t5.product_code_spilt, reg_rule.regexp_str, reg_rule.replace_str)
                else t5.product_code_spilt
            end as product_code_handle,
            row_number() over (partition by t5.customercode, t5.product_code, t5.product_code_spilt order by reg_rule.seq) rn
        from t5
        left join (
            select distinct seq, concat('(?i)', regexp_str) as regexp_str, replace_str
            from ods_azure_blob_regular_expression_df r
            left semi join (select max(ds) ds from ods_azure_blob_regular_expression_df) o
                on r.ds = o.ds
        ) reg_rule
            on t5.product_code_spilt regexp reg_rule.regexp_str
    ) t where t.rn = 1
)

-- select * from t6

, t7 as (
    select 
    * ,
    case 
        when length(t6.product_code_handle) > 6 then lpad(substr(t6.product_code_handle, 0, 10), 10, '0')
        else t6.product_code_handle
    end as product_code_handle_10_digits,
    REGEXP_REPLACE(t6.product_code_handle, '^0+|0+$', '') AS product_code_handle_trim0
    from t6
)

, t8 as (
    select
        t7.customercode,
        t7.product_code,
        case
            when t4.boschpartno is not null then t4.boschpartno
            when t1.material_10_digits is not null then t1.material_10_digits
            when t2.comp_alt_no is not null then t2.material
            when t3.khnr_verd is not null then t3.matnr
            else '0'
        end as boschpartno_verified,
        CASE
            WHEN t4.boschpartno is not null THEN 'dealer_product_code_mapping'
            WHEN t1.material_10_digits is not null THEN 'aamm_gen_material'
            WHEN t2.comp_alt_no is not null THEN 'ymtk00101'
            WHEN t3.khnr_verd is not null THEN 'ymtk10001_cross_ref_info'
        END AS from_table,
        CASE
            WHEN t4.boschpartno is not null THEN 1
            WHEN t1.material_10_digits is not null THEN t1.packaging_use_order
            WHEN t2.comp_alt_no is not null THEN t2.packaging_use_order
            WHEN t3.khnr_verd is not null THEN t3.packaging_use_order
            else 99
        END AS packaging_use_order,
        CASE
            WHEN t4.boschpartno is not null THEN 1
            WHEN t1.material_10_digits is not null THEN t1.cross_plant_material_status_order
            WHEN t2.comp_alt_no is not null THEN t2.cross_plant_material_status_order
            WHEN t3.khnr_verd is not null THEN t3.cross_plant_material_status_order
            else 99
        END AS cross_plant_material_status_order,
        CASE
            WHEN t4.boschpartno is not null THEN 1
            WHEN t1.material_10_digits is not null THEN 2
            WHEN t2.comp_alt_no is not null THEN 3
            WHEN t3.khnr_verd is not null THEN 4
            else 99
        END AS from_table_order
    from t7
    left join t4 ON t7.customercode = t4.customercode and upper(t7.product_code) = upper(t4.product_code)
    left join tmp_aamm_gen_material_df t1 ON upper(t7.product_code_handle_10_digits) = upper(t1.material_10_digits) and t4.product_code is null
    left join tmp_ymtk00101_df t2 ON upper(t7.product_code_handle_trim0) = upper(t2.comp_alt_no) and t4.product_code is null and t1.material_10_digits is null
    left join tmp_ymtk10001_cross_ref_info_df t3 ON upper(t7.product_code_handle_trim0) = upper(t3.khnr_verd) and t4.product_code is null and t1.material_10_digits is null and t2.comp_alt_no is null
)

-- select * from t8

, t9 as (
    select customercode, product_code, boschpartno_verified, from_table from (
        select
            *
            , row_number() over(partition by customercode, product_code 
            order by from_table_order, packaging_use_order, cross_plant_material_status_order, boschpartno_verified) rn
        from t8
    ) t where t.rn = 1
)

INSERT OVERWRITE TABLE dim_del_dealer_product_code_mapping_merge_df PARTITION(ds = ${bdp.system.bizdate})
select t11.*
    from (
    select 
        t9.customercode,
        t9.product_code,
        t9.boschpartno_verified as boschpartno,
        t9.from_table
    from t9
    where from_table = 'dealer_product_code_mapping'

    union all

    select 
        t.customercode,
        t.product_code,
        t10.boschpartno_verified as boschpartno,
        t10.from_table
    from t9 t
    left join (
        select 
            upper(product_code) as product_code,
            boschpartno_verified,
            from_table,
            row_number() over(partition by upper(product_code)) rn
        from t9
        where from_table != 'dealer_product_code_mapping' or from_table is null
    ) t10 on upper(t10.product_code) = upper(t.product_code) and t10.rn = 1
    where t.from_table != 'dealer_product_code_mapping' or t.from_table is null
) t11