--********************************************************************--
--所属主题: 产品域
--功能描述: FEPAA——货币解析
--创建者:王兆翔
--创建日期:2023/08/29
--修改日期  修改人  修改内容
--yyyymmdd  name  comment
--********************************************************************--
DROP TABLE IF EXISTS ods_fepaa_currency_lc_df;
CREATE TABLE IF NOT EXISTS ods_fepaa_currency_lc_df(
    material_number             STRING COMMENT '料号'   
    ,calendar_year_month        STRING COMMENT '年月'     
    ,year                       STRING COMMENT '年'   
    ,month                      STRING COMMENT '月'     
    ,fepaa                      STRING COMMENT '货币值'     
    ,first_entry_currency       STRING COMMENT '币种'     
    ,is_valid                   STRING COMMENT '是否核验 1-是 2-否'     
    ,etl_load_time              STRING COMMENT '数据加载时间'
) COMMENT 'FEPAA LC币种解析表'          
PARTITIONED by(pday STRING COMMENT '时间分区') STORED AS parquet LOCATION 'boschfs://boschfs/warehouse/ods/ods_fepaa_currency_lc_df';