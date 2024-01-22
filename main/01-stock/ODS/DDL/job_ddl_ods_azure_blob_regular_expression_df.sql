DROP TABLE IF EXISTS ods_azure_blob_regular_expression_df;
CREATE TABLE IF NOT EXISTS ods_azure_blob_regular_expression_df(
    seq int COMMENT '优先级',
    regexp_str STRING COMMENT '正则',
    replace_str STRING COMMENT '替换内容'
) COMMENT 'regexp_rule'
PARTITIONED BY (ds STRING COMMENT '时间分区') STORED AS parquet LOCATION 'boschfs://boschfs/warehouse/azure_blob/ods_azure_blob_regular_expression_df';
