--********************************************************************--
--所属主题: 车主域
--功能描述: 数据小程序_来源渠道_数据改造
--创建者:王兆翔
--创建日期:2023-04-10
--修改日期  修改人  修改内容
--yyyymmdd  name  comment
--********************************************************************--
-- 设置动态分区 压缩等参数
--分区
SET hive.exec.dynamic.partition = TRUE;
SET hive.exec.dynamic.partition.mode = nonstrict;
--分区参数设置
SET hive.exec.max.dynamic.partitions.pernode = 10000;
SET hive.exec.max.dynamic.partitions = 100000;
SET hive.exec.max.created.files = 150000;
--hive压缩
SET hive.exec.compress.intermediate = TRUE;
SET hive.exec.compress.output = TRUE;
--写入时压缩生效
SET hive.exec.orc.compression.strategy = COMPRESSION;
-- 每个容器设置4G大小
SET hive.tez.container.size = 4096;
-- imp代码
WITH t1 AS (
    SELECT
        --  渠道mapping表
        source_type,
        count(source_type) rn
    FROM
        ads_car_driver_mini_program_channel_mapping_d
    GROUP BY
        source_type
),
t2 AS (
    SELECT
        tmp.source_id,
        tmp.source,
        tmp.source_type,
        tmp.source_name,
        t1.rn
    FROM
        ads_car_driver_mini_program_channel_mapping_d tmp
        JOIN t1 ON tmp.source_type = t1.source_type AND t1.rn = 1
) INSERT OVERWRITE TABLE ads_car_driver_mini_program_channel_fission_table_d 
SELECT
    t2.source_id,
    t2.source,
    t2.source_type,
    t2.source_name
FROM
    t2
