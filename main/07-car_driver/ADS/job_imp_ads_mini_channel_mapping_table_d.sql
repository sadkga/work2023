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
WITH t1 AS (
    -- 门店参数表
    SELECT
        distinct object_id,
        source
    FROM
        dwd_car_driver_mini_program_app_visit_record_log_cl
) INSERT OVERWRITE TABLE ads_car_driver_mini_program_channel_mapping_d 
SELECT
    row_number() OVER () source_id,
    -- ID
    tmp.source_id source,
    -- 来源渠道ID
    tmp.source_type,
    -- 渠道类型
    tmp.source_name,
    -- 渠道名称
    tmp.source_page,
    -- 来源页面
    tmp.source_activity,
    -- 来源活动
    concat(
        tmp.path,
        '?id=',
        t1.object_id,
        '&source=',
        tmp.source_id
    ) source_path -- 渠道页面路径
FROM
    ads_car_driver_mini_program_channel_tmp_d tmp
LEFT JOIN
    t1 ON tmp.source_id = t1.source



