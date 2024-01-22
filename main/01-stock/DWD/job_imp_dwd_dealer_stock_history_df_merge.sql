--********************************************************************--
--所属主题: 库存域
--功能描述: 进销商库存每日增量数据插入
--创建者: 王兆翔
--创建日期:2023-05-24
--********************************************************************--
set hive.tez.container.size=4096;

INSERT overwrite TABLE dwd_latest_dealer_stock_di PARTITION(ds = '${bdp.system.bizdate}')
SELECT 
    e.kehu AS customercode   -- 客户代码
    ,e.warehousename	    -- 仓库名称
    ,e.warehouseno	    -- 仓库编号
    ,e.boschpartno      -- 博世10位料号
    ,e.boschpartno13	    -- 博世13位料号
    ,e.productname	    -- 产品描述
    ,e.productcategory    -- 产品品类
    ,e.stockqty           -- 库存数量
    ,e.unit               -- 计量单位
    ,e.loaddate           -- 库存导出日期
    ,e.loadtime           -- 库存导出时间 
    ,m.boschpartno AS boschpartno_verified  -- 匹配过的博世10位料号
FROM (
        SELECT *
        , row_number() OVER(PARTITION BY kehu,warehousename,warehouseno,boschpartno,boschpartno13,productname,productcategory,stockqty,unit,loaddate,loadtime) rn
        FROM 
            ods_azure_blob_auto_dealer_stock_di
        WHERE ds='${bdp.system.bizdate}'
    ) e
LEFT JOIN dim_del_dealer_product_code_mapping_merge_df m
    ON m.customercode = e.kehu AND m.product_code = e.boschpartno AND m.ds='${bdp.system.bizdate}'
WHERE e.rn = 1