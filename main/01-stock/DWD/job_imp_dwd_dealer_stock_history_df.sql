--********************************************************************--
--所属主题: 库存域
--功能描述: 进销商库存本月数据及其他月月初月末数据
--创建者: 王兆翔
--创建日期:2023-06-07
--********************************************************************--
SET hive.tez.container.size = 4096;
INSERT OVERWRITE TABLE dwd_dealer_stock_history_df PARTITION (ds = '${bdp.system.bizdate}')
SELECT
    customercode,
    warehousename,
    warehouseno,
    boschpartno,
    boschpartno13,
    productname,
    productcategory,
    stockqty,
    unit,
    loaddate,
    loadtime,
    boschpartno_verified
FROM
    (
        SELECT
            *,
            row_number() OVER(
                PARTITION by customercode,
                warehousename,
                warehouseno,
                boschpartno,
                boschpartno13,
                productname,
                productcategory,
                stockqty,
                unit,
                loaddate,
                loadtime,
                boschpartno_verified
            ) rn
        FROM
            (
                SELECT
                    *
                FROM
                    dwd_dealer_stock_history_df
                WHERE
                    (ds = '${yyyyMMdd, -2d}')
                UNION ALL
                SELECT
                    *
                FROM
                    dwd_latest_dealer_stock_di
                WHERE
                    (ds = '${bdp.system.bizdate}')
            ) a
    ) b
WHERE
    rn = 1