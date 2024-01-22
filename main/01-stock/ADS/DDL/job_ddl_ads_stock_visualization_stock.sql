--********************************************************************--
--所属主题: 库存域
--功能描述: 进销商库存本月数据及其他月月初月末数据
--创建者: 王兆翔
--创建日期:2023-06-07
--********************************************************************--
DROP TABLE IF EXISTS ads_stock_visualization_dealer_stock_history_df;
CREATE TABLE IF NOT EXISTS ads_stock_visualization_dealer_stock_history_df (
	customercode        string  comment '客户代码'
	,warehousename		string	comment	'仓库名称'
	,warehouseno		string	comment	'仓库编号'
	,boschpartno		string	comment	'博世10位料号'
	,boschpartno13		string	comment	'博世13位料号'
	,productname	    string	comment	'产品描述'
	,productcategory	string	comment	'产品品类'
    ,stockqty           string	comment	'库存数量'
    ,unit               string	comment	'计量单位'
    ,loaddate           string	comment	'库存导出日期YYYYMMDD'
    ,loadtime           string	comment	'库存导出时间HH:MM:SS'
	,boschpartno_verified	string comment '匹配过后的料号'
) COMMENT '经销商月份库存数据'
PARTITIONED BY (ds STRING COMMENT '经销商分区') STORED AS parquet
LOCATION 'boschfs://boschfs/warehouse/azure_blob/ads_stock_visualization_dealer_stock_history_df';
