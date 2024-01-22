--********************************************************************--
--所属主题: 产品域
--功能描述: 经销商stock——历史数据
--创建者:王兆翔
--创建日期:2023-06-01
--修改日期  修改人  修改内容
--yyyymmdd  name  comment
--********************************************************************--
drop table if exists dwd_latest_dealer_stock_di;
create table if not exists dwd_latest_dealer_stock_di(
    customercode            string      comment '经销商代号'
    ,warehousename		    string	    comment	'仓库名称'
	,warehouseno		    string	    comment	'仓库编号'
	,boschpartno		    string	    comment	'博世10位料号'
	,boschpartno13		    string	    comment	'博世13位料号'
	,productname	        string	    comment	'产品描述'
	,productcategory	    string	    comment	'产品品类'
    ,stockqty               string	    comment	'库存数量'
    ,unit                   string	    comment	'计量单位'
    ,loaddate               string	    comment	'库存导出日期yyyymmdd'
    ,loadtime               string	    comment	'库存导出时间hh:mm:ss'
    ,boschpartno_verified   string      comment '匹配过后的bosch10位料号'
) comment '经销商历史数据整合表'
partitioned by(ds string comment '时间分区') stored as parquet location 'boschfs://boschfs/warehouse/dwd_latest_dealer_stock_di';