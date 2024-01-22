--********************************************************************--
--所属主题: 供应链域
--功能描述: 供应链域 - Smart code 
--创建者: wzx
--创建日期:2023-12-14
--修改日期  修改人  修改内容
--********************************************************************--
with t1 as (    -- 获取min_code
select sernr_sub
from 
(select sernr_sub from dwd_lgs_smart_code_info_df where ds = ${bdp.system.bizdate} ) a
left join 
(select sernr_main from dwd_lgs_smart_code_info_df where ds = ${bdp.system.bizdate} ) b 
on
a.sernr_sub = b.sernr_main
where sernr_main is null
)
, t2 as (   -- 获取min_code相关的所有层级信息
select c.*
from t1 left join 
(select * from dwd_lgs_smart_code_binding_rltnp_df where ds = ${bdp.system.bizdate}) c on t1.sernr_sub = c.min_smart_code
)
, t3 as (  -- 行列转换
select min_smart_code, primary_code from t2
union all
select min_smart_code, secondary_code from t2
union all
select min_smart_code, level_three_code from t2
union all
select min_smart_code, level_four_code from t2
union all
select min_smart_code, level_five_code from t2
union all
select min_smart_code, level_six_code from t2
union all
select min_smart_code, level_seven_code from t2
union all
select min_smart_code, level_eight_code from t2
)
, t4 as (  -- 获取所有的层级code 与其对应的min_code
select
    primary_code, min_smart_code as leaf_node_smart_code
from t3
where primary_code is not null and primary_code != ''
)
, t5 as (  -- 获取每级code对应的信息
    SELECT 
	 t1.min_smart_code 			AS    scanned_smart_code		-- 所有编码
	,t1.min_smart_code_level	AS    min_smart_code_level		-- 所在层级
	,t1.max_level				AS	  max_level					-- 链路最高层级码
	,t1.material_code			AS    material_code				-- 物料编码
	,t1.material_name			AS	  material_name				-- 物料名称
	,t2.sold_to_party			AS    customer_code				-- 售达方
	,t2.sold_to_cst_name		AS 	  customer_name		     	-- 售达方客户名称
	,t1.is_leaf_node            AS    is_leaf_node              -- 是否是配件
	,t1.rlt_code_qty			AS	  rlt_code_qty				-- 向下所有包含物料数量
	,date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')  AS    etl_load_time				-- 数据加载时间
FROM
	(
		SELECT 
			 a.min_smart_code		-- 所有编码
			,min_smart_code_level	-- 所在层级
			,material_code			-- 物料编码
			,material_name			-- 物料名称
			,rlt_code_qty			-- 向下所有包含物料数量
			,is_leaf_node           -- 是否为叶子节点
			,max_level				-- 链路最高层级码
			,vbeln					-- 交货单号
		FROM dwd_lgs_smart_code_binding_rltnp_df a
		WHERE ds = '${bdp.system.bizdate}'
		AND a.min_smart_code NOT IN	-- 过滤有多个父级的编码数据
				(
					SELECT min_smart_code
					FROM dwd_lgs_smart_code_binding_rltnp_df
					WHERE ds = '${bdp.system.bizdate}'
					GROUP BY min_smart_code
					HAVING COUNT(1) > 1
				)
	)t1
LEFT JOIN
	(
		SELECT 
			 sold_to_party		-- 售达方
			,sold_to_cst_name	-- 售达方客户名称
			,delivery_num		-- 交货单号
		FROM dwd_delivery_header_cn_df
		WHERE ds = '${bdp.system.bizdate}'
		GROUP BY 
			 sold_to_party
			,sold_to_cst_name
			,delivery_num
	)t2
ON t1.vbeln = t2.delivery_num
)
insert overwrite table ads_smart_code_wks_scan_df
select
    t5.scanned_smart_code           -- 所有编码
    ,t4.leaf_node_smart_code	    -- 最小层级编码
    ,t5.min_smart_code_level	    -- 所在层级
    ,t5.max_level			        -- 链路最高层级码	
    ,t5.material_code		        -- 物料编码	
    ,t5.material_name		        -- 物料名称	
    ,t5.customer_code		        -- 售达方	
    ,t5.customer_name		        -- 售达方客户名称    
    ,t5.is_leaf_node                -- 是否是配件 
    ,t5.rlt_code_qty			    -- 向下所有包含物料数量
    ,t5.etl_load_time			    -- 数据加载时间
from t5 left join t4 on t5.scanned_smart_code = t4.primary_code

