--********************************************************************--
--所属主题: 订单域
--功能描述: dwd层dwd_o2c_customer_order_item_detail_info_di表的数据来源加工处理脚本
--创建者: xulei
--创建日期:2023-07-27
--更新日期:2023-11-03
--更新内容：新增vbap的werks字段，目标表中字段名为delivery_plant_code
--更新人: xulei
--********************************************************************--
-- select count(1) from  dwd_o2c_customer_order_item_detail_info_di
-- select pk_order_no, pk_order_item_no,count(1)  from  dwd_o2c_customer_order_item_detail_info_di group by pk_order_no,pk_order_item_no
-- having count(1)>1
SET hive.tez.container.size = 8096;
-- 1. calculate partiton value of primary table incr data
--根据从表发生变化的数据，关联主表取主表的分区值，后续将这些分区值内的数据进行重新计算以保证目标表中使用从表的字段都是最新状态。
drop table if exists tmp_dwd_o2c_customer_order_item_detail_info_di_partition_incr;
create table if not exists tmp_dwd_o2c_customer_order_item_detail_info_di_partition_incr as
select
        t2.pday
from
        ods_pcd_v_hist_vbak_mtxcd_di t1
join
        ods_pcd_v_hist_vbap_mtxcd_di t2
on
        t1.vbeln = t2.vbeln
and     t1.pday  = ${bdp.system.bizdate}

union

select
        t2.pday
from
        ods_pcd_v_hist_vbkd_mtxcd_di t1
join
        ods_pcd_v_hist_vbap_mtxcd_di t2
on
        t1.vbeln = t2.vbeln
and     t1.posnr = t2.posnr
and     t1.pday  = ${bdp.system.bizdate}

union

select
        t2.pday
from
        ods_pcd_v_hist_vbpa_mtxcd_di t1
join
        ods_pcd_v_hist_vbap_mtxcd_di t2
on
        t1.vbeln = t2.vbeln
and     t1.posnr = t2.posnr
and     t1.pday  = ${bdp.system.bizdate}

union

select
        t2.pday
from
        ods_pcd_v_hist_vbep_mtxcd_di t1
join
        ods_pcd_v_hist_vbap_mtxcd_di t2
on
        t1.vbeln = t2.vbeln
and     t1.posnr = t2.posnr
and     t1.pday  = ${bdp.system.bizdate}

union

select
        t2.pday
from
        ods_pcd_v_hist_rb11_yv2_lstdys_mtxcd_di t1
join
        ods_pcd_v_hist_vbap_mtxcd_di t2
on
        t1.vbeln = t2.vbeln
and     t1.posnr = t2.posnr
and     t1.pday  = ${bdp.system.bizdate}

union

select
        t2.pday
from
        ods_pcd_v_hist_vbup_mtxcd_di t1
join
        ods_pcd_v_hist_vbap_mtxcd_di t2
on
        t1.vbeln = t2.vbeln
and     t1.posnr = t2.posnr
and     t1.pday  = ${bdp.system.bizdate}

union

select
        t2.pday
from
        ods_pcd_v_hist_vbfa_mtxcd_di t1
join
        ods_pcd_v_hist_vbap_mtxcd_di t2
on
        t1.vbeln = t2.vbeln
and     t1.posnv = t2.posnr
and     t1.pday  = ${bdp.system.bizdate}

union

select
        cast(${bdp.system.bizdate} as string) as pday;
;
-- 2.1 calculate partiton value of auxiliary table incr data
--找出主表发生变化的数据在从表vbak中对应的分区值，用于后续关联时限制从表分区，避免从表数据进行全表扫描
drop table if exists tmp_vbak_data_partiton_value;
create table if not exists tmp_vbak_data_partiton_value as
select
        t3.pday
from
        ods_pcd_v_hist_vbap_mtxcd_di t1 --订单行表
join
        tmp_dwd_o2c_customer_order_item_detail_info_di_partition_incr t2
on
        t1.pday = t2.pday
join
        ods_pcd_v_hist_vbak_mtxcd_di t3 --订单表
on
        t1.vbeln = t3.vbeln
group by
        t3.pday;
-- 2.2
--找出主表发生变化的数据在从表vbkd中对应的分区值，用于后续关联时限制从表分区，避免从表数据进行全表扫描
drop table if exists tmp_vbkd_data_partiton_value;
create table if not exists tmp_vbkd_data_partiton_value as
select
        t3.pday
from
        ods_pcd_v_hist_vbap_mtxcd_di t1 --订单行表
join
        tmp_dwd_o2c_customer_order_item_detail_info_di_partition_incr t2
on
        t1.pday = t2.pday
join
        ods_pcd_v_hist_vbkd_mtxcd_di t3 --订单表
on
        t1.vbeln = t3.vbeln
and     t1.posnr = t3.posnr
group by
        t3.pday;
-- 2.3
--找出主表发生变化的数据在从表vbpa中对应的分区值，用于后续关联时限制从表分区，避免从表数据进行全表扫描
drop table if exists tmp_vbpa_data_partiton_value;
create table if not exists tmp_vbpa_data_partiton_value as
select
        t3.pday
from
        ods_pcd_v_hist_vbap_mtxcd_di t1 --订单行表
join
        tmp_dwd_o2c_customer_order_item_detail_info_di_partition_incr t2
on
        t1.pday = t2.pday
join
        ods_pcd_v_hist_vbpa_mtxcd_di t3 --订单表
on
        t1.vbeln = t3.vbeln
and     t1.posnr = t3.posnr
group by
        t3.pday;
-- 2.4
--找出主表发生变化的数据在从表rb11_yv2_lstdys中对应的分区值，用于后续关联时限制从表分区，避免从表数据进行全表扫描
drop table if exists tmp_vbep_data_partiton_value;
create table if not exists tmp_vbep_data_partiton_value as
select
        t3.pday
from
        ods_pcd_v_hist_vbap_mtxcd_di t1 --订单行表
join
        tmp_dwd_o2c_customer_order_item_detail_info_di_partition_incr t2
on
        t1.pday = t2.pday
join
        ods_pcd_v_hist_vbep_mtxcd_di t3 --订单表
on
        t1.vbeln = t3.vbeln
and     t1.posnr = t3.posnr
group by
        t3.pday;
-- 2.5
--找出主表发生变化的数据在从表rb11_yv2_lstdys中对应的分区值，用于后续关联时限制从表分区，避免从表数据进行全表扫描
drop table if exists tmp_rb11_yv2_lstdys_data_partiton_value;
create table if not exists tmp_rb11_yv2_lstdys_data_partiton_value as
select
        t3.pday
from
        ods_pcd_v_hist_vbap_mtxcd_di t1 --订单行表
join
        tmp_dwd_o2c_customer_order_item_detail_info_di_partition_incr t2
on
        t1.pday = t2.pday
join
        ods_pcd_v_hist_rb11_yv2_lstdys_mtxcd_di t3 --订单表
on
        t1.vbeln = t3.vbeln
and     t1.posnr = t3.posnr
group by
        t3.pday;
-- 2.6
--找出主表发生变化的数据在从表vbup中对应的分区值，用于后续关联时限制从表分区，避免从表数据进行全表扫描
drop table if exists tmp_vbup_data_partiton_value;
create table if not exists tmp_vbup_data_partiton_value as
select
        t3.pday
from
        ods_pcd_v_hist_vbap_mtxcd_di t1 --订单行表
join
        tmp_dwd_o2c_customer_order_item_detail_info_di_partition_incr t2
on
        t1.pday = t2.pday
join
        ods_pcd_v_hist_vbup_mtxcd_di t3 --订单表
on
        t1.vbeln = t3.vbeln
and     t1.posnr = t3.posnr
group by
        t3.pday;
-- 2.7
--找出主表发生变化的数据在从表vbfa中对应的分区值，用于后续关联时限制从表分区，避免从表数据进行全表扫描
drop table if exists tmp_vbfa_data_partiton_value;
create table if not exists tmp_vbfa_data_partiton_value as
select
        t3.pday
from
        ods_pcd_v_hist_vbap_mtxcd_di t1 --订单行表
join
        tmp_dwd_o2c_customer_order_item_detail_info_di_partition_incr t2
on
        t1.pday = t2.pday
join
        ods_pcd_v_hist_vbfa_mtxcd_di t3 --订单表
on
        t1.vbeln = t3.vbeln
and     t1.posnr = t3.posnv
group by
        t3.pday;
-- 3. pre step1 Calculates incr detail data
drop table if exists tmp_dwd_o2c_customer_order_item_detail_info_di_incr_1;
create table if not exists tmp_dwd_o2c_customer_order_item_detail_info_di_incr_1 as
select
        t1.posnr
,       t3.vbeln
,       t3.auart
,       t3.vbtyp as order_category
,       t3.vkorg
,       t3.vtweg
,       t1.vgbel
,       t1.vgpos
,       t1.posar
,       t1.pstyv
,       t3.kunnr as sold_to_party
,       t1.waerk
,       t3.stwae
,       t1.ernam
,       t3.vdatu
,       t3.lifsk
,       t1.rbcr_yv_dg_date
,       t1.rbcr_yv_dg_times
,       t1.matnr
,       t1.matwa
,       t1.arktx
,       t1.kdmat
,       t1.vrkme
,       t1.meins
,       t1.kmein
,       t1.lgort
,       t1.werks
,       t1.abgru
,       t1.posex
,       t1.vstel
,       t1.route
,       t1.ps_psp_pnr
,       t1.faksp
,
        case
        when
                t3.vbtyp ='H'
                and t4.currency_key is not null
        then
                t1.netwr * (-1) * t4.conversion_factor
        when
                t3.vbtyp ='H'
                and t4.currency_key is null
        then
                t1.netwr * (-1)
        when
                t3.vbtyp <>'H'
                and t4.currency_key is not null
        then
                t1.netwr * t4.conversion_factor
        else
                t1.netwr
        end as netwr
,
        case
        when
                t4.currency_key is not null
        then
                t1.netpr * t4.conversion_factor
        else
                t1.netpr
        end as netpr
,
        case
        when
                t3.vbtyp ='H'
        then
                (t1.kwmeng * -1)
        else
                t1.kwmeng
        end as kwmeng
,       t3.audat
,       t3.erzet                                                                                                                                                  as order_doc_time
,       concat(substr(t3.audat,1,4),'-',substr(t3.audat,5,2),'-',substr(t3.audat,7,2),' ',substr(t3.erzet,1,2),':',substr(t3.erzet,3,2),':',substr(t3.erzet,5,2)) as order_doc_datetime
,       t1.zmeng
,       t1.kpein
,       t1.kzwi1
,       t1.kzwi2
,       t1.kzwi3
,       t1.kzwi4
,       t1.kzwi5
,       t1.kzwi6
,       t1.erdat                                                                                                                                                  as item_create_date
,       t1.erzet                                                                                                                                                  as item_create_time
,       concat(substr(t1.erdat,1,4),'-',substr(t1.erdat,5,2),'-',substr(t1.erdat,7,2),' ',substr(t1.erzet,1,2),':',substr(t1.erzet,3,2),':',substr(t1.erzet,5,2)) as item_create_datetime
,       t1.aedat
,       date_format(CURRENT_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss') AS etl_load_time
,       t1.pday
,       substr(t1.erdat,1,6) as erdat_pmonth
from
        ods_pcd_v_hist_vbap_mtxcd_di t1
join
        tmp_dwd_o2c_customer_order_item_detail_info_di_partition_incr t2
on
        t1.pday = t2.pday
left join
        (
                select
                        t1.*
                from
                        ods_pcd_v_hist_vbak_mtxcd_di t1 --订单状态表
                join
                        tmp_vbak_data_partiton_value t2
                on
                        t1.pday = t2.pday ) t3
on
        t1.vbeln = t3.vbeln
left join
        dim_decimal_places_in_currmanual_df t4
on
        t1.waerk = t4.currency_key
inner join
        dim_order_type_df t5
on
        t3.auart = t5.pk_order_type_id;
-- 4.找出vbkd表中没有生成订单行号的订单，只有一条记录为'000000'的行号
drop table if exists tmp_vbkd_order_not_item_num;
create table tmp_vbkd_order_not_item_num as
select
        t1.vbeln
,       t1.posnr
,       t1.bzirk
,       t1.pltyp
,       t1.konda
,       t1.kdgrp
,       t1.inco1
,       t1.inco2
,       t1.ihrez
,       t1.kursk
,       t1.prsdt
,       t1.zterm
from
        ods_pcd_v_hist_vbkd_mtxcd_di t1 --订单状态表
join
        tmp_vbkd_data_partiton_value t2
on
        t1.pday = t2.pday
where
        t1.posnr='000000';
-- pre step2 Calculates incr detail data
drop table if exists tmp_dwd_o2c_customer_order_item_detail_info_di_incr_2;
create table if not exists tmp_dwd_o2c_customer_order_item_detail_info_di_incr_2 as
select
        t1.posnr
,       t1.vbeln
,       t1.auart
,       t1.order_category
,       t1.vkorg
,       t1.vtweg
,       t1.vgbel
,       t1.vgpos
,       t1.posar
,       t1.pstyv
,
        case
        when
                trim(t2.bzirk) is not null
        then
                trim(t2.bzirk)
        when
                trim(t3.bzirk) is not null
        then
                trim(t3.bzirk)
        end as sales_district
,       t1.sold_to_party
,       t1.waerk
,       t1.stwae
,       t1.ernam
,       t1.vdatu
,       t1.lifsk
,       t1.rbcr_yv_dg_date
,       t1.rbcr_yv_dg_times
,       t1.matnr
,       t1.matwa
,       t1.arktx
,       t1.kdmat
,       t1.vrkme
,       t1.meins
,       t1.kmein
,       t1.lgort
,       t1.werks
,
        case
        when
                trim(t2.pltyp) is not null
        then
                trim(t2.pltyp)
        when
                trim(t3.pltyp) is not null
        then
                t3.pltyp
        end as price_list
,
        case
        when
                trim(t2.konda) is not null
        then
                trim(t2.konda)
        when
                trim(t3.konda) is not null
        then
                t3.konda
        end as price_group
,
        case
        when
                trim(t2.kdgrp) is not null
        then
                trim(t2.kdgrp)
        when
                trim(t3.kdgrp) is not null
        then
                t3.kdgrp
        end as cust_pricing_group
,       t1.abgru
,       t1.posex
,       t1.vstel
,       t1.route
,       t1.ps_psp_pnr
,       t1.faksp
,
        case
        when
                trim(t2.inco1) is not null
        then
                trim(t2.inco1)
        when
                trim(t3.inco1) is not null
        then
                t3.inco1
        end as incoterm
,
        case
        when
                trim(t2.inco2) is not null
        then
                trim(t2.inco2)
        when
                trim(t3.inco2) is not null
        then
                t3.inco2
        end as incoterm2
,
        case
        when
                trim(t2.zterm) is not null
        then
                trim(t2.zterm)
        when
                trim(t3.zterm) is not null
        then
                t3.zterm
        end as payment_term
,
        case
        when
                trim(t2.ihrez) is not null
        then
                trim(t2.ihrez)
        when
                trim(t3.ihrez) is not null
        then
                t3.ihrez
        end as cust_vendor_contact
,       t1.netwr
,       t1.netpr
,       t1.kwmeng
,
        case
        when
                trim(t2.kursk) is not null
        then
                trim(t2.kursk)
        when
                trim(t3.kursk) is not null
        then
                trim(t3.kursk)
        end as exchange_rate
,       t1.audat
,       t1.order_doc_time
,       t1.order_doc_datetime
,
        case
        when
                trim(t2.prsdt) is not null
        then
                trim(t2.prsdt)
        when
                trim(t3.prsdt) is not null
        then
                trim(t3.prsdt)
        end as pricing_date
,       t1.zmeng
,       t1.kpein
,       t1.kzwi1
,       t1.kzwi2
,       t1.kzwi3
,       t1.kzwi4
,       t1.kzwi5
,       t1.kzwi6
,       t1.item_create_date
,       t1.item_create_time
,       t1.item_create_datetime
,       t1.aedat
,       t1.etl_load_time
,       t1.pday
,       t1.erdat_pmonth
from
        tmp_dwd_o2c_customer_order_item_detail_info_di_incr_1 t1
left join
        (
                select
                        t1.vbeln
                ,       t1.posnr
                ,       t1.bzirk
                ,       t1.pltyp
                ,       t1.konda
                ,       t1.kdgrp
                ,       t1.inco1
                ,       t1.inco2
                ,       t1.ihrez
                ,       t1.kursk
                ,       t1.prsdt
                ,       t1.zterm
                from
                        ods_pcd_v_hist_vbkd_mtxcd_di t1 --订单状态表
                join
                        tmp_vbkd_data_partiton_value t2
                on
                        t1.pday = t2.pday ) t2
on
        t1.vbeln = t2.vbeln
and     t1.posnr = t2.posnr
left join
        tmp_vbkd_order_not_item_num t3
on
        t1.vbeln = t3.vbeln;
drop table if exists tmp_vbpa_order;
create table if not exists tmp_vbpa_order as
select
        t1.vbeln
,       t1.posnr
,       t1.kunnr
,       t1.parvw
from
        ods_pcd_v_hist_vbpa_mtxcd_di t1
join
        tmp_vbpa_data_partiton_value t2
on
        t1.pday = t2.pday
where
        t1.parvw in ('WE'
                    , 'RG')
and     t1.posnr='000000';
-- pre step2 Calculates incr detail data
drop table if exists tmp_dwd_o2c_customer_order_item_detail_info_di_incr_3;
create table if not exists tmp_dwd_o2c_customer_order_item_detail_info_di_incr_3 as
select
        t1.posnr
,       t1.vbeln
,       t1.auart
,       t1.order_category
,       t1.vkorg
,       t1.vtweg
,       t1.vgbel
,       t1.vgpos
,       t1.posar
,       t1.pstyv
,       t1.sales_district
,       t1.sold_to_party
,
        case
        when
                trim(t2.kunnr) is not null
                and t2.parvw ='WE'
        then
                trim(t2.kunnr)
        when
                trim(t3.kunnr) is not null
                and t3.parvw ='WE'
        then
                trim(t3.kunnr)
        end as ship_to_party
,
        case
        when
                trim(t2.kunnr) is not null
                and t2.parvw ='RG'
        then
                trim(t2.kunnr)
        when
                trim(t4.kunnr) is not null
                and t4.parvw ='RG'
        then
                trim(t4.kunnr)
        end as payer
,       t1.waerk
,       t1.stwae
,       t1.ernam
,       t1.vdatu
,       t1.lifsk
,       t1.rbcr_yv_dg_date
,       t1.rbcr_yv_dg_times
,       t1.matnr
,       t1.matwa
,       t1.arktx
,       t1.kdmat
,       t1.vrkme
,       t1.meins
,       t1.kmein
,       t1.lgort
,       t1.werks
,       t1.price_list
,       t1.price_group
,       t1.cust_pricing_group
,       t1.abgru
,       t1.posex
,       t1.vstel
,       t1.route
,       t1.ps_psp_pnr
,       t1.faksp
,       t1.incoterm
,       t1.incoterm2
,       t1.payment_term
,       t1.cust_vendor_contact
,       t1.netwr
,       t1.netpr
,       t1.kwmeng
,       t1.exchange_rate
,       t1.audat
,       t1.order_doc_time
,       t1.order_doc_datetime
,       t1.pricing_date
,       t1.zmeng
,       t1.kpein
,       t1.kzwi1
,       t1.kzwi2
,       t1.kzwi3
,       t1.kzwi4
,       t1.kzwi5
,       t1.kzwi6
,       t1.item_create_date
,       t1.item_create_time
,       t1.item_create_datetime
,       t1.aedat
,       t1.etl_load_time
,       t1.pday
,       t1.erdat_pmonth
from
        tmp_dwd_o2c_customer_order_item_detail_info_di_incr_2 t1
left join
        (
                select
                        t1.vbeln
                ,       t1.posnr
                ,       t1.kunnr
                ,       t1.parvw
                from
                        ods_pcd_v_hist_vbpa_mtxcd_di t1 --订单状态表
                join
                        tmp_vbpa_data_partiton_value t2
                on
                        t1.pday = t2.pday ) t2
on
        t1.vbeln = t2.vbeln
and     t1.posnr = t2.posnr
left join
        tmp_vbpa_order t3
on
        t1.vbeln = t3.vbeln
and     t3.parvw ='WE'
left join
        tmp_vbpa_order t4
on
        t1.vbeln = t4.vbeln
and     t4.parvw ='RG';
-- vbep表预处理
drop table if exists tmp_vbep_resurt;
create table if not exists tmp_vbep_resurt as
select
        vbeln
,       posnr
,       edatu
,       mbdat
,       lifsp
,       wadat
,       'min' as flg_type
from
        (
                select
                        t1.vbeln
                ,       t1.posnr
                ,       t1.edatu
                ,       t1.mbdat
                ,       t1.lifsp
                ,       t1.wadat
                ,       row_number() over
                                          (
                                                  partition by t1.vbeln
                                                          , t1.posnr
                                                  order by t1.etenr
                                          )
                        as rn
                from
                        ods_pcd_v_hist_vbep_mtxcd_di t1
                join
                        tmp_vbep_data_partiton_value t2
                on
                        t1.pday = t2.pday )t1
where
        rn = 1

union

select
        vbeln
,       posnr
,       edatu
,       mbdat
,       lifsp
,       wadat
,       'max' as flg_type
from
        (
                select
                        t1.vbeln
                ,       t1.posnr
                ,       t1.edatu
                ,       t1.mbdat
                ,       t1.lifsp
                ,       t1.wadat
                ,       row_number() over
                                          (
                                                  partition by t1.vbeln
                                                          , t1.posnr
                                                  order by t1.etenr desc
                                          )
                        as rn
                from
                        ods_pcd_v_hist_vbep_mtxcd_di t1
                join
                        tmp_vbep_data_partiton_value t2
                on
                        t1.pday = t2.pday )t1
where
        rn = 1;
drop table if exists tmp_vbep_resurt2;
create table if not exists tmp_vbep_resurt2 as
select
        t1.vbeln
,       t1.posnr
,       lifsp as del_block_schedule_line
from
        (
                select
                        t1.vbeln
                ,       t1.posnr
                ,       trim(t1.lifsp) as lifsp
                ,       row_number() over
                                          (
                                                  partition by t1.vbeln
                                                          , t1.posnr
                                                  order by t1.etenr desc
                                          )
                        as rn
                from
                        ods_pcd_v_hist_vbep_mtxcd_di t1
                join
                        tmp_vbep_data_partiton_value t2
                on
                        t1.pday  = t2.pday
                and     t1.wmeng > 0
                and     t1.lifsp is not null
                and     trim(t1.lifsp) <> '' )t1
where
        t1.rn = 1;
drop table if exists tmp_indicatrix_pre;
create table if not exists tmp_indicatrix_pre as
select
        t1.vbeln
,       t1.posnr
,       sum(
                case
                when
                        t2.order_type_general_description ='return'
                        and t3.vbtyp_n                    ='T'
                then
                        t3.rfmng * -1
                else
                        0
                end) as delivery_quantity_lvl1
,       sum(
                case
                when
                        t2.order_type_general_description ='Direct shipment'
                then
                        0
                else
                        0
                end) as delivery_quantity_lvl2
,       sum(
                case
                when
                        t3.vbtyp_n='J'
                then
                        t3.rfmng
                else
                        0
                end) as delivery_quantity_lvl3
,       sum(
                case
                when
                        t2.order_type_general_description ='return'
                        and t3.vbtyp_n                    ='O'
                then
                        t3.rfmng * -1
                else
                        0
                end) as billing_quantity_lvl1
,       sum(
                case
                when
                        t2.order_type_general_description in ('Standard-no billing'
                                                             , 'Consignment')
                then
                        0
                else
                        0
                end) as billing_quantity_lvl2
,       sum(
                case
                when
                        t3.vbtyp_n='M'
                then
                        t3.rfmng
                else
                        0
                end) as billing_quantity_lvl3
,       sum(
                case
                when
                        t2.order_type_general_description ='return'
                        and t3.vbtyp_n                    ='R'
                        and t3.bwart in ('653'
                                        , '632')
                then
                        t3.rfmng * -1
                else
                        0
                end) as goods_issued_quantity_lvl1
,       sum(
                case
                when
                        t2.order_type_general_description ='Direct shipment'
                then
                        0
                else
                        0
                end) as goods_issued_quantity_lvl2
,       sum(
                case
                when
                        t3.vbtyp_n='R'
                        and t3.bwart not in ('653'
                                            , '654'
                                            , '632'
                                            , '634')
                then
                        t3.rfmng
                else
                        0
                end) as goods_issued_quantity_lvl3
from
        tmp_dwd_o2c_customer_order_item_detail_info_di_incr_3 t1
left join
        dim_order_type_df t2
on
        t1.auart = t2.pk_order_type_id
left join
        (
                select
                        t1.vbelv
                ,       t1.posnv
                ,       t1.vbtyp_n
                ,       t1.bwart
                ,       t1.rfmng
                from
                        ods_pcd_v_hist_vbfa_mtxcd_di t1 --销售凭证流
                join
                        tmp_vbfa_data_partiton_value t2
                on
                        t1.pday = t2.pday ) t3
on
        t1.vbeln = t3.vbelv
and     t1.posnr = t3.posnv
group by
        t1.vbeln
,       t1.posnr;
-- pre step3 Calculates incr detail data
drop table if exists tmp_dwd_o2c_customer_order_item_detail_info_di_incr_4;
create table if not exists tmp_dwd_o2c_customer_order_item_detail_info_di_incr_4 as
select
        t1.posnr as pk_order_item_no
,       t1.vbeln as pk_order_no
,       t1.auart as order_type
,       t1.order_category
,       t6.order_type_general_description as order_type_gen_descr
,       t1.vkorg                          as sales_org
,       t1.vtweg                          as distr_channel
,       t1.vgbel                          as parent_doc_no
,       t1.vgpos                          as parent_doc_item_no
,       t1.posar                          as item_type
,       t1.pstyv                          as item_category
,       t1.sales_district
,       t1.sold_to_party
,       t1.ship_to_party
,       t1.payer
,       t1.waerk            as document_currency
,       t1.stwae            as local_currency
,       t1.ernam            as item_create_by
,       t1.vdatu            as initial_req_del_date
,       t2.edatu            as cust_req_del_date
,       t3.edatu            as sys_cfm_del_date
,       t3.mbdat            as cfm_material_avail_date
,       t3.wadat            as cfm_goods_issue_date
,       t1.rbcr_yv_dg_date  as downgrade_date
,       t1.rbcr_yv_dg_times as downgrade_time
,       t1.matnr            as material_no
,       t1.matwa            as material_entered
,       t1.arktx            as order_material_descr
,       t1.kdmat            as cust_material_no
,       t1.vrkme            as material_sold_sales_unit
,       t1.meins            as base_unit_meas
,       t1.kmein            as cond_ref_unit_meas
,       t1.lgort            as storage_location
,       t1.werks            as delivery_plant_code
,       t1.price_list
,       t1.price_group
,       t1.cust_pricing_group
,       t1.abgru      as reject_reason
,       t1.posex      as cust_po_item_no
,       t1.vstel      as shipping_point
,       t1.route      as del_route
,       t1.ps_psp_pnr as wbs_element
,       t1.incoterm
,       t1.incoterm2
,       t1.payment_term
,       t1.cust_vendor_contact
,       t1.faksp as is_bill_block_order_item
,       t5.del_block_schedule_line
,       t1.lifsk as del_block_order
,
        case
        when
                t1.netwr is null
        then
                0
        else
                t1.netwr
        end as net_value
,
        case
        when
                t1.netpr is null
        then
                0
        else
                t1.netpr
        end as net_price
,
        case
        when
                t1.kwmeng is null
        then
                0
        else
                t1.kwmeng
        end as item_qty
,
        case
        when
                t1.exchange_rate is null
        then
                0
        else
                t1.exchange_rate
        end      as exchange_rate
,       t1.audat as order_doc_date
,       t1.order_doc_time
,       t1.order_doc_datetime
,       t1.pricing_date
,
        case
        when
                t1.zmeng is null
        then
                0
        else
                t1.zmeng
        end as target_quantity
,
        case
        when
                t1.kpein is null
        then
                0
        else
                t1.kpein
        end      as cond_pricing_unit
,       t1.kzwi1 as pricing_cond_subtotal_1
,       t1.kzwi2 as pricing_cond_subtotal_2
,       t1.kzwi3 as pricing_cond_subtotal_3
,       t1.kzwi4 as pricing_cond_subtotal_4
,       t1.kzwi5 as pricing_cond_subtotal_5
,       t1.kzwi6 as pricing_cond_subtotal_6
,
        case
        when
                t6.order_type_general_description ='return'
        then
                t4.delivery_quantity_lvl1
        when
                t6.order_type_general_description ='Direct shipment'
        then
                t4.delivery_quantity_lvl2
        when
                t6.order_type_general_description not in ('return'
                                                         , 'Direct shipment')
        then
                t4.delivery_quantity_lvl3
        else
                0
        end as item_del_qty
,
        case
        when
                t6.order_type_general_description ='return'
        then
                t4.billing_quantity_lvl1
        when
                t6.order_type_general_description in ('Standard-no billing'
                                                     , 'Consignment')
        then
                t4.billing_quantity_lvl2
        when
                t6.order_type_general_description not in ('return'
                                                         , 'Standard-no billing'
                                                         , 'Consignment')
        then
                t4.billing_quantity_lvl3
        else
                0
        end as item_bill_qty
,
        case
        when
                t6.order_type_general_description ='return'
        then
                t4.goods_issued_quantity_lvl1
        when
                t6.order_type_general_description ='Direct shipment'
        then
                t4.goods_issued_quantity_lvl2
        when
                t6.order_type_general_description not in ('return'
                                                         , 'Direct shipment')
        then
                t4.goods_issued_quantity_lvl3
        else
                0
        end as item_goods_issue_qty
,       t1.item_create_date
,       t1.item_create_time
,       t1.item_create_datetime
,       t1.aedat as item_update_date
,       t1.etl_load_time
,       t1.pday
,       t1.erdat_pmonth
from
        tmp_dwd_o2c_customer_order_item_detail_info_di_incr_3 t1
left join
        tmp_vbep_resurt t2 --计划行数据
on
        t1.vbeln    = t2.vbeln
and     t1.posnr    = t2.posnr
and     t2.flg_type = 'min'
left join
        tmp_vbep_resurt t3 --计划行数据
on
        t1.vbeln    = t3.vbeln
and     t1.posnr    = t3.posnr
and     t3.flg_type = 'max'
left join
        tmp_indicatrix_pre t4
on
        t1.vbeln = t4.vbeln
and     t1.posnr = t4.posnr
left join
        tmp_vbep_resurt2 t5
on
        t1.vbeln = t5.vbeln
and     t1.posnr = t5.posnr
left join
        dim_order_type_df t6
on
        t1.auart = t6.pk_order_type_id;
drop table if exists tmp_dwd_o2c_customer_order_item_detail_info_di_incr_5;
create table if not exists tmp_dwd_o2c_customer_order_item_detail_info_di_incr_5 as
select
        t1.pk_order_item_no
,       t1.pk_order_no
,       t1.order_type
,       t1.order_type_gen_descr
,       t1.order_category
,       t1.sales_org
,       t1.distr_channel
,       t1.parent_doc_no
,       t1.parent_doc_item_no
,       t1.item_type
,       t1.item_category
,       t1.sales_district
,       t1.sold_to_party
,       t1.ship_to_party
,       t1.payer
,       t1.document_currency
,       t1.local_currency
,       t1.item_create_by
,       t1.initial_req_del_date
,       t1.cust_req_del_date
,       t1.sys_cfm_del_date
,       t1.cfm_material_avail_date
,       t1.cfm_goods_issue_date
,       trim(t2.mbdat)    as tgt_material_avail_date
,       trim(t2.wadat)    as tgt_goods_issue_date
,       trim(t2.edatu)    as tgt_del_date
,       trim(t2.mbdat_lt) as planned_material_avail_date
,       trim(t2.wadat_lt) as planned_goods_issue_date
,       trim(t2.edatu_lt) as planned_del_date
,       t1.downgrade_date
,       t1.downgrade_time
,       trim(t2.y11v2_prat1_lt) as product_attribute
,       t1.material_no
,       t1.material_entered
,       t1.order_material_descr
,       t1.cust_material_no
,       t1.material_sold_sales_unit
,       t1.base_unit_meas
,       t1.cond_ref_unit_meas
,       t1.storage_location
,       t1.delivery_plant_code
,       t1.price_list
,       t1.price_group
,       t1.cust_pricing_group
,       t1.reject_reason
,       t1.cust_po_item_no
,       t1.shipping_point
,       t1.del_route
,       t1.wbs_element
,       t1.incoterm
,       t1.incoterm2
,       t1.payment_term
,       t1.cust_vendor_contact
,       t3.gbsta as overall_item_status
,       t3.lfsta as item_del_status
,       t3.fksaa as item_related_bill_status
,       t3.lfgsa as overall_item_delivery_status
,       t3.pksta as item_packing_status
,       t3.wbsta as item_goods_mvmt_status
,       t3.fksta as item_del_related_bill_status
,       t3.absta as item_reject_status
,       t1.is_bill_block_order_item
,       t1.del_block_schedule_line
,       t1.del_block_order
,       cast(t1.net_value as decimal(28,8))    as net_value
,       cast(t1.net_price as decimal(28,8))    as net_price
,       cast(t1.item_qty as decimal(28,3))     as item_qty
,       cast(t1.exchange_rate as decimal(9,5)) as exchange_rate
,       t1.order_doc_date
,       t1.order_doc_time
,       t1.order_doc_datetime
,       t1.pricing_date
,       cast(t1.target_quantity as bigint)   as target_quantity
,       cast(t1.cond_pricing_unit as bigint) as cond_pricing_unit
,       t1.pricing_cond_subtotal_1
,       t1.pricing_cond_subtotal_2
,       t1.pricing_cond_subtotal_3
,       t1.pricing_cond_subtotal_4
,       t1.pricing_cond_subtotal_5
,       t1.pricing_cond_subtotal_6
,       cast(t1.item_del_qty as decimal(28,3))         as item_del_qty
,       cast(t1.item_bill_qty as decimal(28,3))        as item_bill_qty
,       cast(t1.item_goods_issue_qty as decimal(28,3)) as item_goods_issue_qty
,       t1.item_create_date
,       t1.item_create_time
,       t1.item_create_datetime
,       t1.item_update_date
,       t1.etl_load_time
,       t1.pday
,       t1.erdat_pmonth
from
        tmp_dwd_o2c_customer_order_item_detail_info_di_incr_4 t1
left join
        (
                select
                        t1.*
                from
                        ods_pcd_v_hist_rb11_yv2_lstdys_mtxcd_di t1 --订单状态表
                join
                        tmp_rb11_yv2_lstdys_data_partiton_value t2
                on
                        t1.pday = t2.pday
                and     t1.etenr='0001' ) t2
on
        t1.pk_order_no      = t2.vbeln
and     t1.pk_order_item_no = t2.posnr
left join
        (
                select
                        t1.*
                from
                        ods_pcd_v_hist_vbup_mtxcd_di t1 --订单行状态表
                join
                        tmp_vbup_data_partiton_value t2
                on
                        t1.pday = t2.pday ) t3
on
        t1.pk_order_no      = t3.vbeln
and     t1.pk_order_item_no = t3.posnr;
--4. no changed data in incr data partition
drop table if exists tmp_dwd_o2c_customer_order_item_detail_info_di_not_exits;
create table if not exists tmp_dwd_o2c_customer_order_item_detail_info_di_not_exits as
select
        t1.pk_order_item_no
,       t1.pk_order_no
,       t1.order_type
,       t1.order_type_gen_descr
,       t1.order_category
,       t1.sales_org
,       t1.distr_channel
,       t1.parent_doc_no
,       t1.parent_doc_item_no
,       t1.item_type
,       t1.item_category
,       t1.sales_district
,       t1.sold_to_party
,       t1.ship_to_party
,       t1.payer
,       t1.document_currency
,       t1.local_currency
,       t1.item_create_by
,       t1.initial_req_del_date
,       t1.cust_req_del_date
,       t1.sys_cfm_del_date
,       t1.cfm_material_avail_date
,       t1.cfm_goods_issue_date
,       t1.tgt_material_avail_date
,       t1.tgt_goods_issue_date
,       t1.tgt_del_date
,       t1.planned_material_avail_date
,       t1.planned_goods_issue_date
,       t1.planned_del_date
,       t1.downgrade_date
,       t1.downgrade_time
,       t1.product_attribute
,       t1.material_no
,       t1.material_entered
,       t1.order_material_descr
,       t1.cust_material_no
,       t1.material_sold_sales_unit
,       t1.base_unit_meas
,       t1.cond_ref_unit_meas
,       t1.storage_location
,       t1.delivery_plant_code
,       t1.price_list
,       t1.price_group
,       t1.cust_pricing_group
,       t1.reject_reason
,       t1.cust_po_item_no
,       t1.shipping_point
,       t1.del_route
,       t1.wbs_element
,       t1.incoterm
,       t1.incoterm2
,       t1.payment_term
,       t1.cust_vendor_contact
,       t1.overall_item_status
,       t1.item_del_status
,       t1.item_related_bill_status
,       t1.overall_item_delivery_status
,       t1.item_packing_status
,       t1.item_goods_mvmt_status
,       t1.item_del_related_bill_status
,       t1.item_reject_status
,       t1.is_bill_block_order_item
,       t1.del_block_schedule_line
,       t1.del_block_order
,       t1.net_value
,       t1.net_price
,       t1.item_qty
,       t1.exchange_rate
,       t1.order_doc_date
,       t1.order_doc_time
,       t1.order_doc_datetime
,       t1.pricing_date
,       t1.target_quantity
,       t1.cond_pricing_unit
,       t1.pricing_cond_subtotal_1
,       t1.pricing_cond_subtotal_2
,       t1.pricing_cond_subtotal_3
,       t1.pricing_cond_subtotal_4
,       t1.pricing_cond_subtotal_5
,       t1.pricing_cond_subtotal_6
,       t1.item_del_qty
,       t1.item_bill_qty
,       t1.item_goods_issue_qty
,       t1.item_create_date
,       t1.item_create_time
,       t1.item_create_datetime
,       t1.item_update_date
,       date_format(CURRENT_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss') AS etl_load_time
,       t1.pday
,       t1.erdat_pmonth
from
        dwd_o2c_customer_order_item_detail_info_di t1
join
        (
                select
                        erdat_pmonth
                from
                        tmp_dwd_o2c_customer_order_item_detail_info_di_incr_5
                group by
                        erdat_pmonth )t2
on
        t1.erdat_pmonth = t2.erdat_pmonth
left join
        tmp_dwd_o2c_customer_order_item_detail_info_di_incr_5 t3
on
        t1.pk_order_item_no = t3.pk_order_item_no
and     t1.pk_order_no      = t3.pk_order_no
where
        t3.pk_order_item_no is null
and     t3.pk_order_no is null;