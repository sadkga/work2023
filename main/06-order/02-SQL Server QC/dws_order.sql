--********************************************************************
--所属主题: 订单域
--功能描述: dws层dws_o2c_customer_order_item_basic_indicator_di表的数据加工处理脚本
--用途：dws层order item表粒度和dwd层order item粒度一致，没有做轻度聚合，只是计算了派生指标，供分析人员使用
--创建者: xulei
--创建日期:2023-08-03
--********************************************************************
-- 1. calculate partiton value of primary table incr data
-- dws层获取dwd层增量数据只能限定etl_load_time值等于当天，然后找出所有分区字段，
-- 通过pday找会丢失在dwd层计算逻辑中从表字段发生变化，主表未变化的数据，因为dwd层pday字段值来自于主表
drop table if exists tmp_dws_o2c_customer_order_item_basic_indicator_di_partition_incr;
create table if not exists tmp_dws_o2c_customer_order_item_basic_indicator_di_partition_incr as
select
        t1.erdat_pmonth
from
        dwd_o2c_customer_order_item_detail_info_di t1
join
        dwd_o2c_customer_order_header_info_di t2
on
        t1.pk_order_no                = t2.pk_order_no
and     substr(t2.etl_load_time,1,10) = current_date()

union

select
        erdat_pmonth
from
        dwd_o2c_customer_order_item_detail_info_di
where
        substr(etl_load_time,1,10) = current_date();
-- 2. calculate partiton value of auxiliary table incr data
drop table if exists tmp_dwd_o2c_customer_order_header_info_di_data_partiton_value;
create table if not exists tmp_dwd_o2c_customer_order_header_info_di_data_partiton_value as
select
        t2.erdat_pmonth
from
        (
                select
                        t1.pk_order_no
                from
                        dwd_o2c_customer_order_item_detail_info_di t1
                join
                        tmp_dws_o2c_customer_order_item_basic_indicator_di_partition_incr t2
                on
                        t1.erdat_pmonth = t2.erdat_pmonth
                group by
                        pk_order_no ) t1
join
        dwd_o2c_customer_order_header_info_di t2
on
        t1.pk_order_no = t2.pk_order_no
group by
        t2.erdat_pmonth

union

select
        erdat_pmonth
from
        dwd_o2c_customer_order_header_info_di
where
        substr(etl_load_time,1,10) = current_date()
group by
        erdat_pmonth;
-- 3. pre step1 Calculates incr data
drop table if exists tmp_dws_o2c_customer_order_item_basic_indicator_di_pre;
create table if not exists tmp_dws_o2c_customer_order_item_basic_indicator_di_pre as
select
        t1.pk_order_item_no
,       t1.pk_order_no
,       t3.overall_credit_check_status
,       t3.bill_block_order
,       t3.local_currency
,
        case
        when
                t3.order_type_gen_descr <> 'Direct shipment'
                and ABS(t1.item_qty)    > ABS(t1.item_goods_issue_qty)
                and (
                        t1.reject_reason is null
                        or trim(t1.reject_reason) ='')
        then
                'Y'
        when
                t3.order_type_gen_descr = 'Direct shipment'
                and t1.item_qty         > t1.item_bill_qty
                and (
                        t1.reject_reason is null
                        or trim(t1.reject_reason) ='')
        then
                'Y'
        else
                null
        end as is_open_order
from
        dwd_o2c_customer_order_item_detail_info_di t1
join
        tmp_dws_o2c_customer_order_item_basic_indicator_di_partition_incr t2
on
        t1.erdat_pmonth = t2.erdat_pmonth
left join
        dwd_o2c_customer_order_header_info_di t3
on
        t1.pk_order_no = t3.pk_order_no;
--对汇率维表进行日期打平处理，因为存在部分日期没有汇率
drop table if exists tmp_dws_o2c_customer_order_item_basic_indicator_di_pre2;
create table if not exists tmp_dws_o2c_customer_order_item_basic_indicator_di_pre2 as
select
        pk_from_currency
,       pk_valid_from as startdate
,
        case
        when
                (lead(pk_valid_from, 1, 0) OVER
                                                (
                                                        PARTITION BY pk_from_currency
                                                        ORDER BY pk_valid_from
                                                )
                ) =0
        then
                '99991231'
        else
                lead(pk_valid_from, 1, 0) OVER
                                               (
                                                       PARTITION BY pk_from_currency
                                                       ORDER BY pk_valid_from
                                               )
        end as enddate
,       exchange_rate_ratio_considered
from
        dim_exchange_rate_df
where
        pk_exchange_rate_type = 'EURX'
and     pk_to_currency        = 'EUR';
--订单创建日期为20200104时存在1152条订单order_doc_date日期为空的数据，导致daily_exchange_rate和net_value_gc以及order_value_wo_del_gc结果为null，最终影响1906行历史数据
drop table if exists tmp_dws_o2c_customer_order_item_basic_indicator_di_pre3;
create table if not exists tmp_dws_o2c_customer_order_item_basic_indicator_di_pre3 as
select
        t1.pk_order_no
,       t1.local_currency
,       t1.order_doc_date
,
        case
        when
                t1.local_currency = 'EUR'
        then
                1
        when
                t1.local_currency <> 'EUR'
                and t3.startdate is not null
        then
                t3.exchange_rate_ratio_considered
        end as daily_exchange_rate
from
        dwd_o2c_customer_order_header_info_di t1 --该表存在order_doc_date为空的订单
join
        tmp_dwd_o2c_customer_order_header_info_di_data_partiton_value t2
on
        t1.erdat_pmonth = t2.erdat_pmonth
left join
        tmp_dws_o2c_customer_order_item_basic_indicator_di_pre2 t3
on
        t1.local_currency = t3.pk_from_currency
and     t1.order_doc_date >= t3.startdate
and     t1.order_doc_date < t3.enddate;
--  select erdat_pmonth from tmp_dws_o2c_customer_order_item_basic_indicator_di_incr_1
--  where  daily_exchange_rate is null group by erdat_pmonth or net_value is null or net_value_lc is null
-- 3. pre step1 Calculates incr detail data
drop table if exists tmp_dws_o2c_customer_order_item_basic_indicator_di_incr_1;
create table if not exists tmp_dws_o2c_customer_order_item_basic_indicator_di_incr_1 as
select
        t1.pk_order_item_no
,       t1.pk_order_no
,       t1.order_type
,       t1.order_type_gen_descr
,       t1.sales_org
,       t1.distr_channel
,       t1.parent_doc_no
,       t1.parent_doc_item_no
,       t1.item_type
,       t1.order_category
,       t1.sales_district as sales_distr
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
,       t1.base_unit_meas     as base_unit_meas
,       t1.cond_ref_unit_meas as cond_ref_unit_meas
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
,       t1.item_category
,       t1.net_value
,
        case
        when
                t1.exchange_rate < 0
        then
                t1.net_value / ABS(t1.exchange_rate)
        else
                t1.net_value * t1.exchange_rate
        end as net_value_lc
,       t1.net_price
,       t1.item_qty
,       t1.exchange_rate
,       t1.is_bill_block_order_item
,       t1.del_block_order
,       t4.daily_exchange_rate
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
,       '' as distr_channel_pl
,       t3.overall_credit_check_status
,       t3.bill_block_order
,       t1.del_block_schedule_line
,       t1.item_del_qty
,       t1.item_bill_qty
,       t1.item_goods_issue_qty
,       t3.is_open_order
,       t1.item_del_qty - t1.item_goods_issue_qty as open_del_qty
,       t1.item_qty     - t1.item_del_qty         as order_qty_wo_del
,
        case
        when
                t3.is_open_order            = 'Y'
                and t1.order_type_gen_descr <> 'Direct shipment'
        then
                t1.item_qty - t1.item_goods_issue_qty
        when
                t3.is_open_order            = 'Y'
                and t1.order_type_gen_descr = 'Direct shipment'
        then
                t1.item_qty - t1.item_bill_qty
        else
                0
        end as open_order_qty
,
        case
        when
                t1.item_goods_issue_qty = 0
                or t1.net_price         =0
                or t1.cond_pricing_unit =0
        then
                0
        else
                t1.item_goods_issue_qty * t1.net_price / t1.cond_pricing_unit
        end as goods_issued_value
,
        case
        when
                (t1.item_qty - t1.item_del_qty) = 0
                or t1.net_price                 =0
                or t1.cond_pricing_unit         =0
        then
                0
        else
                (t1.item_qty - t1.item_del_qty) * t1.net_price / t1.cond_pricing_unit
        end as order_value_wo_del
,
        case
        when
                t1.item_bill_qty       = 0
                or t1.net_price        =0
                or t1.cond_pricing_unit=0
        then
                0
        else
                t1.item_bill_qty * t1.net_price / t1.cond_pricing_unit
        end as billing_value
,       t1.item_create_date
,       t1.item_create_time
,       t1.item_create_datetime
,       t1.item_update_date
,       date_format(CURRENT_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss') AS etl_load_time
,       t1.erdat_pmonth
from
        dwd_o2c_customer_order_item_detail_info_di t1
join
        tmp_dws_o2c_customer_order_item_basic_indicator_di_partition_incr t2
on
        t1.erdat_pmonth = t2.erdat_pmonth
left join
        tmp_dws_o2c_customer_order_item_basic_indicator_di_pre t3
on
        t1.pk_order_no      = t3.pk_order_no
and     t1.pk_order_item_no = t3.pk_order_item_no
left join
        tmp_dws_o2c_customer_order_item_basic_indicator_di_pre3 t4
on
        t1.pk_order_no = t4.pk_order_no;
-- select * from tmp_dws_o2c_customer_order_item_basic_indicator_di_incr_2
-- where  daily_exchange_rate is null or net_value is null or net_value_lc is null or net_value_gc is null
-- or order_value_without_delivery_lc is null or open_order_value_dc is null
-- 3. pre step1 Calculates incr detail data
drop table if exists tmp_dws_o2c_customer_order_item_basic_indicator_di_incr_2;
create table if not exists tmp_dws_o2c_customer_order_item_basic_indicator_di_incr_2 as
select
        t1.pk_order_item_no
,       t1.pk_order_no
,       t1.order_type
,       t1.order_type_gen_descr
,       t1.sales_org
,       t1.distr_channel
,       t1.parent_doc_no
,       t1.parent_doc_item_no
,       t1.item_type
,       t1.order_category
,       t1.sales_distr
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
,       t1.item_category
,       t1.net_value
,       t1.net_value_lc
,
        case
        when
                t1.document_currency = 'EUR'
        then
                t1.net_value
        when
                t1.document_currency <> 'EUR'
        then
                (
                        case
                        when
                                t1.daily_exchange_rate < 0
                        then
                                net_value_lc / ABS(daily_exchange_rate)
                        else
                                net_value_lc * daily_exchange_rate
                        end)
        end as net_value_gc
,       t1.net_price
,       t1.item_qty
,       t1.exchange_rate
,       t1.daily_exchange_rate
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
,       t1.overall_credit_check_status
,       t1.distr_channel_pl
,       t1.item_del_qty
,       t1.item_bill_qty
,       t1.item_goods_issue_qty
,       t1.is_open_order
,       t1.open_del_qty
,       t1.order_qty_wo_del
,       t1.open_order_qty
, -- open order quantity
        t1.goods_issued_value
,       t1.order_value_wo_del
,
        case
        when
                t1.exchange_rate < 0
        then
                t1.order_value_wo_del / ABS(t1.exchange_rate)
        else
                t1.order_value_wo_del * t1.exchange_rate
        end as order_value_wo_del_lc
,       t1.billing_value
,
        case
        when
                t1.is_open_order            = 'Y'
                and t1.order_type_gen_descr <> 'Direct shipment'
        then
                coalesce(net_value - goods_issued_value,0)
        when
                t1.is_open_order            = 'Y'
                and t1.order_type_gen_descr = 'Direct shipment'
        then
                coalesce(net_value - billing_value,0)
        else
                0
        end as open_order_value
,
        case
        when
                t1.is_open_order is null
                or t1.order_type_gen_descr = 'Direct shipment'
        then
                0
        when
                t1.order_qty_wo_del            <> 0
                and t1.tgt_material_avail_date <= ${bdp.system.bizdate}
        then
                t1.order_value_wo_del
        else
                0
        end as backorder_value_dc_material_availability_date
,       t1.del_block_order
,       t1.bill_block_order
,       t1.del_block_schedule_line
,       t1.is_bill_block_order_item
,       t1.item_create_date
,       t1.item_create_time
,       t1.item_create_datetime
,       t1.item_update_date
,       t1.etl_load_time
,       t1.erdat_pmonth
from
        tmp_dws_o2c_customer_order_item_basic_indicator_di_incr_1 t1;
-- 3. pre step1 Calculates incr detail data
drop table if exists tmp_dws_o2c_customer_order_item_basic_indicator_di_incr_3;
create table if not exists tmp_dws_o2c_customer_order_item_basic_indicator_di_incr_3 as
select
        t1.pk_order_item_no
,       t1.pk_order_no
,       t1.order_type
,       t1.order_type_gen_descr
,       t1.sales_org
,       t1.distr_channel
,       t1.parent_doc_no
,       t1.parent_doc_item_no
,       t1.item_type
,       t1.order_category
,       t1.sales_distr
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
,       t1.is_bill_block_order_item
,       t1.del_block_schedule_line
,       t1.del_block_order
,       t1.cust_vendor_contact
,       t1.overall_item_status
,       t1.item_del_status
,       t1.item_related_bill_status
,       t1.overall_item_delivery_status
,       t1.item_packing_status
,       t1.item_goods_mvmt_status
,       t1.item_del_related_bill_status
,       t1.item_reject_status
,       t1.item_category
,       t1.net_value
,       t1.net_value_lc
,       t1.net_value_gc
,       t1.net_price
,       t1.item_qty
,       t1.exchange_rate
,       t1.daily_exchange_rate
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
,       t1.distr_channel_pl
,       t1.item_del_qty
,       t1.item_bill_qty
,       t1.item_goods_issue_qty
,       t1.is_open_order
,       t1.open_del_qty
,       t1.order_qty_wo_del
,       t1.open_order_qty
, -- open order quantity
        t1.goods_issued_value
,       t1.order_value_wo_del
,       t1.order_value_wo_del_lc
,
        case
        when
                t1.document_currency = 'EUR'
        then
                t1.order_value_wo_del
        when
                t1.document_currency <> 'EUR'
        then
                (
                        case
                        when
                                t1.daily_exchange_rate < 0
                        then
                                order_value_wo_del_lc / ABS(daily_exchange_rate)
                        else
                                order_value_wo_del_lc * daily_exchange_rate
                        end)
        else
                0
        end as order_value_wo_del_gc
,       t1.billing_value
,       t1.open_order_value
,
        case
        when
                t1.is_open_order = 'Y'
        then
                (
                        case
                        when
                                t1.exchange_rate < 0
                        then
                                t1.open_order_value / ABS(t1.exchange_rate)
                        else
                                t1.open_order_value * t1.exchange_rate
                        end )
        when
                t1.is_open_order is null
        then
                0
        end as open_order_value_lc
,
        case
        when
                t1.is_open_order is null
                or t1.order_type_gen_descr = 'Direct shipment'
        then
                0
        when
                t1.open_order_qty           <> 0
                and t1.tgt_goods_issue_date <= ${bdp.system.bizdate}
        then
                t1.open_order_value
        else
                0
        end as backorder_value_dc_goods_issue_date
,       t1.backorder_value_dc_material_availability_date
,
        case
        when
                t1.is_open_order is null
                or t1.order_type_gen_descr = 'Direct shipment'
        then
                0
        when
                t1.exchange_rate < 0
        then
                t1.backorder_value_dc_material_availability_date / ABS(t1.exchange_rate)
        else
                t1.backorder_value_dc_material_availability_date * t1.exchange_rate
        end as backorder_value_lc_material_availability_date
,
        case
        when
                t1.is_open_order is null
                or t1.order_type_gen_descr = 'Return'
        then
                0
        when
                t1.overall_credit_check_status in ('B'
                                                  , 'C')
        then
                t1.open_order_value
        else
                0
        end as open_order_value_dc_credit_blocked
,
        case
        when
                t1.is_open_order is null
                or t1.order_type_gen_descr in('Return'
                                              , 'Direct shipment')
        then
                0
        when
                t1.del_block_order is not null
                and trim(t1.del_block_order)<>''
        then
                t1.open_order_value
        else
                0
        end as open_order_value_dc_delivery_header_blocked
,
        case
        when
                t1.is_open_order is null
                or t1.order_type_gen_descr in ('Return'
                                              , 'Direct shipment')
        then
                0
        when
                t1.del_block_schedule_line is not null
                and trim(t1.del_block_schedule_line)<>''
        then
                t1.open_order_value
        else
                0
        end as open_order_value_dc_delivery_item_blocked
,
        case
        when
                t1.is_open_order is null
                or t1.order_type_gen_descr in('Standard-no billing'
                                              , 'Consignment'
                                              , 'Return')
        then
                0
        when
                (t1.bill_block_order is not null
                and trim(t1.bill_block_order)<>'')
                or (
                        t1.is_bill_block_order_item is not null
                        and trim(t1.is_bill_block_order_item)<>'')
        then
                t1.open_order_value
        else
                0
        end as open_order_value_dc_bill_blocked
,       t1.item_create_date
,       t1.item_create_time
,       t1.item_create_datetime
,       t1.item_update_date
,       t1.etl_load_time
,       t1.erdat_pmonth
from
        tmp_dws_o2c_customer_order_item_basic_indicator_di_incr_2 t1;
drop table if exists tmp_dws_o2c_customer_order_item_basic_indicator_di_incr_4;
create table if not exists tmp_dws_o2c_customer_order_item_basic_indicator_di_incr_4 as
select
        t1.pk_order_item_no
,       t1.pk_order_no
,       t1.order_type
,       t1.order_type_gen_descr
,       t1.sales_org
,       t1.distr_channel
,       t1.parent_doc_no
,       t1.parent_doc_item_no
,       t1.item_type
,       t1.order_category
,       t1.sales_distr
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
,       t1.is_bill_block_order_item
,       t1.del_block_schedule_line
,       t1.del_block_order
,       t1.cust_vendor_contact
,       t1.overall_item_status
,       t1.item_del_status
,       t1.item_related_bill_status
,       t1.overall_item_delivery_status
,       t1.item_packing_status
,       t1.item_goods_mvmt_status
,       t1.item_del_related_bill_status
,       t1.item_reject_status
,       t1.item_category
,       t1.net_value
,       t1.net_value_lc
,       t1.net_value_gc
,       t1.net_price
,       t1.item_qty
,       t1.exchange_rate
,       t1.daily_exchange_rate
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
,       t1.distr_channel_pl
,       t1.item_del_qty
,       t1.item_bill_qty
,       t1.item_goods_issue_qty
,       t1.is_open_order
,       t1.open_del_qty
,       t1.order_qty_wo_del
,       t1.open_order_qty
, -- open order quantity
        t1.goods_issued_value
,       t1.order_value_wo_del
,       t1.order_value_wo_del_lc
,       t1.order_value_wo_del_gc
,       t1.billing_value
,       t1.open_order_value
,       t1.open_order_value_lc
,
        case
        when
                t1.is_open_order         = 'Y'
                and t1.document_currency = 'EUR'
        then
                t1.open_order_value
        when
                t1.is_open_order         = 'Y'
                and t1.document_currency <> 'EUR'
        then
                (
                        case
                        when
                                t1.daily_exchange_rate < 0
                        then
                                open_order_value_lc / ABS(daily_exchange_rate)
                        else
                                open_order_value_lc * daily_exchange_rate
                        end)
        else
                0
        end as open_order_value_gc
,       t1.backorder_value_dc_goods_issue_date
,
        case
        when
                t1.is_open_order is null
                or t1.order_type_gen_descr = 'Direct shipment'
        then
                0
        when
                t1.exchange_rate < 0
        then
                backorder_value_dc_goods_issue_date / ABS(t1.exchange_rate)
        else
                backorder_value_dc_goods_issue_date * t1.exchange_rate
        end as backorder_value_lc_goods_issue_date
,       t1.backorder_value_dc_material_availability_date
,       t1.backorder_value_lc_material_availability_date
,
        case
        when
                t1.is_open_order is null
                or t1.order_type_gen_descr = 'Direct shipment'
        then
                0
        when
                t1.document_currency = 'EUR'
        then
                t1.backorder_value_dc_material_availability_date
        when
                t1.document_currency <> 'EUR'
        then
                (
                        case
                        when
                                t1.daily_exchange_rate < 0
                        then
                                t1.backorder_value_lc_material_availability_date / ABS(t1.daily_exchange_rate)
                        else
                                t1.backorder_value_lc_material_availability_date * t1.daily_exchange_rate
                        end )
        end as backorder_value_gc_material_availability_date
,       t1.open_order_value_dc_credit_blocked
,
        case
        when
                t1.is_open_order is null
                or t1.order_type_gen_descr = 'Return'
        then
                0
        when
                t1.exchange_rate < 0
        then
                t1.open_order_value_dc_credit_blocked / ABS(t1.exchange_rate)
        else
                t1.open_order_value_dc_credit_blocked * t1.exchange_rate
        end as open_order_value_lc_credit_blocked
,       t1.open_order_value_dc_delivery_header_blocked
,
        case
        when
                t1.is_open_order is null
                or t1.order_type_gen_descr in ('Return'
                                              , 'Direct shipment')
        then
                0
        when
                t1.exchange_rate < 0
        then
                t1.open_order_value_dc_delivery_header_blocked / ABS(t1.exchange_rate)
        else
                t1.open_order_value_dc_delivery_header_blocked * t1.exchange_rate
        end as open_order_value_lc_delivery_header_blocked
,       t1.open_order_value_dc_delivery_item_blocked
,
        case
        when
                t1.is_open_order is null
                or t1.order_type_gen_descr in ('Return'
                                              , 'Direct shipment')
        then
                0
        when
                t1.exchange_rate < 0
        then
                t1.open_order_value_dc_delivery_item_blocked / ABS(t1.exchange_rate)
        else
                t1.open_order_value_dc_delivery_item_blocked * t1.exchange_rate
        end as open_order_value_lc_delivery_item_blocked
,       t1.open_order_value_dc_bill_blocked
,
        case
        when
                t1.is_open_order is null
                or t1.order_type_gen_descr in ('Standard-no billing'
                                              , 'Consignment'
                                              , 'Return')
        then
                0
        when
                t1.exchange_rate < 0
        then
                t1.open_order_value_dc_bill_blocked / ABS(t1.exchange_rate)
        else
                t1.open_order_value_dc_bill_blocked * t1.exchange_rate
        end as open_order_value_lc_bill_blocked
,
        case
        when
                t1.is_open_order = 'Y'
        then
                (
                        case
                        when
                                t1.open_order_value_dc_credit_blocked             <> 0
                                or t1.open_order_value_dc_delivery_header_blocked <> 0
                                or t1.open_order_value_dc_delivery_item_blocked   <> 0
                                or t1.open_order_value_dc_bill_blocked            <> 0
                        then
                                open_order_value
                        else
                                0
                        end )
        when
                t1.is_open_order is null
        then
                0
        end as open_order_value_dc_blocked
,
        case
        when
                t1.is_open_order = 'Y'
        then
                (
                        case
                        when
                                t1.open_order_value_dc_credit_blocked              = 0
                                and t1.open_order_value_dc_delivery_header_blocked = 0
                                and t1.open_order_value_dc_delivery_item_blocked   = 0
                                and t1.open_order_value_dc_bill_blocked            = 0
                        then
                                t1.open_order_value
                        else
                                0
                        end)
        when
                t1.is_open_order is null
        then
                0
        end as open_order_value_dc_unblocked
,       t1.item_create_date
,       t1.item_create_time
,       t1.item_create_datetime
,       t1.item_update_date
,       t1.etl_load_time
,       t1.erdat_pmonth
from
        tmp_dws_o2c_customer_order_item_basic_indicator_di_incr_3 t1;
drop table if exists tmp_dws_o2c_customer_order_item_basic_indicator_di_incr_5;
create table if not exists tmp_dws_o2c_customer_order_item_basic_indicator_di_incr_5 as
select
        t1.pk_order_item_no
,       t1.pk_order_no
,       t1.order_type
,       t1.order_type_gen_descr
,       t1.sales_org
,       t1.distr_channel
,       t1.parent_doc_no
,       t1.parent_doc_item_no
,       t1.item_type
,       t1.order_category
,       t1.sales_distr
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
,       t1.is_bill_block_order_item
,       t1.del_block_schedule_line
,       t1.del_block_order
,       t1.cust_vendor_contact
,       t1.overall_item_status
,       t1.item_del_status
,       t1.item_related_bill_status
,       t1.overall_item_delivery_status
,       t1.item_packing_status
,       t1.item_goods_mvmt_status
,       t1.item_del_related_bill_status
,       t1.item_reject_status
,       t1.item_category
,       t1.net_value
,       t1.net_value_lc
,       t1.net_value_gc
,       t1.net_price
,       t1.item_qty
,       t1.exchange_rate
,       t1.daily_exchange_rate
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
,       t1.distr_channel_pl
,       t1.item_del_qty
,       t1.item_bill_qty
,       t1.item_goods_issue_qty
,       t1.is_open_order
,       t1.open_del_qty
,       t1.order_qty_wo_del
,       t1.open_order_qty
, -- open order quantity
        t1.goods_issued_value
,       t1.order_value_wo_del
,       t1.order_value_wo_del_lc
,       t1.order_value_wo_del_gc
,       t1.billing_value
,       t1.open_order_value
,       t1.open_order_value_lc
,       t1.open_order_value_gc
,       t1.backorder_value_dc_goods_issue_date
,       t1.backorder_value_lc_goods_issue_date
,
        case
        when
                t1.is_open_order is null
                or t1.order_type_gen_descr='Direct shipment'
        then
                0
        when
                t1.document_currency = 'EUR'
        then
                t1.backorder_value_dc_goods_issue_date
        when
                t1.document_currency <> 'EUR'
        then
                (
                        case
                        when
                                t1.daily_exchange_rate < 0
                        then
                                t1.backorder_value_lc_goods_issue_date / ABS(t1.daily_exchange_rate)
                        else
                                t1.backorder_value_lc_goods_issue_date * t1.daily_exchange_rate
                        end)
        end as backorder_value_gc_goods_issue_date
,
        case
        when
                t1.is_open_order = 'Y'
        then
                (
                        case
                        when
                                t1.backorder_value_lc_goods_issue_date is null
                                or t1.backorder_value_lc_goods_issue_date = 0
                        then
                                0
                        else
                                1
                        end )
        when
                t1.is_open_order is null
        then
                0
        end as is_backorder
,       t1.backorder_value_dc_material_availability_date
,       t1.backorder_value_lc_material_availability_date
,       t1.backorder_value_gc_material_availability_date
,       t1.open_order_value_dc_credit_blocked
,       t1.open_order_value_lc_credit_blocked
,
        case
        when
                t1.is_open_order is null
                or t1.order_type_gen_descr = 'Return'
        then
                0
        when
                t1.document_currency = 'EUR'
        then
                t1.open_order_value_dc_credit_blocked
        when
                t1.document_currency <> 'EUR'
        then
                (
                        case
                        when
                                t1.daily_exchange_rate < 0
                        then
                                t1.open_order_value_lc_credit_blocked / ABS(t1.daily_exchange_rate)
                        else
                                t1.open_order_value_lc_credit_blocked * t1.daily_exchange_rate
                        end )
        else
                0
        end as open_order_value_gc_credit_blocked
,       t1.open_order_value_dc_delivery_header_blocked
,       t1.open_order_value_lc_delivery_header_blocked
,
        case
        when
                t1.is_open_order is null
                or t1.order_type_gen_descr in ('Return'
                                              , 'Direct shipment')
        then
                0
        when
                t1.document_currency = 'EUR'
        then
                t1.open_order_value_dc_delivery_header_blocked
        when
                t1.document_currency <> 'EUR'
        then
                (
                        case
                        when
                                t1.daily_exchange_rate < 0
                        then
                                t1.open_order_value_lc_delivery_header_blocked / ABS(t1.daily_exchange_rate)
                        else
                                t1.open_order_value_lc_delivery_header_blocked * t1.daily_exchange_rate
                        end)
        else
                0
        end as open_order_value_gc_delivery_header_blocked
,       t1.open_order_value_dc_delivery_item_blocked
,       t1.open_order_value_lc_delivery_item_blocked
,
        case
        when
                t1.is_open_order is null
                or t1.order_type_gen_descr in ('Return'
                                              , 'Direct shipment')
        then
                0
        when
                t1.document_currency = 'EUR'
        then
                t1.open_order_value_dc_delivery_item_blocked
        when
                t1.document_currency <> 'EUR'
        then
                (
                        case
                        when
                                t1.daily_exchange_rate < 0
                        then
                                t1.open_order_value_lc_delivery_item_blocked / ABS(t1.daily_exchange_rate)
                        else
                                t1.open_order_value_lc_delivery_item_blocked * t1.daily_exchange_rate
                        end)
        else
                0
        end as open_order_value_gc_delivery_item_blocked
,       t1.open_order_value_dc_bill_blocked
,       t1.open_order_value_lc_bill_blocked
,
        case
        when
                t1.is_open_order is null
                or t1.order_type_gen_descr in ('Standard-no billing'
                                              , 'Consignment'
                                              , 'Return')
        then
                0
        when
                t1.document_currency = 'EUR'
        then
                t1.open_order_value_dc_bill_blocked
        when
                t1.document_currency <> 'EUR'
        then
                (
                        case
                        when
                                t1.daily_exchange_rate < 0
                        then
                                t1.open_order_value_lc_bill_blocked / ABS(t1.daily_exchange_rate)
                        else
                                t1.open_order_value_lc_bill_blocked * t1.daily_exchange_rate
                        end)
        else
                0
        end as open_order_value_gc_bill_blocked
,       t1.open_order_value_dc_blocked
,
        case
        when
                t1.is_open_order = 'Y'
        then
                (
                        case
                        when
                                t1.exchange_rate < 0
                        then
                                t1.open_order_value_dc_blocked /ABS(t1.exchange_rate)
                        else
                                t1.open_order_value_dc_blocked * t1.exchange_rate
                        end)
        when
                t1.is_open_order is null
        then
                0
        end as open_order_value_lc_blocked
,       t1.open_order_value_dc_unblocked
,
        case
        when
                t1.is_open_order = 'Y'
        then
                (
                        case
                        when
                                t1.exchange_rate < 0
                        then
                                t1.open_order_value_dc_unblocked / ABS(t1.exchange_rate)
                        else
                                t1.open_order_value_dc_unblocked * t1.exchange_rate
                        end)
        else
                0
        end as open_order_value_lc_unblocked
,
        case
        when
                t1.is_open_order is null
                or t1.order_type_gen_descr = 'Direct shipment'
        then
                0
        when
                t1.open_order_qty           > 0
                and t1.tgt_goods_issue_date <= ${bdp.system.bizdate}
        then
                t1.open_order_value_dc_unblocked
        else
                0
        end as backorder_value_dc_unblocked
,
        case
        when
                t1.is_open_order ='Y'
        then
                (
                        case
                        when
                                t1.open_order_value_lc_delivery_header_blocked is null
                                or t1.open_order_value_lc_delivery_header_blocked = 0
                        then
                                0
                        else
                                1
                        end)
        when
                t1.is_open_order is null
        then
                0
        end as open_order_delivery_block_header_indicator
,
        case
        when
                t1.is_open_order ='Y'
        then
                (
                        case
                        when
                                t1.open_order_value_lc_delivery_item_blocked is null
                                or t1.open_order_value_lc_delivery_item_blocked = 0
                        then
                                0
                        else
                                1
                        end)
        when
                t1.is_open_order is null
        then
                0
        end as open_order_delivery_block_item_indicator
,
        case
        when
                t1.is_open_order ='Y'
        then
                (
                        case
                        when
                                t1.open_order_value_lc_bill_blocked is null
                                or t1.open_order_value_lc_bill_blocked = 0
                        then
                                0
                        else
                                1
                        end)
        when
                t1.is_open_order is null
        then
                0
        end as open_order_bill_block_indicator
,       t1.item_create_date
,       t1.item_create_time
,       t1.item_create_datetime
,       t1.item_update_date
,       t1.etl_load_time
,       t1.erdat_pmonth
from
        tmp_dws_o2c_customer_order_item_basic_indicator_di_incr_4 t1;
drop table if exists tmp_dws_o2c_customer_order_item_basic_indicator_di_incr_6;
create table if not exists tmp_dws_o2c_customer_order_item_basic_indicator_di_incr_6 as
select
        t1.pk_order_item_no
,       t1.pk_order_no
,       t1.order_type
,       t1.order_type_gen_descr
,       t1.sales_org
,       t1.distr_channel
,       t1.parent_doc_no
,       t1.parent_doc_item_no
,       t1.item_type
,       t1.order_category
,       t1.sales_distr
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
,       t1.is_bill_block_order_item
,       t1.del_block_schedule_line
,       t1.del_block_order
,       t1.cust_vendor_contact
,       t1.overall_item_status
,       t1.item_del_status
,       t1.item_related_bill_status
,       t1.overall_item_delivery_status
,       t1.item_packing_status
,       t1.item_goods_mvmt_status
,       t1.item_del_related_bill_status
,       t1.item_reject_status
,       t1.item_category
,       t1.net_value
,       t1.net_value_lc
,       t1.net_value_gc
,       t1.net_price
,       t1.item_qty
,       t1.exchange_rate
,       t1.daily_exchange_rate
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
,       t1.distr_channel_pl
,       t1.item_del_qty
,       t1.item_bill_qty
,       t1.item_goods_issue_qty
,       t1.is_open_order
,       t1.open_del_qty
,       t1.order_qty_wo_del
,       t1.open_order_qty
, -- open order quantity
        t1.goods_issued_value
,       t1.order_value_wo_del
,       t1.order_value_wo_del_lc
,       t1.order_value_wo_del_gc
,       t1.billing_value
,       t1.open_order_value
,       t1.open_order_value_lc
,       t1.open_order_value_gc
,       t1.backorder_value_dc_goods_issue_date
,       t1.backorder_value_lc_goods_issue_date
,
        case
        when
                t1.is_open_order = 'Y'
        then
                (
                        case
                        when
                                t1.backorder_value_lc_goods_issue_date is null
                                or t1.backorder_value_lc_goods_issue_date = 0
                        then
                                0
                        else
                                1
                        end )
        when
                t1.is_open_order is null
        then
                0
        end as is_backorder
,       t1.backorder_value_gc_goods_issue_date
,       t1.backorder_value_dc_material_availability_date
,       t1.backorder_value_lc_material_availability_date
,       t1.backorder_value_gc_material_availability_date
,       t1.open_order_value_dc_credit_blocked
,       t1.open_order_value_lc_credit_blocked
,       t1.open_order_value_gc_credit_blocked
,       t1.open_order_value_dc_delivery_header_blocked
,       t1.open_order_value_lc_delivery_header_blocked
,       t1.open_order_value_gc_delivery_header_blocked
,       t1.open_order_value_dc_delivery_item_blocked
,       t1.open_order_value_lc_delivery_item_blocked
,       t1.open_order_value_gc_delivery_item_blocked
,       t1.open_order_value_dc_bill_blocked
,       t1.open_order_value_lc_bill_blocked
,       t1.open_order_value_gc_bill_blocked
,       t1.open_order_value_dc_blocked
,       t1.open_order_value_lc_blocked
,
        case
        when
                t1.is_open_order         = 'Y'
                and t1.document_currency = 'EUR'
        then
                t1.open_order_value_dc_blocked
        when
                t1.is_open_order         = 'Y'
                and t1.document_currency <> 'EUR'
        then
                (
                        case
                        when
                                t1.daily_exchange_rate < 0
                        then
                                open_order_value_lc_blocked / ABS(t1.daily_exchange_rate)
                        else
                                open_order_value_lc_blocked * t1.daily_exchange_rate
                        end )
        when
                t1.is_open_order is null
        then
                0
        end as open_order_value_gc_blocked
,       t1.open_order_value_dc_unblocked
,       t1.open_order_value_lc_unblocked
,
        case
        when
                t1.is_open_order         = 'Y'
                and t1.document_currency = 'EUR'
        then
                t1.open_order_value_dc_unblocked
        when
                t1.is_open_order         = 'Y'
                and t1.document_currency <> 'EUR'
        then
                (
                        case
                        when
                                t1.daily_exchange_rate < 0
                        then
                                open_order_value_lc_unblocked / ABS(t1.daily_exchange_rate)
                        else
                                open_order_value_lc_unblocked * t1.daily_exchange_rate
                        end )
        when
                t1.is_open_order is null
        then
                0
        end as open_order_value_gc_unblocked
,       t1.backorder_value_dc_unblocked
,
        case
        when
                t1.is_open_order is null
                or t1.order_type_gen_descr = 'Direct shipment"'
        then
                0
        when
                t1.exchange_rate < 0
        then
                t1.backorder_value_dc_unblocked / ABS(t1.exchange_rate)
        else
                t1.backorder_value_dc_unblocked * t1.exchange_rate
        end as backorder_value_lc_unblocked
,
        case
        when
                t1.is_open_order ='Y'
        then
                (
                        case
                        when
                                t1.open_order_value_lc_blocked is null
                                or t1.open_order_value_lc_blocked = 0
                        then
                                0
                        else
                                1
                        end)
        when
                t1.is_open_order is null
        then
                0
        end as is_block_order
,       t1.open_order_delivery_block_header_indicator
,       t1.open_order_delivery_block_item_indicator
,       t1.open_order_bill_block_indicator
,       t1.item_create_date
,       t1.item_create_time
,       t1.item_create_datetime
,       t1.item_update_date
,       t1.etl_load_time
,       t1.erdat_pmonth
from
        tmp_dws_o2c_customer_order_item_basic_indicator_di_incr_5 t1;
drop table if exists tmp_dws_o2c_customer_order_item_basic_indicator_di_incr_7;
create table if not exists tmp_dws_o2c_customer_order_item_basic_indicator_di_incr_7 as
select
        t1.pk_order_item_no
,       t1.pk_order_no
,       t1.order_type
,       t1.order_type_gen_descr
,       t1.sales_org
,       t1.distr_channel
,       t1.parent_doc_no
,       t1.parent_doc_item_no
,       t1.item_type
,       t1.order_category
,       t1.sales_distr
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
,       t1.is_bill_block_order_item
,       t1.del_block_schedule_line
,       t1.del_block_order
,       t1.cust_vendor_contact
,       t1.overall_item_status
,       t1.item_del_status
,       t1.item_related_bill_status
,       t1.overall_item_delivery_status
,       t1.item_packing_status
,       t1.item_goods_mvmt_status
,       t1.item_del_related_bill_status
,       t1.item_reject_status
,       t1.item_category
,       cast(t1.net_value as decimal(38,2))           as net_value
,       cast(t1.net_value_lc as decimal(38,2))        as net_value_lc
,       cast(t1.net_value_gc as decimal(38,2))        as net_value_gc
,       cast(t1.net_price as decimal(38,2))           as net_price
,       cast(t1.item_qty as decimal(28,2))            as item_qty
,       cast(t1.exchange_rate as decimal(38,5))       as exchange_rate
,       cast(t1.daily_exchange_rate as decimal(38,5)) as daily_exchange_rate
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
,       t1.distr_channel_pl
,       cast(t1.item_del_qty as decimal(28,2))         as item_del_qty
,       cast(t1.item_bill_qty as decimal(28,2))        as item_bill_qty
,       cast(t1.item_goods_issue_qty as decimal(28,2)) as item_goods_issue_qty
,       t1.is_open_order
,       cast(t1.open_del_qty as decimal(28,2))     as open_del_qty
,       cast(t1.order_qty_wo_del as decimal(28,2)) as order_qty_wo_del
,       cast(t1.open_order_qty as decimal(28,2))   as open_order_qty
, -- open order quantity
        cast(t1.goods_issued_value as decimal(38,2))                            as goods_issued_value
,       cast(t1.order_value_wo_del as decimal(38,2))                            as order_value_wo_del
,       cast(t1.order_value_wo_del_lc as decimal(38,2))                         as order_value_wo_del_lc
,       cast(t1.order_value_wo_del_gc as decimal(38,2))                         as order_value_wo_del_gc
,       cast(t1.billing_value as decimal(38,2))                                 as billing_value
,       cast(t1.open_order_value as decimal(38,2))                              as open_order_value
,       cast(t1.open_order_value_lc as decimal(38,2))                           as open_order_value_lc
,       cast(t1.open_order_value_gc as decimal(38,2))                           as open_order_value_gc
,       cast(t1.backorder_value_dc_goods_issue_date as decimal(38,2))           as backorder_value_tgid
,       cast(t1.backorder_value_lc_goods_issue_date as decimal(38,2))           as backorder_value_tgid_lc
,       cast(t1.is_backorder as int)                                            as is_backorder
,       cast(t1.backorder_value_gc_goods_issue_date as decimal(38,2))           as backorder_value_tgid_gc
,       cast(t1.backorder_value_dc_material_availability_date as decimal(38,2)) as backorder_value_tmad
,       cast(t1.backorder_value_lc_material_availability_date as decimal(38,2)) as backorder_value_tmad_lc
,       cast(t1.backorder_value_gc_material_availability_date as decimal(38,2)) as backorder_value_tmad_gc
,       cast(t1.open_order_value_dc_credit_blocked as decimal(38,2))            as open_order_val_credit_blocked
,       cast(t1.open_order_value_lc_credit_blocked as decimal(38,2))            as open_order_val_credit_blocked_lc
,       cast(t1.open_order_value_gc_credit_blocked as decimal(38,2))            as open_order_val_credit_blocked_gc
,       cast(t1.open_order_value_dc_delivery_header_blocked as decimal(38,2))   as open_order_val_del_header_blocked
,       cast(t1.open_order_value_lc_delivery_header_blocked as decimal(38,2))   as open_order_val_del_header_blocked_lc
,       cast(t1.open_order_value_gc_delivery_header_blocked as decimal(38,2))   as open_order_val_del_header_blocked_gc
,       cast(t1.open_order_value_dc_delivery_item_blocked as decimal(38,2))     as open_order_val_del_item_blocked
,       cast(t1.open_order_value_lc_delivery_item_blocked as decimal(38,2))     as open_order_val_del_item_blocked_lc
,       cast(t1.open_order_value_gc_delivery_item_blocked as decimal(38,2))     as open_order_val_del_item_blocked_gc
,       cast(t1.open_order_value_dc_bill_blocked as decimal(38,2))              as open_order_val_bill_blocked
,       cast(t1.open_order_value_lc_bill_blocked as decimal(38,2))              as open_order_val_bill_blocked_lc
,       cast(t1.open_order_value_gc_bill_blocked as decimal(38,2))              as open_order_val_bill_blocked_gc
,       cast(t1.open_order_value_dc_blocked as decimal(38,2))                   as open_order_val_blocked
,       cast(t1.open_order_value_lc_blocked as decimal(38,2))                   as open_order_val_blocked_lc
,       cast(t1.open_order_value_gc_blocked as decimal(38,2))                   as open_order_val_blocked_gc
,       cast(t1.open_order_value_dc_unblocked as decimal(38,2))                 as open_order_val_unblocked
,       cast(t1.open_order_value_gc_unblocked as decimal(38,2))                 as open_order_val_unblocked_lc
,       cast(t1.open_order_value_lc_unblocked as decimal(38,2))                 as open_order_val_unblocked_gc
,       cast(t1.backorder_value_dc_unblocked as decimal(38,2))                  as backorder_value_unblocked
,       cast(t1.backorder_value_lc_unblocked as decimal(38,2))                  as backorder_value_unblocked_lc
,       cast(
                case
                when
                        t1.is_open_order is null
                        or t1.order_type_gen_descr = 'Direct shipment'
                then
                        0
                when
                        t1.document_currency = 'EUR'
                then
                        t1.backorder_value_dc_unblocked
                when
                        t1.document_currency <> 'EUR'
                then
                        (
                                case
                                when
                                        t1.daily_exchange_rate < 0
                                then
                                        t1.backorder_value_lc_unblocked / ABS(t1.daily_exchange_rate)
                                else
                                        t1.backorder_value_lc_unblocked * t1.daily_exchange_rate
                                end)
                end as decimal(38,2))                              as backorder_value_unblocked_gc
,       cast(t1.is_block_order as int)                             as open_order_block_ind
,       cast(t1.open_order_delivery_block_header_indicator as int) as open_order_del_block_header_ind
,       cast(t1.open_order_delivery_block_item_indicator as int)   as open_order_del_block_item_ind
,       cast(t1.open_order_bill_block_indicator as int)            as open_order_bill_block_ind
,       t1.item_create_date
,       t1.item_create_time
,       t1.item_create_datetime
,       t1.item_update_date
,       t1.etl_load_time
,       t1.erdat_pmonth
from
        tmp_dws_o2c_customer_order_item_basic_indicator_di_incr_6 t1;
--4. no changed data in incr data partition
drop table if exists tmp_dws_o2c_customer_order_item_basic_indicator_di_not_exits;
create table if not exists tmp_dws_o2c_customer_order_item_basic_indicator_di_not_exits as
select
        t1.pk_order_no
,       t1.pk_order_item_no
,       t1.order_type
,       t1.order_type_gen_descr
,       t1.order_category
,       t1.sales_org
,       t1.distr_channel
,       t1.sold_to_party
,       t1.ship_to_party
,       t1.payer
,       t1.material_no
,       t1.storage_location
,       t1.delivery_plant_code
,       t1.order_doc_date
,       t1.order_doc_time
,       t1.order_doc_datetime
,       t1.item_create_date
,       t1.item_create_time
,       t1.item_create_datetime
,       t1.item_update_date
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
,       t1.pricing_date
,       t1.overall_item_status
,       t1.item_del_status
,       t1.item_related_bill_status
,       t1.overall_item_delivery_status
,       t1.item_packing_status
,       t1.item_goods_mvmt_status
,       t1.item_del_related_bill_status
,       t1.item_reject_status
,       t1.item_type
,       t1.item_category
,       t1.sales_distr
,       t1.document_currency
,       t1.local_currency
,       t1.product_attribute
,       t1.material_entered
,       t1.order_material_descr
,       t1.cust_material_no
,       t1.target_quantity
,       t1.material_sold_sales_unit
,       t1.base_unit_meas
,       t1.cond_ref_unit_meas
,       t1.parent_doc_no
,       t1.parent_doc_item_no
,       t1.price_list
,       t1.price_group
,       t1.cust_pricing_group
,       t1.cond_pricing_unit
,       t1.pricing_cond_subtotal_1
,       t1.pricing_cond_subtotal_2
,       t1.pricing_cond_subtotal_3
,       t1.pricing_cond_subtotal_4
,       t1.pricing_cond_subtotal_5
,       t1.pricing_cond_subtotal_6
,       t1.reject_reason
,       t1.cust_po_item_no
,       t1.shipping_point
,       t1.del_route
,       t1.wbs_element
,       t1.incoterm
,       t1.incoterm2
,       t1.payment_term
,       t1.cust_vendor_contact
,       t1.item_create_by
,       t1.is_bill_block_order_item
,       t1.del_block_schedule_line
,       t1.del_block_order
,       t1.exchange_rate
,       t1.net_value
,       t1.net_price
,       t1.item_qty
,       t1.item_del_qty
,       t1.item_bill_qty
,       t1.item_goods_issue_qty
,       t1.daily_exchange_rate
,       t1.net_value_lc
,       t1.net_value_gc
,       t1.is_open_order
,       t1.open_del_qty
,       t1.order_qty_wo_del
,       t1.open_order_qty
,       t1.goods_issued_value
,       t1.order_value_wo_del
,       t1.order_value_wo_del_lc
,       t1.order_value_wo_del_gc
,       t1.billing_value
,       t1.open_order_value
,       t1.open_order_value_lc
,       t1.open_order_value_gc
,       t1.backorder_value_tgid
,       t1.backorder_value_tgid_lc
,       t1.backorder_value_tgid_gc
,       t1.backorder_value_tmad
,       t1.backorder_value_tmad_lc
,       t1.backorder_value_tmad_gc
,       t1.open_order_val_credit_blocked
,       t1.open_order_val_credit_blocked_lc
,       t1.open_order_val_credit_blocked_gc
,       t1.open_order_val_del_header_blocked
,       t1.open_order_val_del_header_blocked_lc
,       t1.open_order_val_del_header_blocked_gc
,       t1.open_order_val_del_item_blocked
,       t1.open_order_val_del_item_blocked_lc
,       t1.open_order_val_del_item_blocked_gc
,       t1.open_order_val_bill_blocked
,       t1.open_order_val_bill_blocked_lc
,       t1.open_order_val_bill_blocked_gc
,       t1.open_order_val_blocked
,       t1.open_order_val_blocked_lc
,       t1.open_order_val_blocked_gc
,       t1.open_order_val_unblocked
,       t1.open_order_val_unblocked_lc
,       t1.open_order_val_unblocked_gc
,       t1.backorder_value_unblocked
,       t1.backorder_value_unblocked_lc
,       t1.backorder_value_unblocked_gc
,       t1.open_order_block_ind
,       t1.open_order_del_block_header_ind
,       t1.open_order_del_block_item_ind
,       t1.open_order_bill_block_ind
,       t1.is_backorder
,       t1.distr_channel_pl
,       date_format(CURRENT_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss') AS etl_load_time
,       t1.erdat_pmonth
from
        dws_o2c_customer_order_item_basic_indicator_di t1
join
        tmp_dws_o2c_customer_order_item_basic_indicator_di_partition_incr t2
on
        t1.erdat_pmonth = t2.erdat_pmonth
left join
        tmp_dws_o2c_customer_order_item_basic_indicator_di_incr_7 t3
on
        t1.pk_order_item_no = t3.pk_order_item_no
and     t1.pk_order_no      = t3.pk_order_no
where
        t3.pk_order_item_no is null
and     t3.pk_order_no is null;
-- 5. merge incr and no changed data insert target table
set hive.tez.container.size=8096;
INSERT  overwrite TABLE dws_o2c_customer_order_item_basic_indicator_di PARTITION
        (
                erdat_pmonth
        )
SELECT
        pk_order_no
,       pk_order_item_no
,       order_type
,       order_type_gen_descr
,       order_category
,       sales_org
,       distr_channel
,       sold_to_party
,       ship_to_party
,       payer
,       material_no
,       storage_location
,       delivery_plant_code
,       order_doc_date
,       order_doc_time
,       order_doc_datetime
,       item_create_date
,       item_create_time
,       item_create_datetime
,       item_update_date
,       initial_req_del_date
,       cust_req_del_date
,       sys_cfm_del_date
,       cfm_material_avail_date
,       cfm_goods_issue_date
,       tgt_material_avail_date
,       tgt_goods_issue_date
,       tgt_del_date
,       planned_material_avail_date
,       planned_goods_issue_date
,       planned_del_date
,       downgrade_date
,       downgrade_time
,       pricing_date
,       overall_item_status
,       item_del_status
,       item_related_bill_status
,       overall_item_delivery_status
,       item_packing_status
,       item_goods_mvmt_status
,       item_del_related_bill_status
,       item_reject_status
,       item_type
,       item_category
,       sales_distr
,       document_currency
,       local_currency
,       product_attribute
,       material_entered
,       order_material_descr
,       cust_material_no
,       target_quantity
,       material_sold_sales_unit
,       base_unit_meas
,       cond_ref_unit_meas
,       parent_doc_no
,       parent_doc_item_no
,       price_list
,       price_group
,       cust_pricing_group
,       cond_pricing_unit
,       pricing_cond_subtotal_1
,       pricing_cond_subtotal_2
,       pricing_cond_subtotal_3
,       pricing_cond_subtotal_4
,       pricing_cond_subtotal_5
,       pricing_cond_subtotal_6
,       reject_reason
,       cust_po_item_no
,       shipping_point
,       del_route
,       wbs_element
,       incoterm
,       incoterm2
,       payment_term
,       cust_vendor_contact
,       item_create_by
,       is_bill_block_order_item
,       del_block_schedule_line
,       del_block_order
,       exchange_rate
,       net_value
,       net_price
,       item_qty
,       item_del_qty
,       item_bill_qty
,       item_goods_issue_qty
,       daily_exchange_rate
,       net_value_lc
,       net_value_gc
,       is_open_order
,       open_del_qty
,       order_qty_wo_del
,       open_order_qty
,       goods_issued_value
,       order_value_wo_del
,       order_value_wo_del_lc
,       order_value_wo_del_gc
,       billing_value
,       open_order_value
,       open_order_value_lc
,       open_order_value_gc
,       backorder_value_tgid
,       backorder_value_tgid_lc
,       backorder_value_tgid_gc
,       backorder_value_tmad
,       backorder_value_tmad_lc
,       backorder_value_tmad_gc
,       open_order_val_credit_blocked
,       open_order_val_credit_blocked_lc
,       open_order_val_credit_blocked_gc
,       open_order_val_del_header_blocked
,       open_order_val_del_header_blocked_lc
,       open_order_val_del_header_blocked_gc
,       open_order_val_del_item_blocked
,       open_order_val_del_item_blocked_lc
,       open_order_val_del_item_blocked_gc
,       open_order_val_bill_blocked
,       open_order_val_bill_blocked_lc
,       open_order_val_bill_blocked_gc
,       open_order_val_blocked
,       open_order_val_blocked_lc
,       open_order_val_blocked_gc
,       open_order_val_unblocked
,       open_order_val_unblocked_lc
,       open_order_val_unblocked_gc
,       backorder_value_unblocked
,       backorder_value_unblocked_lc
,       backorder_value_unblocked_gc
,       open_order_block_ind
,       open_order_del_block_header_ind
,       open_order_del_block_item_ind
,       open_order_bill_block_ind
,       is_backorder
,       distr_channel_pl
,       etl_load_time
,       erdat_pmonth
from
        tmp_dws_o2c_customer_order_item_basic_indicator_di_incr_7

union all

select
        pk_order_no
,       pk_order_item_no
,       order_type
,       order_type_gen_descr
,       order_category
,       sales_org
,       distr_channel
,       sold_to_party
,       ship_to_party
,       payer
,       material_no
,       storage_location
,       delivery_plant_code
,       order_doc_date
,       order_doc_time
,       order_doc_datetime
,       item_create_date
,       item_create_time
,       item_create_datetime
,       item_update_date
,       initial_req_del_date
,       cust_req_del_date
,       sys_cfm_del_date
,       cfm_material_avail_date
,       cfm_goods_issue_date
,       tgt_material_avail_date
,       tgt_goods_issue_date
,       tgt_del_date
,       planned_material_avail_date
,       planned_goods_issue_date
,       planned_del_date
,       downgrade_date
,       downgrade_time
,       pricing_date
,       overall_item_status
,       item_del_status
,       item_related_bill_status
,       overall_item_delivery_status
,       item_packing_status
,       item_goods_mvmt_status
,       item_del_related_bill_status
,       item_reject_status
,       item_type
,       item_category
,       sales_distr
,       document_currency
,       local_currency
,       product_attribute
,       material_entered
,       order_material_descr
,       cust_material_no
,       target_quantity
,       material_sold_sales_unit
,       base_unit_meas
,       cond_ref_unit_meas
,       parent_doc_no
,       parent_doc_item_no
,       price_list
,       price_group
,       cust_pricing_group
,       cond_pricing_unit
,       pricing_cond_subtotal_1
,       pricing_cond_subtotal_2
,       pricing_cond_subtotal_3
,       pricing_cond_subtotal_4
,       pricing_cond_subtotal_5
,       pricing_cond_subtotal_6
,       reject_reason
,       cust_po_item_no
,       shipping_point
,       del_route
,       wbs_element
,       incoterm
,       incoterm2
,       payment_term
,       cust_vendor_contact
,       item_create_by
,       is_bill_block_order_item
,       del_block_schedule_line
,       del_block_order
,       exchange_rate
,       net_value
,       net_price
,       item_qty
,       item_del_qty
,       item_bill_qty
,       item_goods_issue_qty
,       daily_exchange_rate
,       net_value_lc
,       net_value_gc
,       is_open_order
,       open_del_qty
,       order_qty_wo_del
,       open_order_qty
,       goods_issued_value
,       order_value_wo_del
,       order_value_wo_del_lc
,       order_value_wo_del_gc
,       billing_value
,       open_order_value
,       open_order_value_lc
,       open_order_value_gc
,       backorder_value_tgid
,       backorder_value_tgid_lc
,       backorder_value_tgid_gc
,       backorder_value_tmad
,       backorder_value_tmad_lc
,       backorder_value_tmad_gc
,       open_order_val_credit_blocked
,       open_order_val_credit_blocked_lc
,       open_order_val_credit_blocked_gc
,       open_order_val_del_header_blocked
,       open_order_val_del_header_blocked_lc
,       open_order_val_del_header_blocked_gc
,       open_order_val_del_item_blocked
,       open_order_val_del_item_blocked_lc
,       open_order_val_del_item_blocked_gc
,       open_order_val_bill_blocked
,       open_order_val_bill_blocked_lc
,       open_order_val_bill_blocked_gc
,       open_order_val_blocked
,       open_order_val_blocked_lc
,       open_order_val_blocked_gc
,       open_order_val_unblocked
,       open_order_val_unblocked_lc
,       open_order_val_unblocked_gc
,       backorder_value_unblocked
,       backorder_value_unblocked_lc
,       backorder_value_unblocked_gc
,       open_order_block_ind
,       open_order_del_block_header_ind
,       open_order_del_block_item_ind
,       open_order_bill_block_ind
,       is_backorder
,       distr_channel_pl
,       etl_load_time
,       erdat_pmonth
from
        tmp_dws_o2c_customer_order_item_basic_indicator_di_not_exits;