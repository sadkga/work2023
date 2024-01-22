--********************************************************************--
--所属主题: 车主域
--功能描述: 小程序订单数据
--创建者: 王兆翔
--创建日期:2023-12-15
--********************************************************************--
insert overwrite table dwd_car_driver_mini_program_order_df partition(pday=${bdp.system.bizdate})
select 
    distinct id                           
    ,order_no                    
    ,customer_id                 
    ,mobile as phone9                     
    ,open_id                     
    ,total_amount                
    ,total_score                 
    ,preferential_price          
    ,activity_preferential_price 
    ,coupon_preferential_price   
    ,freight                     
    ,final_amount                
    ,refund_amount               
    ,customer_coupon_id          
    ,activity_id                 
    ,pay_at                      
    ,cancelled_by                
    ,cancelled_at                
    ,is_del                      
    ,quantity                    
    ,after_sale_num              
    ,is_all_refund               
    ,order_logistics_id          
    ,province                    
    ,city                        
    ,area                        
    ,province_code               
    ,city_code                   
    ,area_code                   
    ,address   as a9                  
    ,expire_at                   
    ,sync_status                 
    ,from_type                   
    ,from_id                     
    ,created_at                  
    ,updated_at                  
    ,deleted_at                  
    ,date_format(current_timestamp(),'yyyy-MM-dd HH:mm:ss') etl_load_time               
from ods_car_driver_mini_program_order_df
where ds = ${bdp.system.bizdate}


-- select * from dwd_car_driver_mini_program_order_df;