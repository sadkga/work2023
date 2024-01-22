drop table if exists dwd_car_driver_mini_program_store_df;
create table if not exists dwd_car_driver_mini_program_store_df(
    id             string comment ''             
    ,name          string comment '门店名称'                 
    ,store_no      string comment '门店编号'                     
    ,show_name9    string comment '显示名称'                     
    ,address9      string comment '门店详细地址'                     
    ,latitude      string comment '纬度'                     
    ,longitude     string comment '经度'                     
    ,province      string comment '省份code'                     
    ,city          string comment '城市code'                 
    ,area          string comment '城区code'                 
    ,city_index    string comment '城市首字母'                     
    ,status        string comment '状态 1：显示，2：不显示'                 
    ,created_at    string comment '创建时间'                     
    ,updated_at    string comment '修改时间'                     
    ,deleted_at    string comment '删除时间'                     
    ,etl_load_time string comment '数据加载时间'
)
partitioned by(pday string comment "时间分区")
stored as parquet 
location 'boschfs://boschfs/warehouse/dwd/dwd_car_driver_mini_program_store_df'