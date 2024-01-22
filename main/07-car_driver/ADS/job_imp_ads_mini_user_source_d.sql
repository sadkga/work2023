--********************************************************************--
--所属主题: 车主域
--功能描述: 数据小程序_来源渠道_数据改造
--创建者:王兆翔
--创建日期:2023-04-10
--修改日期  修改人  修改内容
--yyyymmdd  name  comment
--********************************************************************--
-- 设置动态分区 压缩等参数
-- 分区
SET hive.exec.dynamic.partition = TRUE;
SET hive.exec.dynamic.partition.mode = nonstrict;
-- 分区参数设置
SET hive.exec.max.dynamic.partitions.pernode = 10000;
SET hive.exec.max.dynamic.partitions = 100000;
SET hive.exec.max.created.files = 150000;
-- hive压缩
SET hive.exec.compress.intermediate = TRUE;
SET hive.exec.compress.output = TRUE;
-- 每个容器设置4G大小
SET hive.tez.container.size = 4096;

INSERT OVERWRITE TABLE ads_car_driver_mini_program_user_source_d
SELECT
    distinct cd.id as customer_id, -- 客户主键ID
    cd.mobile, -- 手机号
    cd.open_id, -- 小程序 open_id
    cd.union_id, -- 小程序 union_id
    cd.nick_name, -- 昵称
    cd.real_name, -- 真实姓名
    cd.avatar, -- 头像
    cd.gender, -- 性别 (0:未知 1:男 2:女 3:保密)
    cd.country, -- 国家
    cd.province, -- 省份
    cd.city, -- 城市
    DATE_FORMAT(FROM_UNIXTIME(cd.birthday),'YYYY-MM-dd') AS birthday, -- 生日
    ca.auth_type,  -- 授权类型 ：1-小程序 2-公众号
    ca.status, -- 公众号关注状态 ：0-默认值 1-已关注 2-已取消关注
    DATE_FORMAT(FROM_UNIXTIME(cd.last_auth_at),'YYYY-MM-dd hh:mm:ss') last_auth_at, -- 最后一次授权时间
    DATE_FORMAT(FROM_UNIXTIME(cd.last_login_at),'YYYY-MM-dd hh:mm:ss') last_login_at, -- 最后一次登录时间
    cd.register_from, -- 注册来源 1-小程序 2-其它平台导入
    cd.is_logout, -- 是否已退出登录 1-否 2-是
    cad.receiver_name, -- 默认收货人姓名
    cad.receiver_mobile, --  默认收货人联系方式
    cad.province, --  默认省名称
    cad.city, --  默认市名称
    cad.area, --  默认区名称
    cad.address, --  默认详细地址
    cad.label, -- 标签 1-公司 2-家 3-其他
    cad.type, -- 地址类型 1-普通地址 2-微信授权地址
    cv.vehicle_brand_id, -- 品牌 id
    cv.brand_name, -- 品牌名称
    cv.brand_logo, -- 品牌logo
    cv.model_class, -- 车系
    cv.vehicle_model_id, -- 车型id
    cv.model_name, -- 车型名称
    cv.sell_name, -- 销售名称
    cv.vehicle_name, -- 具体车型名称
    cv.vehicle_id, -- 具体车型id
    cv.product_year, -- 年份
    cv.output_volume, -- 排量
    cv.manufacturer, -- 厂家
    cv.engine_code, -- 发动机型号
    cv.license_plate, -- 车牌号
    cv.vin, -- 车辆vin码
    cv.remark, -- 用户备注信息
    cv.is_default, -- 是否默认车辆 ：1-默认车辆 2-非默认车辆
    ce.invitation_customer_id,  -- 邀请人ID
    ce.obj_type, -- 系统原始来源类型：1-抽奖活动邀请 2-内部活动注册 3-深圳会员活动注册
    ce.obj_id,   -- 系统原始来源对象id（1-抽奖活动id 2-营销活动id 3-营销活动id）
    chl.source_type, -- 渠道类型
    vrl.scan_code_time, -- 扫码时间
    chl.source_id   -- mapping表的主键ID

FROM
    dwd_car_driver_mini_program_customer_cl cd
    -- 基础数据
    left join dwd_car_driver_mini_program_customer_auth_df_cld ca on cd.id = ca.customer_id
    left join (SELECT * FROM dwd_car_driver_mini_program_customer_address_df_cld WHERE is_default = 2) cad on cd.id = cad.customer_id
    left join (SELECT * FROM dwd_car_driver_mini_program_customer_vehicle_df_cld WHERE is_default =1) cv on cd.id = cv.customer_id
    left join dwd_car_driver_mini_program_customer_extend_cl ce on cd.id = ce.customer_id
    -- 渠道关联
    left join (SELECT customer_id,source,MAX(scan_code_time) scan_code_time FROM dwd_car_driver_mini_program_app_visit_record_log_cl GROUP BY customer_id,source) vrl on cd.id = vrl.customer_id
    left join (SELECT source,source_type,source_id FROM ads_car_driver_mini_program_channel_mapping_d) chl on vrl.source = chl.source

