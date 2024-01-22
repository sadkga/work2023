--********************************************************************--
--所属主题: 供应链域
--功能描述: 供应链域 - Smart code srn 绑定关系事实表
--创建者: yuejian
--创建日期:2022-08-09
--修改日期  修改人  修改内容

--2022-08-30  Jinyu  脚本替换 dwd_lgs_smart_code_info_df ，补充叶子节点标签
--2022-11-18  yuejian 新增max_level（链路最高层级码）字段
--********************************************************************--

--配置参数
SET hive.tez.container.size = 8192;
--临时表管理
DROP TABLE IF EXISTS temp_dwd_lgs_smart_code_binding_rltnp_df_01;
DROP TABLE IF EXISTS temp_dwd_lgs_smart_code_binding_rltnp_df_02;
DROP TABLE IF EXISTS temp_dwd_lgs_smart_code_binding_rltnp_df_03;
DROP TABLE IF EXISTS temp_dwd_lgs_smart_code_binding_rltnp_df_04;
DROP TABLE IF EXISTS temp_dwd_lgs_smart_code_binding_rltnp_df_05;

-- 顶层码表
DROP TABLE IF EXISTS temp_dwd_las_smart_code_first_code;
CREATE TABLE IF NOT EXISTS temp_dwd_las_smart_code_first_code(
    first_code string comment '顶层码'
);

with t0 as (
select sernr_sub,sernr_main FROM dwd_lgs_smart_code_info_df WHERE ds = '${bdp.system.bizdate}' and sernr_sub != sernr_main 
) 
, mid as (
select 
    t0.sernr_sub sernr_sub_0,
    t0.sernr_main sernr_main_0,
    t1.sernr_sub sernr_sub_1,
    t1.sernr_main sernr_main_1,
    t2.sernr_sub sernr_sub_2,
    t2.sernr_main sernr_main_2,
    t3.sernr_sub sernr_sub_3,
    t3.sernr_main sernr_main_3,
    t4.sernr_sub sernr_sub_4,
    t4.sernr_main sernr_main_4,
    t5.sernr_sub sernr_sub_5,
    t5.sernr_main sernr_main_5,
    t6.sernr_sub sernr_sub_6,
    t6.sernr_main sernr_main_6,
    t7.sernr_sub sernr_sub_7,
    t7.sernr_main sernr_main_7,
    t8.sernr_sub sernr_sub_8,
    t8.sernr_main sernr_main_8
from t0 
left join t0 t1 on t0.sernr_main = t1.sernr_sub and t0.sernr_main is not null and t0.sernr_main != ''
left join t0 t2 on t1.sernr_main = t2.sernr_sub and t1.sernr_main is not null and t1.sernr_main != ''
left join t0 t3 on t2.sernr_main = t3.sernr_sub and t2.sernr_main is not null and t2.sernr_main != ''
left join t0 t4 on t3.sernr_main = t4.sernr_sub and t3.sernr_main is not null and t3.sernr_main != ''
left join t0 t5 on t4.sernr_main = t5.sernr_sub and t4.sernr_main is not null and t4.sernr_main != ''
left join t0 t6 on t5.sernr_main = t6.sernr_sub and t5.sernr_main is not null and t5.sernr_main != ''
left join t0 t7 on t6.sernr_main = t7.sernr_sub and t6.sernr_main is not null and t6.sernr_main != ''
left join t0 t8 on t7.sernr_main = t8.sernr_sub and t7.sernr_main is not null and t7.sernr_main != ''
)
, t2 as (
select
    sernr_sub_0, 
    case when sernr_main_8 is not null  and sernr_main_8 != ''   then sernr_main_8
        when sernr_sub_8 is not null    and sernr_sub_8 != ''  then sernr_sub_8
        when sernr_main_7 is not null   and sernr_main_7 != ''  then sernr_main_7
        when sernr_sub_7 is not null    and sernr_sub_7 != ''  then sernr_sub_7
        when sernr_main_6 is not null   and sernr_main_6 != ''  then sernr_main_6
        when sernr_sub_6 is not null    and sernr_sub_6 != ''  then sernr_sub_6
        when sernr_main_5 is not null   and sernr_main_5 != ''  then sernr_main_5
        when sernr_sub_5 is not null    and sernr_sub_5 != ''  then sernr_sub_5
        when sernr_main_4 is not null   and sernr_main_4 != ''  then sernr_main_4
        when sernr_sub_4 is not null    and sernr_sub_4 != ''  then sernr_sub_4
        when sernr_main_3 is not null   and sernr_main_3 != ''  then sernr_main_3
        when sernr_sub_3 is not null    and sernr_sub_3 != ''  then sernr_sub_3
        when sernr_main_2 is not null   and sernr_main_2 != ''  then sernr_main_2
        when sernr_sub_2 is not null    and sernr_sub_2 != ''  then sernr_sub_2
        when sernr_main_1 is not null   and sernr_main_1 != ''  then sernr_main_1
        when sernr_sub_1 is not null    and sernr_sub_1 != ''  then sernr_sub_1
        when sernr_main_0 is not null   and sernr_main_0 != ''  then sernr_main_0
        when sernr_sub_0 is not null    and sernr_sub_0 != ''  then sernr_sub_0
    end first_code,
    case when sernr_main_8 is not null  and sernr_main_8 != ''   then 'sernr_main_8'
        when sernr_sub_8 is not null    and sernr_sub_8 != ''  then 'sernr_sub_8'
        when sernr_main_7 is not null   and sernr_main_7 != ''  then 'sernr_main_7'
        when sernr_sub_7 is not null    and sernr_sub_7 != ''  then 'sernr_sub_7'
        when sernr_main_6 is not null   and sernr_main_6 != ''  then 'sernr_main_6'
        when sernr_sub_6 is not null    and sernr_sub_6 != ''  then 'sernr_sub_6'
        when sernr_main_5 is not null   and sernr_main_5 != ''  then 'sernr_main_5'
        when sernr_sub_5 is not null    and sernr_sub_5 != ''  then 'sernr_sub_5'
        when sernr_main_4 is not null   and sernr_main_4 != ''  then 'sernr_main_4'
        when sernr_sub_4 is not null    and sernr_sub_4 != ''  then 'sernr_sub_4'
        when sernr_main_3 is not null   and sernr_main_3 != ''  then 'sernr_main_3'
        when sernr_sub_3 is not null    and sernr_sub_3 != ''  then 'sernr_sub_3'
        when sernr_main_2 is not null   and sernr_main_2 != ''  then 'sernr_main_2'
        when sernr_sub_2 is not null    and sernr_sub_2 != ''  then 'sernr_sub_2'
        when sernr_main_1 is not null   and sernr_main_1 != ''  then 'sernr_main_1'
        when sernr_sub_1 is not null    and sernr_sub_1 != ''  then 'sernr_sub_1'
        when sernr_main_0 is not null   and sernr_main_0 != ''  then 'sernr_main_0'
        when sernr_sub_0 is not null    and sernr_sub_0 != ''  then 'sernr_sub_0'
    end first_code_source
from mid
) insert overwrite table temp_dwd_las_smart_code_first_code
-- select * from t2 group by sernr_sub,first_code,first_code_source
select first_code from t2 group by first_code;





--将父子级层级关系表拉横转换为树型层级临时表
CREATE TABLE IF NOT EXISTS temp_dwd_lgs_smart_code_binding_rltnp_df_01 AS -- 一级层级
SELECT
    sernr_sub AS min_smart_code --最小层级编码
    ,sernr_sub AS primary_code --一级编码
    ,NULL AS secondary_code --二级编码
    ,NULL AS level_three_code --三级编码
    ,NULL AS level_four_code --四级编码
    ,NULL AS level_five_code --五级编码
    ,NULL AS level_six_code --六级编码
    ,NULL AS level_seven_code --七级编码
    ,NULL AS level_eight_code --八级编码
    ,'1' AS min_smart_code_level --所在层级
    ,NULL AS smart_code_flow --编码流（一级层级无编码流）
    ,matnr_sub AS material_code --物料编码
FROM dwd_lgs_smart_code_info_df
WHERE ds = '${bdp.system.bizdate}' 
AND sernr_main ='' --上级物料为空即为第一层
union all 
SELECT
    sernr_main AS min_smart_code --最小层级编码
    ,sernr_main AS primary_code --一级编码
    ,NULL AS secondary_code --二级编码
    ,NULL AS level_three_code --三级编码
    ,NULL AS level_four_code --四级编码
    ,NULL AS level_five_code --五级编码
    ,NULL AS level_six_code --六级编码
    ,NULL AS level_seven_code --七级编码
    ,NULL AS level_eight_code --八级编码
    ,'1' AS min_smart_code_level --所在层级
    ,NULL AS smart_code_flow --编码流（一级层级无编码流）
    ,replace(matnr_main,' ','') AS material_code --物料编码
FROM dwd_lgs_smart_code_info_df
WHERE ds = '${bdp.system.bizdate}' 
AND (sernr_main in (select first_code from temp_dwd_las_smart_code_first_code)) --特定的规则，即为顶层码
GROUP BY sernr_main,matnr_main
UNION ALL
    -- 二级层级
SELECT
    p2.sernr_sub AS min_smart_code --最小层级编码
    ,p1.sernr_sub AS sernr_sub --一级编码
    ,p2.sernr_sub AS secondary_code --二级编码
    ,NULL AS level_three_code --三级编码
    ,NULL AS level_four_code --四级编码
    ,NULL AS level_five_code --五级编码
    ,NULL AS level_six_code --六级编码
    ,NULL AS level_seven_code --七级编码
    ,NULL AS level_eight_code --八级编码
    ,'2' AS min_smart_code_level --所在层级
    ,CONCAT(p1.sernr_sub, '->', p2.sernr_sub) AS smart_code_flow --编码流（拼接父级编码->子级编码）
    ,p2.matnr_sub AS material_code --物料编码
FROM
    (
        select first_code sernr_sub from temp_dwd_las_smart_code_first_code
    ) p1
LEFT JOIN
    (
        SELECT
            sernr_sub,
            matnr_sub,
            sernr_main
        FROM dwd_lgs_smart_code_info_df
        WHERE ds = '${bdp.system.bizdate}' 
    ) p2 
ON p1.sernr_sub = p2.sernr_main 
AND p2.sernr_main IS NOT NULL
UNION ALL
    -- 三级层级
SELECT
    p2.sernr_sub AS min_smart_code --最小层级编码
    ,p1.level_1 AS sernr_sub --一级编码
    ,p1.level_2 AS secondary_code --二级编码
    ,p2.sernr_sub AS level_three_code --三级编码
    ,NULL AS level_four_code --四级编码
    ,NULL AS level_five_code --五级编码
    ,NULL AS level_six_code --六级编码
    ,NULL AS level_seven_code --七级编码
    ,NULL AS level_eight_code --八级编码
    ,'3' AS min_smart_code_level --所在层级
    ,CONCAT(p1.level_2, '->', p2.sernr_sub) AS smart_code_flow --编码流（拼接父级编码->子级编码）
    ,p2.matnr_sub AS material_code --物料编码
FROM
    (
        SELECT
            t1.sernr_sub AS level_1,
            t2.sernr_sub AS level_2
        FROM
            (
               select first_code sernr_sub from temp_dwd_las_smart_code_first_code
            ) t1
        LEFT JOIN
            (
                SELECT
                    sernr_sub,
                    sernr_main
                FROM dwd_lgs_smart_code_info_df
                WHERE ds = '${bdp.system.bizdate}' 
            ) t2 
        ON t1.sernr_sub = t2.sernr_main
        WHERE t2.sernr_main IS NOT NULL
    ) p1
LEFT JOIN
    (
        SELECT
            sernr_sub,
            matnr_sub,
            sernr_main
        FROM dwd_lgs_smart_code_info_df
        WHERE ds = '${bdp.system.bizdate}' 
    ) p2 
ON p1.level_2 = p2.sernr_main
WHERE p2.sernr_main IS NOT NULL
UNION ALL
    -- 四级层级
SELECT
    p2.sernr_sub AS min_smart_code --最小层级编码
    ,p1.level_1 AS sernr_sub --一级编码
    ,p1.level_2 AS secondary_code --二级编码
    ,p1.level_3 AS level_three_code --三级编码
    ,p2.sernr_sub AS level_four_code --四级编码
    ,NULL AS level_five_code --五级编码
    ,NULL AS level_six_code --六级编码
    ,NULL AS level_seven_code --七级编码
    ,NULL AS level_eight_code --八级编码
    ,'4' AS min_smart_code_level --所在层级
    ,CONCAT(p1.level_3, '->', p2.sernr_sub) AS smart_code_flow --编码流（拼接父级编码->子级编码）
    ,p2.matnr_sub AS material_code --物料编码
FROM
    (
        SELECT
            t3.level_1 AS level_1,
            t3.level_2 AS level_2,
            t4.sernr_sub AS level_3
        FROM
            (
                SELECT
                    t1.sernr_sub AS level_1,
                    t2.sernr_sub AS level_2
                FROM
                    (
                      select first_code sernr_sub from temp_dwd_las_smart_code_first_code
                    ) t1
                LEFT JOIN
                    (
                        SELECT
                            sernr_sub,
                            sernr_main
                        FROM dwd_lgs_smart_code_info_df
                        WHERE ds = '${bdp.system.bizdate}'
                    ) t2 
                ON t1.sernr_sub = t2.sernr_main
                WHERE t2.sernr_main IS NOT NULL
            ) t3
        LEFT JOIN
            (
                SELECT
                    sernr_sub,
                    sernr_main
                FROM dwd_lgs_smart_code_info_df
                WHERE ds = '${bdp.system.bizdate}' 
            ) t4 
        ON t3.level_2 = t4.sernr_main
        WHERE t4.sernr_main IS NOT NULL
    ) p1
LEFT JOIN
    (
        SELECT
            sernr_sub,
            matnr_sub,
            sernr_main
        FROM
            dwd_lgs_smart_code_info_df
        WHERE ds = '${bdp.system.bizdate}' 
    ) p2 
ON p1.level_3 = p2.sernr_main
WHERE p2.sernr_main IS NOT NULL
UNION ALL
    -- 五级层级
SELECT
    p2.sernr_sub AS min_smart_code --最小层级编码
    ,p1.level_1 AS sernr_sub --一级编码
    ,p1.level_2 AS secondary_code --二级编码
    ,p1.level_3 AS level_three_code --三级编码
    ,p1.level_4 AS level_four_code --四级编码
    ,p2.sernr_sub AS level_five_code --五级编码
    ,NULL AS level_six_code --六级编码
    ,NULL AS level_seven_code --七级编码
    ,NULL AS level_eight_code --八级编码
    ,'5' AS min_smart_code_level --所在层级
    ,CONCAT(p1.level_4, '->', p2.sernr_sub) AS smart_code_flow --编码流（拼接父级编码->子级编码）
    ,p2.matnr_sub AS material_code --物料编码
FROM
    (
        SELECT
            t5.level_1 AS level_1,
            t5.level_2 AS level_2,
            t5.level_3 AS level_3,
            t6.sernr_sub AS level_4
        FROM
            (
                SELECT
                    t3.level_1 AS level_1,
                    t3.level_2 AS level_2,
                    t4.sernr_sub AS level_3
                FROM
                    (
                        SELECT
                            t1.sernr_sub AS level_1,
                            t2.sernr_sub AS level_2
                        FROM
                            (
                             select first_code sernr_sub from temp_dwd_las_smart_code_first_code
                            ) t1
                        LEFT JOIN
                            (
                                SELECT
                                    sernr_sub,
                                    sernr_main
                                FROM dwd_lgs_smart_code_info_df
                                WHERE ds = '${bdp.system.bizdate}' 
                            ) t2 
                        ON t1.sernr_sub = t2.sernr_main
                        WHERE t2.sernr_main IS NOT NULL
                    ) t3
                LEFT JOIN
                    (
                        SELECT
                            sernr_sub,
                            sernr_main
                        FROM dwd_lgs_smart_code_info_df
                        WHERE ds = '${bdp.system.bizdate}' 
                    ) t4 
                ON t3.level_2 = t4.sernr_main
                WHERE t4.sernr_main IS NOT NULL
            ) t5
        LEFT JOIN
            (
                SELECT
                    sernr_sub,
                    sernr_main
                FROM dwd_lgs_smart_code_info_df
                WHERE ds = '${bdp.system.bizdate}' 
            ) t6 
        ON t5.level_3 = t6.sernr_main
        WHERE t6.sernr_main IS NOT NULL
    ) p1
LEFT JOIN
    (
        SELECT
            sernr_sub,
            matnr_sub,
            sernr_main
        FROM dwd_lgs_smart_code_info_df
        WHERE ds = '${bdp.system.bizdate}' 
    ) p2 
ON p1.level_4 = p2.sernr_main
WHERE p2.sernr_main IS NOT NULL
UNION ALL
    -- 六级层级
SELECT
    p2.sernr_sub AS min_smart_code --最小层级编码
    ,p1.level_1 AS sernr_sub --一级编码
    ,p1.level_2 AS secondary_code --二级编码
    ,p1.level_3 AS level_three_code --三级编码
    ,p1.level_4 AS level_four_code --四级编码
    ,p1.level_5 AS level_five_code --五级编码
    ,p2.sernr_sub AS level_six_code --六级编码
    ,NULL AS level_seven_code --七级编码
    ,NULL AS level_eight_code --八级编码
    ,'6' AS min_smart_code_level --所在层级
    ,CONCAT(p1.level_5, '->', p2.sernr_sub) AS smart_code_flow --编码流（拼接父级编码->子级编码）
    ,p2.matnr_sub AS material_code --物料编码
FROM
    (
        SELECT
            t7.level_1 AS level_1,
            t7.level_2 AS level_2,
            t7.level_3 AS level_3,
            t7.level_4 AS level_4,
            t8.sernr_sub AS level_5
        FROM
            (
                SELECT
                    t5.level_1 AS level_1,
                    t5.level_2 AS level_2,
                    t5.level_3 AS level_3,
                    t6.sernr_sub AS level_4
                FROM
                    (
                        SELECT
                            t3.level_1 AS level_1,
                            t3.level_2 AS level_2,
                            t4.sernr_sub AS level_3
                        FROM
                            (
                                SELECT
                                    t1.sernr_sub AS level_1,
                                    t2.sernr_sub AS level_2
                                FROM
                                    (
                                       select first_code sernr_sub from temp_dwd_las_smart_code_first_code
                                    ) t1
                                LEFT JOIN
                                    (
                                        SELECT
                                            sernr_sub,
                                            sernr_main
                                        FROM dwd_lgs_smart_code_info_df
                                        WHERE ds = '${bdp.system.bizdate}' 
                                    ) t2 
                                ON t1.sernr_sub = t2.sernr_main
                                WHERE t2.sernr_main IS NOT NULL
                            ) t3
                        LEFT JOIN
                            (
                                SELECT
                                    sernr_sub,
                                    sernr_main
                                FROM dwd_lgs_smart_code_info_df
                                WHERE ds = '${bdp.system.bizdate}' 
                            ) t4 
                        ON t3.level_2 = t4.sernr_main
                        WHERE t4.sernr_main IS NOT NULL
                    ) t5
                LEFT JOIN
                    (
                        SELECT
                            sernr_sub,
                            sernr_main
                        FROM dwd_lgs_smart_code_info_df
                        WHERE ds = '${bdp.system.bizdate}' 
                    ) t6 
                ON t5.level_3 = t6.sernr_main
                WHERE t6.sernr_main IS NOT NULL
            ) t7
        LEFT JOIN
            (
                SELECT
                    sernr_sub,
                    sernr_main
                FROM dwd_lgs_smart_code_info_df
                WHERE ds = '${bdp.system.bizdate}' 
            ) t8 
        ON t7.level_4 = t8.sernr_main
        WHERE t8.sernr_main IS NOT NULL
    ) p1
LEFT JOIN
    (
        SELECT
            sernr_sub,
            matnr_sub,
            sernr_main
        FROM dwd_lgs_smart_code_info_df
        WHERE ds = '${bdp.system.bizdate}' 
    ) p2 
ON p1.level_5 = p2.sernr_main
WHERE p2.sernr_main IS NOT NULL
UNION ALL
    -- 七级层级
SELECT
    p2.sernr_sub AS min_smart_code --最小层级编码
    ,p1.level_1 AS sernr_sub --一级编码
    ,p1.level_2 AS secondary_code --二级编码
    ,p1.level_3 AS level_three_code --三级编码
    ,p1.level_4 AS level_four_code --四级编码
    ,p1.level_5 AS level_five_code --五级编码
    ,p1.level_6 AS level_six_code --六级编码
    ,p2.sernr_sub AS level_seven_code --七级编码
    ,NULL AS level_eight_code --八级编码
    ,'7' AS min_smart_code_level --所在层级
    ,CONCAT(p1.level_6, '->', p2.sernr_sub) AS smart_code_flow --编码流（拼接父级编码->子级编码）
    ,p2.matnr_sub AS material_code --物料编码
FROM
    (
        SELECT
            t9.level_1 AS level_1,
            t9.level_2 AS level_2,
            t9.level_3 AS level_3,
            t9.level_4 AS level_4,
            t9.level_5 AS level_5,
            t10.sernr_sub AS level_6
        FROM
            (
                SELECT
                    t7.level_1 AS level_1,
                    t7.level_2 AS level_2,
                    t7.level_3 AS level_3,
                    t7.level_4 AS level_4,
                    t8.sernr_sub AS level_5
                FROM
                    (
                        SELECT
                            t5.level_1 AS level_1,
                            t5.level_2 AS level_2,
                            t5.level_3 AS level_3,
                            t6.sernr_sub AS level_4
                        FROM
                            (
                                SELECT
                                    t3.level_1 AS level_1,
                                    t3.level_2 AS level_2,
                                    t4.sernr_sub AS level_3
                                FROM
                                    (
                                        SELECT
                                            t1.sernr_sub AS level_1,
                                            t2.sernr_sub AS level_2
                                        FROM
                                            (
                                               select first_code sernr_sub from temp_dwd_las_smart_code_first_code
                                            ) t1
                                        LEFT JOIN
                                            (
                                                SELECT
                                                    sernr_sub,
                                                    sernr_main
                                                FROM dwd_lgs_smart_code_info_df
                                                WHERE ds = '${bdp.system.bizdate}' 
                                            ) t2 
                                        ON t1.sernr_sub = t2.sernr_main
                                        WHERE t2.sernr_main IS NOT NULL
                                    ) t3
                                LEFT JOIN
                                    (
                                        SELECT
                                            sernr_sub,
                                            sernr_main
                                        FROM dwd_lgs_smart_code_info_df
                                        WHERE ds = '${bdp.system.bizdate}' 
                                    ) t4 
                                ON t3.level_2 = t4.sernr_main
                                WHERE t4.sernr_main IS NOT NULL
                            ) t5
                        LEFT JOIN
                            (
                                SELECT
                                    sernr_sub,
                                    sernr_main
                                FROM dwd_lgs_smart_code_info_df
                                WHERE ds = '${bdp.system.bizdate}' 
                            ) t6 
                        ON t5.level_3 = t6.sernr_main
                        WHERE t6.sernr_main IS NOT NULL
                    ) t7
                LEFT JOIN
                    (
                        SELECT
                            sernr_sub,
                            sernr_main
                        FROM dwd_lgs_smart_code_info_df
                        WHERE ds = '${bdp.system.bizdate}' 
                    ) t8 
                ON t7.level_4 = t8.sernr_main
                WHERE t8.sernr_main IS NOT NULL
            ) t9
        LEFT JOIN
            (
                SELECT
                    sernr_sub,
                    sernr_main
                FROM dwd_lgs_smart_code_info_df
                WHERE ds = '${bdp.system.bizdate}' 
            ) t10 
        ON t9.level_5 = t10.sernr_main
        WHERE t10.sernr_main IS NOT NULL
    ) p1
LEFT JOIN
    (
        SELECT
            sernr_sub,
            matnr_sub,
            sernr_main
        FROM dwd_lgs_smart_code_info_df
        WHERE ds = '${bdp.system.bizdate}' 
    ) p2 
ON p1.level_6 = p2.sernr_main
WHERE p2.sernr_main IS NOT NULL
UNION ALL
    -- 八级层级
SELECT
    p2.sernr_sub AS min_smart_code --最小层级编码
    ,p1.level_1 AS sernr_sub --一级编码
    ,p1.level_2 AS secondary_code --二级编码
    ,p1.level_3 AS level_three_code --三级编码
    ,p1.level_4 AS level_four_code --四级编码
    ,p1.level_5 AS level_five_code --五级编码
    ,p1.level_6 AS level_six_code --六级编码
    ,p1.level_7 AS level_seven_code --七级编码
    ,p2.sernr_sub AS level_eight_code --八级编码
    ,'8' AS min_smart_code_level --所在层级
    ,CONCAT(p1.level_7, '->', p2.sernr_sub) AS smart_code_flow --编码流（拼接父级编码->子级编码）
    ,p2.matnr_sub AS material_code --物料编码
FROM
    (
        SELECT
            t11.level_1 AS level_1,
            t11.level_2 AS level_2,
            t11.level_3 AS level_3,
            t11.level_4 AS level_4,
            t11.level_5 AS level_5,
            t11.level_6 AS level_6,
            t12.sernr_sub AS level_7
        FROM
            (
                SELECT
                    t9.level_1 AS level_1,
                    t9.level_2 AS level_2,
                    t9.level_3 AS level_3,
                    t9.level_4 AS level_4,
                    t9.level_5 AS level_5,
                    t10.sernr_sub AS level_6
                FROM
                    (
                        SELECT
                            t7.level_1 AS level_1,
                            t7.level_2 AS level_2,
                            t7.level_3 AS level_3,
                            t7.level_4 AS level_4,
                            t8.sernr_sub AS level_5
                        FROM
                            (
                                SELECT
                                    t5.level_1 AS level_1,
                                    t5.level_2 AS level_2,
                                    t5.level_3 AS level_3,
                                    t6.sernr_sub AS level_4
                                FROM
                                    (
                                        SELECT
                                            t3.level_1 AS level_1,
                                            t3.level_2 AS level_2,
                                            t4.sernr_sub AS level_3
                                        FROM
                                            (
                                                SELECT
                                                    t1.sernr_sub AS level_1,
                                                    t2.sernr_sub AS level_2
                                                FROM
                                                    (
                                                        select first_code sernr_sub from temp_dwd_las_smart_code_first_code
                                                    ) t1
                                                LEFT JOIN
                                                    (
                                                        SELECT
                                                            sernr_sub,
                                                            sernr_main
                                                        FROM dwd_lgs_smart_code_info_df
                                                        WHERE ds = '${bdp.system.bizdate}' 
                                                    ) t2 
                                                ON t1.sernr_sub = t2.sernr_main
                                                WHERE t2.sernr_main IS NOT NULL
                                            ) t3
                                        LEFT JOIN
                                            (
                                                SELECT
                                                    sernr_sub,
                                                    sernr_main
                                                FROM dwd_lgs_smart_code_info_df
                                                WHERE ds = '${bdp.system.bizdate}' 
                                            ) t4 
                                        ON t3.level_2 = t4.sernr_main
                                        WHERE t4.sernr_main IS NOT NULL
                                    ) t5
                                LEFT JOIN
                                    (
                                        SELECT
                                            sernr_sub,
                                            sernr_main
                                        FROM dwd_lgs_smart_code_info_df
                                        WHERE ds = '${bdp.system.bizdate}' 
                                    ) t6 
                                ON t5.level_3 = t6.sernr_main
                                WHERE t6.sernr_main IS NOT NULL
                            ) t7
                        LEFT JOIN
                            (
                                SELECT
                                    sernr_sub,
                                    sernr_main
                                FROM dwd_lgs_smart_code_info_df
                                WHERE ds = '${bdp.system.bizdate}' 
                            ) t8 
                        ON t7.level_4 = t8.sernr_main
                        WHERE t8.sernr_main IS NOT NULL
                    ) t9
                LEFT JOIN
                    (
                        SELECT
                            sernr_sub,
                            sernr_main
                        FROM dwd_lgs_smart_code_info_df
                        WHERE ds = '${bdp.system.bizdate}' 
                    ) t10 
                ON t9.level_5 = t10.sernr_main
                WHERE t10.sernr_main IS NOT NULL
            ) t11
        LEFT JOIN
            (
                SELECT
                    sernr_sub,
                    sernr_main
                FROM dwd_lgs_smart_code_info_df
                WHERE ds = '${bdp.system.bizdate}' 
            ) t12 
        ON t11.level_6 = t12.sernr_main
        WHERE t12.sernr_main IS NOT NULL
    ) p1
LEFT JOIN
    (
        SELECT
            sernr_sub,
            matnr_sub,
            sernr_main
        FROM dwd_lgs_smart_code_info_df
        WHERE ds = '${bdp.system.bizdate}' 
    ) p2 
ON p1.level_7 = p2.sernr_main
WHERE p2.sernr_main IS NOT NULL;
    
-- 根据打横的树状层级树判断是否为根结点
CREATE TABLE IF NOT EXISTS temp_dwd_lgs_smart_code_binding_rltnp_df_02 AS
SELECT
    t1.min_smart_code --最小层级编码
    ,t1.primary_code --一级编码
    ,t1.secondary_code --二级编码
    ,t1.level_three_code --三级编码
    ,t1.level_four_code --四级编码
    ,t1.level_five_code --五级编码
    ,t1.level_six_code --六级编码
    ,t1.level_seven_code --七级编码
    ,t1.level_eight_code --八级编码
    ,CASE WHEN t2.sernr_main IS NULL THEN 1 ELSE 0
    END AS roots --是否为根结点，通过树状层级树的最小层级编码与srn表的父级编码字段关联判断，如果关联的上，则不为根节点，反之则为根结点
FROM
    (
        SELECT
            min_smart_code --最小层级编码
            ,primary_code --一级编码
            ,secondary_code --二级编码
            ,level_three_code --三级编码
            ,level_four_code --四级编码
            ,level_five_code --五级编码
            ,level_six_code --六级编码
            ,level_seven_code --七级编码
            ,level_eight_code --八级编码
        FROM
            temp_dwd_lgs_smart_code_binding_rltnp_df_01
    ) t1
LEFT JOIN
    (
        SELECT
            sernr_main --父级物料编码
        FROM
            dwd_lgs_smart_code_info_df
        WHERE
            ds = '${bdp.system.bizdate}' 
    ) t2 ON t1.min_smart_code = t2.sernr_main
GROUP BY
    t1.min_smart_code,
    t1.primary_code,
    t1.secondary_code,
    t1.level_three_code,
    t1.level_four_code,
    t1.level_five_code,
    t1.level_six_code,
    t1.level_seven_code,
    t1.level_eight_code,
    CASE WHEN t2.sernr_main IS NULL THEN 1 ELSE 0 END
;

-- 根据根结点临时表计算每个节点向下所有的根结点数量
CREATE TABLE IF NOT EXISTS temp_dwd_lgs_smart_code_binding_rltnp_df_03 AS --根据一级编码分组，汇总一级编码向下所有包含物料数量
SELECT
    primary_code AS min_smart_code --最小层级编码
    ,primary_code AS primary_code --一级编码
    ,sum(roots) AS rlt_code_qty --向下所有包含物料数量
FROM
    temp_dwd_lgs_smart_code_binding_rltnp_df_02
GROUP BY
    primary_code
UNION ALL
    --根据二级编码分组，汇总二级编码向下所有包含物料数量
SELECT
    secondary_code AS min_smart_code --最小层级编码
    ,primary_code AS primary_code --一级编码
    ,sum(roots) AS rlt_code_qty --向下所有包含物料数量
FROM
    temp_dwd_lgs_smart_code_binding_rltnp_df_02
WHERE
    secondary_code IS NOT NULL
GROUP BY
    primary_code,
    secondary_code
UNION ALL
    --根据三级编码分组，汇总三级编码向下所有包含物料数量
SELECT
    level_three_code AS min_smart_code --最小层级编码
    ,primary_code AS primary_code --一级编码
    ,sum(roots) AS rlt_code_qty --向下所有包含物料数量
FROM
    temp_dwd_lgs_smart_code_binding_rltnp_df_02
WHERE
    level_three_code IS NOT NULL
GROUP BY
    primary_code,
    secondary_code,
    level_three_code
UNION ALL
    --根据四级编码分组，汇总四级编码向下所有包含物料数量
SELECT
    level_four_code AS min_smart_code,
    primary_code AS primary_code,
    sum(roots) AS rlt_code_qty
FROM
    temp_dwd_lgs_smart_code_binding_rltnp_df_02
WHERE
    level_four_code IS NOT NULL
GROUP BY
    primary_code,
    secondary_code,
    level_three_code,
    level_four_code
UNION ALL
    --根据五级编码分组，汇总五级编码向下所有包含物料数量
SELECT
    level_five_code AS min_smart_code,
    primary_code AS primary_code,
    sum(roots) AS rlt_code_qty
FROM
    temp_dwd_lgs_smart_code_binding_rltnp_df_02
WHERE
    level_five_code IS NOT NULL
GROUP BY
    primary_code,
    secondary_code,
    level_three_code,
    level_four_code,
    level_five_code
UNION ALL
    --根据六级编码分组，汇总六级编码向下所有包含物料数量
SELECT
    level_six_code AS min_smart_code,
    primary_code AS primary_code,
    sum(roots) AS rlt_code_qty
FROM
    temp_dwd_lgs_smart_code_binding_rltnp_df_02
WHERE
    level_six_code IS NOT NULL
GROUP BY
    primary_code,
    secondary_code,
    level_three_code,
    level_four_code,
    level_five_code,
    level_six_code
UNION ALL
    --根据七级编码分组，汇总七级编码向下所有包含物料数量
SELECT
    level_seven_code AS min_smart_code,
    primary_code AS primary_code,
    sum(roots) AS rlt_code_qty
FROM
    temp_dwd_lgs_smart_code_binding_rltnp_df_02
WHERE
    level_seven_code IS NOT NULL
GROUP BY
    primary_code,
    secondary_code,
    level_three_code,
    level_four_code,
    level_five_code,
    level_six_code,
    level_seven_code
UNION ALL
    --根据八级编码分组，汇总八级编码向下所有包含物料数量
SELECT
    level_eight_code AS min_smart_code,
    primary_code AS primary_code,
    sum(roots) AS rlt_code_qty
FROM
    temp_dwd_lgs_smart_code_binding_rltnp_df_02
WHERE
    level_eight_code IS NOT NULL
GROUP BY
    primary_code,
    secondary_code,
    level_three_code,
    level_four_code,
    level_five_code,
    level_six_code,
    level_seven_code,
    level_eight_code
;

-- 补充物料编码和物料名称，以及计算是否为叶子节点，并打上标签
CREATE TABLE IF NOT EXISTS temp_dwd_lgs_smart_code_binding_rltnp_df_04 AS
SELECT
    t1.min_smart_code AS min_smart_code --最小层级编码
    ,t1.primary_code AS primary_code --一级编码
    ,t2.secondary_code AS secondary_code --二级编码
    ,t2.level_three_code AS level_three_code --三级编码
    ,t2.level_four_code AS level_four_code --四级编码
    ,t2.level_five_code AS level_five_code --五级编码
    ,t2.level_six_code AS level_six_code --六级编码
    ,t2.level_seven_code AS level_seven_code --七级编码
    ,t2.level_eight_code AS level_eight_code --八级编码
    ,t2.min_smart_code_level AS min_smart_code_level --所在层级
    ,t2.smart_code_flow AS smart_code_flow --编码流
    ,t2.material_code AS material_code --物料编码
    ,t3.zh_name AS material_name --物料名称
    ,t1.rlt_code_qty AS rlt_code_qty --向下所有包含物料数量
    ,case when t4.roots = 1 then 'Y' when t4.roots = 0 then 'N' else '异常' end as is_leaf_node --是否为叶子节点
FROM
    temp_dwd_lgs_smart_code_binding_rltnp_df_03 t1
LEFT JOIN
    temp_dwd_lgs_smart_code_binding_rltnp_df_01 t2 
ON t1.min_smart_code = t2.min_smart_code 
AND t1.primary_code = t2.primary_code
LEFT JOIN
    (
        SELECT
            product_number,
            zh_name
        FROM
            ods_spiderb_sys_product_df
        WHERE
            ds = '${bdp.system.bizdate}'
    ) t3 
ON SUBSTR(t2.material_code, 1, 10) = t3.product_number --使用物料编码的前10位与产品编码关联，获取物料名称
left join temp_dwd_lgs_smart_code_binding_rltnp_df_02 t4
on t1.min_smart_code = t4.min_smart_code
;


-- 由于vbeln只有叶子结点有，对应父节点打上对应数据，对应多个dn的，取最大值
CREATE TABLE IF NOT EXISTS temp_dwd_lgs_smart_code_binding_rltnp_df_05 AS
select 
	t.min_smart_code
	,t.vbeln
from 
	(
		select 
		    t1.primary_code as min_smart_code
		    ,max(t2.vbeln) as vbeln
		from temp_dwd_lgs_smart_code_binding_rltnp_df_04 t1
		left join dwd_lgs_smart_code_info_df t2
		on t1.min_smart_code = t2.sernr_sub
		and t2.ds = '${bdp.system.bizdate}'  
		where t1.is_leaf_node = 'Y'
		group by 
			t1.primary_code
		union all 
		select 
		    t1.secondary_code as min_smart_code
		    ,max(t2.vbeln) as vbeln
		from temp_dwd_lgs_smart_code_binding_rltnp_df_04 t1
		left join dwd_lgs_smart_code_info_df t2
		on t1.min_smart_code = t2.sernr_sub
		and t2.ds = '${bdp.system.bizdate}'  
		where t1.is_leaf_node = 'Y'
		group by 
			t1.secondary_code
		union all 
		select 
		    t1.level_three_code as min_smart_code
		    ,max(t2.vbeln) as vbeln
		from temp_dwd_lgs_smart_code_binding_rltnp_df_04 t1
		left join dwd_lgs_smart_code_info_df t2
		on t1.min_smart_code = t2.sernr_sub
		and t2.ds = '${bdp.system.bizdate}'  
		where t1.is_leaf_node = 'Y'
		group by 
			t1.level_three_code
		union all 
		select 
		    t1.level_four_code as min_smart_code
		    ,max(t2.vbeln) as vbeln
		from temp_dwd_lgs_smart_code_binding_rltnp_df_04 t1
		left join dwd_lgs_smart_code_info_df t2
		on t1.min_smart_code = t2.sernr_sub
		and t2.ds = '${bdp.system.bizdate}'  
		where t1.is_leaf_node = 'Y'
		group by 
			t1.level_four_code
		union all 
		select 
		    t1.level_five_code as min_smart_code
		    ,max(t2.vbeln) as vbeln
		from temp_dwd_lgs_smart_code_binding_rltnp_df_04 t1
		left join dwd_lgs_smart_code_info_df t2
		on t1.min_smart_code = t2.sernr_sub
		and t2.ds = '${bdp.system.bizdate}'  
		where t1.is_leaf_node = 'Y'
		group by 
			t1.level_five_code
		union all 
		select 
		    t1.level_six_code as min_smart_code
		    ,max(t2.vbeln) as vbeln
		from temp_dwd_lgs_smart_code_binding_rltnp_df_04 t1
		left join dwd_lgs_smart_code_info_df t2
		on t1.min_smart_code = t2.sernr_sub
		and t2.ds = '${bdp.system.bizdate}'  
		where t1.is_leaf_node = 'Y'
		group by 
			t1.level_six_code
		union all 
		select 
		    t1.level_seven_code as min_smart_code
		    ,max(t2.vbeln) as vbeln
		from temp_dwd_lgs_smart_code_binding_rltnp_df_04 t1
		left join dwd_lgs_smart_code_info_df t2
		on t1.min_smart_code = t2.sernr_sub
		and t2.ds = '${bdp.system.bizdate}'  
		where t1.is_leaf_node = 'Y'
		group by 
			t1.level_seven_code
		union all 
		select 
		    t1.level_eight_code as min_smart_code
		    ,max(t2.vbeln) as vbeln
		from temp_dwd_lgs_smart_code_binding_rltnp_df_04 t1
		left join dwd_lgs_smart_code_info_df t2
		on t1.min_smart_code = t2.sernr_sub
		and t2.ds = '${bdp.system.bizdate}'  
		where t1.is_leaf_node = 'Y'
		group by 
			t1.level_eight_code
	)t
where t.min_smart_code is not null
;


INSERT OVERWRITE TABLE dwd_lgs_smart_code_binding_rltnp_df PARTITION (ds = '${bdp.system.bizdate}')
SELECT
    t1.min_smart_code -- 最小层级编码
    ,t1.primary_code -- 一级编码
    ,t1.secondary_code -- 二级编码
    ,t1.level_three_code -- 三级编码
    ,t1.level_four_code -- 四级编码
    ,t1.level_five_code -- 五级编码
    ,t1.level_six_code -- 六级编码
    ,t1.level_seven_code -- 七级编码
    ,t1.level_eight_code -- 八级编码
    ,t1.min_smart_code_level -- 所在层级
    ,t1.is_leaf_node -- 是否为叶子节点
    ,t2.max_level  -- 链路最高层级码
    ,t3.vbeln  -- 交货单号
    ,t1.smart_code_flow -- 编码流
    ,t1.material_code -- 物料编码
    ,t1.material_name -- 物料名称
    ,t1.rlt_code_qty -- 向下所有包含物料数量
    ,current_timestamp() AS etl_load_time --数据加载时间
FROM 
    temp_dwd_lgs_smart_code_binding_rltnp_df_04 t1
LEFT JOIN
    (
        SELECT
            primary_code
            ,max(min_smart_code_level) as max_level
        FROM
            temp_dwd_lgs_smart_code_binding_rltnp_df_04
        WHERE
            is_leaf_node = 'Y'
        GROUP BY
            primary_code   
    )t2
ON t1.primary_code = t2.primary_code
left join temp_dwd_lgs_smart_code_binding_rltnp_df_05 t3
on t1.min_smart_code = t3.min_smart_code
;