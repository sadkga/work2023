--********************************************************************--
--所属主题: 产品域
--功能描述: 蓄电池O2O
--创建者:王兆翔
--创建日期:2023-04-18
--修改日期  修改人  修改内容
--yyyymmdd  name  comment
--********************************************************************--

-- truncate table ads_battery_o2o_target_order_num_tmp;
-- alter table ads_battery_o2o_target_order_num_tmp drop partition(ds =20230427)
insert overwrite table ads_battery_o2o_target_order_num_tmp partition(ds = ${bdp.system.bizdate}) values
-- 2023
(1,'京东', 2023, 1, 2212, 9100721, 1900,'${bdp.system.bizdate}'),
(2,'京东', 2023, 2, 1476, 589720, 2100, '${bdp.system.bizdate}'),
(3,'京东', 2023, 3, 1716, 733383 ,2400, '${bdp.system.bizdate}'),
(4,'京东', 2023, 4, 1989, null, 2800, '${bdp.system.bizdate}'),
(5,'京东', 2023, 5, 2492, null, 2800, '${bdp.system.bizdate}'),
(6,'京东', 2023, 6, 2850, null, 4000, '${bdp.system.bizdate}'),
(7,'京东', 2023, 7, 1641, null, 3500, '${bdp.system.bizdate}'),
(8,'京东', 2023, 8, 3138, null, 3660, '${bdp.system.bizdate}'),
(9,'京东', 2023, 9, null, null, 4440, '${bdp.system.bizdate}'),
(10,'京东',2023, 10,null, null, 3739, '${bdp.system.bizdate}'),
(11,'京东',2023, 11,null, null, 4900, '${bdp.system.bizdate}'),
(12,'京东',2023, 12,null, null, 3761, '${bdp.system.bizdate}'),
(13,'天猫',2023, 1, 884, 484970, 938, '${bdp.system.bizdate}'),
(14,'天猫',2023, 2, 659, 332440, 1000, '${bdp.system.bizdate}'),
(15,'天猫',2023, 3, 755, 377500, 1262, '${bdp.system.bizdate}'),
(16,'天猫',2023, 4, 688,null, 1200, '${bdp.system.bizdate}'),
(17,'天猫',2023, 5, 869,null, 1800, '${bdp.system.bizdate}'),
(18,'天猫',2023, 6, 966,null, 1800, '${bdp.system.bizdate}'),
(19,'天猫',2023, 7, 1173,null, 2000, '${bdp.system.bizdate}'),
(20,'天猫',2023, 8, 2408,null, 2000, '${bdp.system.bizdate}'),
(21,'天猫',2023, 9, null,null, 2000, '${bdp.system.bizdate}'),
(22,'天猫',2023, 10,null,null, 2200, '${bdp.system.bizdate}'),
(23,'天猫',2023, 11,null,null, 2000, '${bdp.system.bizdate}'),
(24,'天猫',2023, 12,null,null, 2000, '${bdp.system.bizdate}'),
(25,'平安', 2023, 1, null, null, null,'${bdp.system.bizdate}'),
(26,'平安', 2023, 2, null, null, null, '${bdp.system.bizdate}'),
(27,'平安', 2023, 3, null, null ,null, '${bdp.system.bizdate}'),
(28,'平安', 2023, 4, null, null, null, '${bdp.system.bizdate}'),
(29,'平安', 2023, 5, 436, null, null, '${bdp.system.bizdate}'),
(30,'平安', 2023, 6, 687, null, null, '${bdp.system.bizdate}'),
(31,'平安', 2023, 7, 687, null, null, '${bdp.system.bizdate}'),
(32,'平安', 2023, 8, 910, null, null, '${bdp.system.bizdate}'),
(33,'平安', 2023, 9, null, null, null, '${bdp.system.bizdate}'),
(34,'平安',2023, 10,null, null, null, '${bdp.system.bizdate}'),
(35,'平安',2023, 11,null, null, null, '${bdp.system.bizdate}'),
(36,'平安',2023, 12,null, null, null, '${bdp.system.bizdate}'),
(37,'百顺',2023, 1, 461, null, null, '${bdp.system.bizdate}'),
(38,'百顺',2023, 2, 214, null, null, '${bdp.system.bizdate}'),
(39,'百顺',2023, 3, 200, null, null, '${bdp.system.bizdate}'),
(40,'百顺',2023, 4, 183,null, null, '${bdp.system.bizdate}'),
(41,'百顺',2023, 5, 209,null, null, '${bdp.system.bizdate}'),
(42,'百顺',2023, 6, 233,null, null, '${bdp.system.bizdate}'),
(43,'百顺',2023, 7, 338,null, null, '${bdp.system.bizdate}'),
(44,'百顺',2023, 8, 358,null, null, '${bdp.system.bizdate}'),
(45,'百顺',2023, 9, null,null, null, '${bdp.system.bizdate}'),
(46,'百顺',2023, 10,null,null, null, '${bdp.system.bizdate}'),
(47,'百顺',2023, 11,null,null, null, '${bdp.system.bizdate}'),
(48,'百顺',2023, 12,null,null, null, '${bdp.system.bizdate}'),
-- 2022
(1,'京东', 2022, 1, 1712, 619891, null,'${bdp.system.bizdate}'),
(2,'京东', 2022, 2, 1444, 481345, null, '${bdp.system.bizdate}'),
(3,'京东', 2022, 3, 1164, 407930 ,null, '${bdp.system.bizdate}'),
(4,'京东', 2022, 4, 1364, 513248, null, '${bdp.system.bizdate}'),
(5,'京东', 2022, 5, 1780, 757357,   null, '${bdp.system.bizdate}'),
(6,'京东', 2022, 6, 2731, 1085839,   null, '${bdp.system.bizdate}'),
(7,'京东', 2022, 7, 1626, 635176,   null, '${bdp.system.bizdate}'),
(8,'京东', 2022, 8, 1876, 677077,   null, '${bdp.system.bizdate}'),
(9,'京东', 2022, 9, 2208, 859799,   null, '${bdp.system.bizdate}'),
(10,'京东',2022, 10,2355, 908610,   null, '${bdp.system.bizdate}'),
(11,'京东',2022, 11,3135, 1347412,   null, '${bdp.system.bizdate}'),
(12,'京东',2022, 12,3383, 1353529,   null, '${bdp.system.bizdate}'),
(13,'天猫',2022, 1, 434, 261211, null, '${bdp.system.bizdate}'),
(14,'天猫',2022, 2, 325, 189231, null, '${bdp.system.bizdate}'),
(15,'天猫',2022, 3, 289, 118490, null, '${bdp.system.bizdate}'),
(16,'天猫',2022, 4, 234, 95940,   null, '${bdp.system.bizdate}'),
(17,'天猫',2022, 5, 420, 172200,   null, '${bdp.system.bizdate}'),
(18,'天猫',2022, 6, 1057, 433370,   null, '${bdp.system.bizdate}'),
(19,'天猫',2022, 7, 504, 206640,   null, '${bdp.system.bizdate}'),
(20,'天猫',2022, 8, 605, 248050,   null, '${bdp.system.bizdate}'),
(21,'天猫',2022, 9, 712, 291920,   null, '${bdp.system.bizdate}'),
(22,'天猫',2022, 10,721, 295610,   null, '${bdp.system.bizdate}'),
(23,'天猫',2022, 11,1036, 424760,   null, '${bdp.system.bizdate}'),
(24,'天猫',2022, 12,1047, 429270,   null, '${bdp.system.bizdate}')


