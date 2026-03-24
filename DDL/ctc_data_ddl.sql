-- ==========================================
-- ctc_data Database DDL Export
-- Source: 10.1.206.136:3306/ctc_data
-- Generated: 2026-03-24
-- Total Tables: 40
-- ==========================================


-- ==========================================
-- Table: broadbandnetwork
-- ==========================================
CREATE TABLE `broadbandnetwork` (
  `network_id` varchar(64) NOT NULL COMMENT '网络ID',
  `fiber_length` decimal(10,2) DEFAULT NULL COMMENT '光纤长度单位为公里',
  `operational_status` varchar(50) DEFAULT NULL COMMENT '运行状态',
  `dependent_device` varchar(255) DEFAULT NULL COMMENT '依赖设备',
  `Bandwidth` float DEFAULT NULL COMMENT '网络带宽',
  `access_device_count` int DEFAULT '0',
  PRIMARY KEY (`network_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='宽带网络';


-- ==========================================
-- Table: broadbandnetwork_service_link
-- ==========================================
CREATE TABLE `broadbandnetwork_service_link` (
  `id` varchar(64) NOT NULL,
  `network_id` varchar(64) DEFAULT NULL,
  `service_id` varchar(64) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_bn_s_network` (`network_id`),
  KEY `fk_bn_s_service` (`service_id`),
  CONSTRAINT `fk_bn_s_network` FOREIGN KEY (`network_id`) REFERENCES `broadbandnetwork` (`network_id`),
  CONSTRAINT `fk_bn_s_service` FOREIGN KEY (`service_id`) REFERENCES `service` (`service_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='宽带网络-服务 被优化关系';


-- ==========================================
-- Table: broadbandnetwork_terminal_link
-- ==========================================
CREATE TABLE `broadbandnetwork_terminal_link` (
  `id` varchar(64) NOT NULL,
  `network_id` varchar(64) DEFAULT NULL,
  `terminal_id` varchar(64) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_bn_t_network` (`network_id`),
  KEY `fk_bn_t_terminal` (`terminal_id`),
  CONSTRAINT `fk_bn_t_network` FOREIGN KEY (`network_id`) REFERENCES `broadbandnetwork` (`network_id`),
  CONSTRAINT `fk_bn_t_terminal` FOREIGN KEY (`terminal_id`) REFERENCES `terminal` (`terminal_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='宽带网络-终端 关系';


-- ==========================================
-- Table: business_ticket
-- ==========================================
CREATE TABLE `business_ticket` (
  `ticket_id` varchar(200) NOT NULL COMMENT '工单唯一标识ID',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '工单创建时间',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '工单最后更新时间',
  `ticket_content` text NOT NULL COMMENT '工单详细内容描述',
  `ticket_title` varchar(200) DEFAULT NULL COMMENT '工单标题/摘要',
  `ticket_status` varchar(20) NOT NULL DEFAULT '待处理' COMMENT '工单当前状态, 待处理/处理中/已解决/已关闭等',
  PRIMARY KEY (`ticket_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='业务工单表';


-- ==========================================
-- Table: customer
-- ==========================================
CREATE TABLE `customer` (
  `customer_id` varchar(64) NOT NULL COMMENT '客户ID',
  `name` varchar(100) DEFAULT NULL COMMENT '姓名',
  `age` int DEFAULT NULL COMMENT '年龄',
  `income_level` varchar(100) DEFAULT NULL COMMENT '收入水平',
  `region` varchar(100) DEFAULT NULL COMMENT '地域分布',
  `education_level` varchar(100) DEFAULT NULL COMMENT '教育水平',
  `preferred_product_type` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '偏好套餐类型',
  PRIMARY KEY (`customer_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='客户';


-- ==========================================
-- Table: customer_event_link
-- ==========================================
CREATE TABLE `customer_event_link` (
  `id` varchar(64) NOT NULL,
  `customer_id` varchar(64) DEFAULT NULL,
  `event_id` varchar(64) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_c_e_customer` (`customer_id`),
  KEY `fk_c_e_event` (`event_id`),
  CONSTRAINT `fk_c_e_customer` FOREIGN KEY (`customer_id`) REFERENCES `customer` (`customer_id`),
  CONSTRAINT `fk_c_e_event` FOREIGN KEY (`event_id`) REFERENCES `event` (`event_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='客户-事件 被影响关系';


-- ==========================================
-- Table: customer_packageproduct_link
-- ==========================================
CREATE TABLE `customer_packageproduct_link` (
  `id` varchar(64) NOT NULL,
  `customer_id` varchar(64) DEFAULT NULL,
  `product_id` varchar(64) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_c_p_customer` (`customer_id`),
  KEY `fk_c_p_pkg` (`product_id`),
  CONSTRAINT `fk_c_p_customer` FOREIGN KEY (`customer_id`) REFERENCES `customer` (`customer_id`),
  CONSTRAINT `fk_c_p_pkg` FOREIGN KEY (`product_id`) REFERENCES `packageproduct` (`product_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='客户-套餐产品 订购关系';


-- ==========================================
-- Table: customer_service_link
-- ==========================================
CREATE TABLE `customer_service_link` (
  `id` varchar(64) NOT NULL,
  `customer_id` varchar(64) DEFAULT NULL,
  `service_id` varchar(64) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_c_s_customer` (`customer_id`),
  KEY `fk_c_s_service` (`service_id`),
  CONSTRAINT `fk_c_s_customer` FOREIGN KEY (`customer_id`) REFERENCES `customer` (`customer_id`),
  CONSTRAINT `fk_c_s_service` FOREIGN KEY (`service_id`) REFERENCES `service` (`service_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='客户-服务 使用关系';


-- ==========================================
-- Table: customer_valueaddedproduct_link
-- ==========================================
CREATE TABLE `customer_valueaddedproduct_link` (
  `id` varchar(64) NOT NULL,
  `customer_id` varchar(64) DEFAULT NULL,
  `product_id` varchar(64) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_c_v_customer` (`customer_id`),
  KEY `fk_c_v_vap` (`product_id`),
  CONSTRAINT `fk_c_v_customer` FOREIGN KEY (`customer_id`) REFERENCES `customer` (`customer_id`),
  CONSTRAINT `fk_c_v_vap` FOREIGN KEY (`product_id`) REFERENCES `valueaddedproduct` (`product_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='客户-增值产品 订购关系';


-- ==========================================
-- Table: customerbehavior
-- ==========================================
CREATE TABLE `customerbehavior` (
  `behavior_id` varchar(64) NOT NULL COMMENT '客户行为ID',
  `expiration_time` datetime DEFAULT NULL COMMENT '套餐到期时间',
  `avg_bill` float DEFAULT NULL COMMENT '月均账单金额',
  `avg_traffic` float DEFAULT NULL COMMENT '月均流量使用',
  `behavior_type` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '行为类型',
  `customer_id` varchar(64) DEFAULT NULL COMMENT '客户ID',
  `stat_month` varchar(7) DEFAULT NULL COMMENT '统计月份',
  `product_instance_id` varchar(64) DEFAULT NULL COMMENT '产品实例标识',
  `grid_code` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '网格支局编码',
  `grid_name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '网格支局名称',
  `customer_segment` varchar(100) DEFAULT NULL COMMENT '客户战略分群',
  `customer_star` varchar(50) DEFAULT NULL COMMENT '客户星级',
  `terminal_smart_home_count` int DEFAULT NULL COMMENT '终端类智家数',
  `cloud_product_count` int DEFAULT NULL COMMENT '云产品数',
  `rights_product_count` int DEFAULT NULL COMMENT '权益产品数',
  `active_tianyis` int DEFAULT NULL COMMENT '活跃天翼数',
  `active_broadband` int DEFAULT NULL COMMENT '活跃宽带数',
  `active_itv` int DEFAULT NULL COMMENT '活跃ITV数',
  `active_terminal_smart_home` int DEFAULT NULL COMMENT '活跃终端类智家数',
  `active_cloud_product` int DEFAULT NULL COMMENT '活跃云产品数',
  `wing_message_count` int DEFAULT NULL COMMENT '翼留言数',
  `hangup_sms_count` int DEFAULT NULL COMMENT '挂机短信数',
  `youpin_pack_count` int DEFAULT NULL COMMENT '优品包数',
  `tianyi_card_count` int DEFAULT NULL COMMENT '天翼卡数',
  `broadband_count` int DEFAULT NULL COMMENT '宽带数',
  `itv_count` int DEFAULT NULL COMMENT 'ITV数',
  `fixed_phone_count` int DEFAULT NULL COMMENT '固话数',
  `video_ring_count` int DEFAULT NULL COMMENT '视频彩铃数',
  `caller_card_count` int DEFAULT NULL COMMENT '来电名片数',
  `work_top1_cell` varchar(128) DEFAULT NULL COMMENT '工作地top1驻留小区',
  `work_top2_cell` varchar(128) DEFAULT NULL COMMENT '工作地top2驻留小区',
  `home_top1_cell` varchar(128) DEFAULT NULL COMMENT '居住地top1驻留小区',
  `home_top2_cell` varchar(128) DEFAULT NULL COMMENT '居住地top2驻留小区',
  `open_date` date DEFAULT NULL COMMENT '开通日期',
  `network_age_days` int DEFAULT NULL COMMENT '入网时长',
  `card_type` varchar(50) DEFAULT NULL COMMENT '卡类型',
  `is_5g_pack` tinyint(1) DEFAULT NULL COMMENT '是否5G套餐',
  `is_unlimited` tinyint(1) DEFAULT NULL COMMENT '是否不限量套餐用户',
  `is_over_pack` tinyint(1) DEFAULT NULL COMMENT '是否超套用户',
  `is_low_traffic` tinyint(1) DEFAULT NULL COMMENT '是否低流量',
  `is_fusion_product` tinyint(1) DEFAULT NULL COMMENT '是否加入融合产品',
  `is_low_voice` tinyint(1) DEFAULT NULL COMMENT '是否低语音',
  `last3_avg_traffic` float DEFAULT NULL COMMENT '近3个月均使用流量',
  `last3_avg_call_duration` float DEFAULT NULL COMMENT '近3个月均主叫通话时长',
  `current_call_duration` float DEFAULT NULL COMMENT '本月通话总时长',
  `is_repeat_call` tinyint(1) DEFAULT NULL COMMENT '是否重复来电用户',
  `is_abnormal_hangup` tinyint(1) DEFAULT NULL COMMENT '是否非正常挂机用户',
  `is_long_call` tinyint(1) DEFAULT NULL COMMENT '是否超长通话用户',
  `is_fee_mismatch` tinyint(1) DEFAULT NULL COMMENT '是否套餐资费不匹配',
  `is_high_sat_low_perceive` tinyint(1) DEFAULT NULL COMMENT '是否高饱和高活跃低感知',
  `is_agreement_expire` tinyint(1) DEFAULT NULL COMMENT '是否协议到期用户',
  `is_old_pack` tinyint(1) DEFAULT NULL COMMENT '是否老旧套餐用户',
  `is_low_sat_low_active` tinyint(1) DEFAULT NULL COMMENT '是否低饱和低活跃用户',
  `is_active_user` tinyint(1) DEFAULT NULL COMMENT '是否活跃用户',
  `is_zero_user` tinyint(1) DEFAULT NULL COMMENT '是否零次户',
  `current_month_traffic` float DEFAULT NULL COMMENT '本月使用流量',
  `sms_count` int DEFAULT NULL COMMENT '短信条数',
  `voice_duration` float DEFAULT NULL COMMENT '语音时长',
  `called_duration` float DEFAULT NULL COMMENT '被叫时长',
  `call_out_times` int DEFAULT NULL COMMENT '主叫次数',
  `call_in_times` int DEFAULT NULL COMMENT '被叫次数',
  `mobile_online_duration` float DEFAULT NULL COMMENT '手机上网时长',
  `mr_rsrp_below105` bigint DEFAULT NULL COMMENT '本月RSRP小于负105的MR总数目',
  `mr_rsrp_above105` bigint DEFAULT NULL COMMENT '本月RSRP大于等于负105的MR总数目',
  `rsrp_good_ratio` float DEFAULT NULL COMMENT '本月RSRP优良率',
  `min_rsrp_good_ratio` float DEFAULT NULL COMMENT '本月最小RSRP优良率',
  `mr_rsrp_below115` bigint DEFAULT NULL COMMENT '本月RSRP小于负115的MR总数目',
  `mr_rsrp_above115` bigint DEFAULT NULL COMMENT '本月RSRP大于等于负115的MR总数目',
  `rsrp_good_ratio_115` float DEFAULT NULL COMMENT '本月RSRP优良率负115',
  `min_rsrp_good_ratio_115` float DEFAULT NULL COMMENT '本月最小RSRP优良率负115',
  `days_rsrp105_below80` int DEFAULT NULL COMMENT '本月RSRP优良率负105低于百分之80的天数',
  `days_rsrp115_below80` int DEFAULT NULL COMMENT '本月RSRP优良率负115低于百分之80的天数',
  `min_web_resp_rate` float DEFAULT NULL COMMENT '本月最小网页响应成功率',
  `min_web_disp_rate` float DEFAULT NULL COMMENT '本月最小网页显示成功率',
  `web_abnormal_times` int DEFAULT NULL COMMENT '本月网页异常话单总次数',
  `web_resp_rate` float DEFAULT NULL COMMENT '本月网页响应成功率',
  `web_disp_rate` float DEFAULT NULL COMMENT '本月网页显示成功率',
  `web_main_resp_rate` float DEFAULT NULL COMMENT '本月访问主流网站的网页响应成功率',
  `web_main_disp_rate` float DEFAULT NULL COMMENT '本月访问主流网站的网页显示成功率',
  `video_stutter_times` int DEFAULT NULL COMMENT '本月视频播放卡顿总次数',
  `min_video_succ_rate` float DEFAULT NULL COMMENT '本月最小视频播放成功率',
  `video_wait_duration` float DEFAULT NULL COMMENT '本月视频播放等待总时长',
  `max_video_stutter_freq` float DEFAULT NULL COMMENT '本月最大视频播放卡顿频次',
  `max_video_stutter_ratio` float DEFAULT NULL COMMENT '本月最大视频卡顿时长占比',
  `video_abnormal_times` int DEFAULT NULL COMMENT '本月视频异常话单总次数',
  `video_recover_delay_over` int DEFAULT NULL COMMENT '本月视频停顿恢复时延超过门限的次数',
  `video_stutter_per_mb_over` int DEFAULT NULL COMMENT '本月每兆流量视频播放卡顿次数超过门限次数',
  `video_download_good_ratio` float DEFAULT NULL COMMENT '本月视频下载优良率',
  `video_play_succ_rate` float DEFAULT NULL COMMENT '本月视频播放成功率',
  `min_msg_send_rate` float DEFAULT NULL COMMENT '本月最小消息发送成功率',
  `min_msg_recv_rate` float DEFAULT NULL COMMENT '本月最小消息接收成功率',
  `msg_abnormal_times` int DEFAULT NULL COMMENT '本月消息异常话单总次数',
  `msg_recv_rate` float DEFAULT NULL COMMENT '本月消息接收成功率',
  `msg_send_rate` float DEFAULT NULL COMMENT '本月消息发送成功率',
  `game_delay` float DEFAULT NULL COMMENT '本月游戏交互总时延',
  `game_abnormal_times` int DEFAULT NULL COMMENT '本月游戏异常话单总次数',
  `game_delay_good_ratio` float DEFAULT NULL COMMENT '本月游戏交互时延优良率',
  `complain_1m` int DEFAULT NULL COMMENT '近1个月申告次数',
  `complain_2m` int DEFAULT NULL COMMENT '近2个月申告次数',
  `complain_3m` int DEFAULT NULL COMMENT '近3个月申告次数',
  `complain_4m` int DEFAULT NULL COMMENT '近4个月申告次数',
  `complain_5m` int DEFAULT NULL COMMENT '近5个月申告次数',
  `complain_6m` int DEFAULT NULL COMMENT '近6个月申告次数',
  `complaint_3m` int DEFAULT NULL COMMENT '近3个月投诉次数',
  `consult_times` int DEFAULT NULL COMMENT '咨询次数',
  `main_pack_fee` decimal(10,2) DEFAULT NULL COMMENT '天翼主卡套餐资费',
  `current_out_income` decimal(10,2) DEFAULT NULL COMMENT '本月出账收入',
  `min_consumption` decimal(10,2) DEFAULT NULL COMMENT '月最低消费金额',
  `is_high_overflow` tinyint(1) DEFAULT NULL COMMENT '是否高套外溢出',
  `pay_times` int DEFAULT NULL COMMENT '本月缴费总次数',
  `is_frequent_stop` tinyint(1) DEFAULT NULL COMMENT '是否频繁停机',
  `last3_avg_out_income` decimal(10,2) DEFAULT NULL COMMENT '三个月月均出账收入',
  `terminal_price` decimal(10,2) DEFAULT NULL COMMENT '终端价格',
  `pack_fee_before_tax` decimal(10,2) DEFAULT NULL COMMENT '税前套餐费用',
  `pack_fee_real` decimal(10,2) DEFAULT NULL COMMENT '实收套餐费用',
  `traffic_pack_fee` decimal(10,2) DEFAULT NULL COMMENT '流量包费用',
  `traffic_overflow_fee` decimal(10,2) DEFAULT NULL COMMENT '当月流量溢出费用',
  `voice_overflow_fee` decimal(10,2) DEFAULT NULL COMMENT '当月语音溢出费用',
  `arrear_12m` int DEFAULT NULL COMMENT '12个月内欠费次数',
  `arrear_6m` int DEFAULT NULL COMMENT '6个月内欠费次数',
  `arrear_3m` int DEFAULT NULL COMMENT '3个月内欠费次数',
  `sub_card_count` int DEFAULT NULL COMMENT '副卡数量',
  `other_rights_count` int DEFAULT NULL COMMENT '其他权益数',
  `add_pack_level` varchar(50) DEFAULT NULL COMMENT '加包价值档',
  `traffic_saturation` float DEFAULT NULL COMMENT '流量饱和度',
  `voice_saturation` float DEFAULT NULL COMMENT '语音饱和度',
  `complaint_dissatisfy_6m` int DEFAULT NULL COMMENT '6个月内投诉不满意次数',
  `consult_dissatisfy_6m` int DEFAULT NULL COMMENT '6个月内咨询不满意次数',
  `last3_dissatisfy_ticket` int DEFAULT NULL COMMENT '近3个月不满意工单数',
  `last3_consult_dissatisfy_ticket` int DEFAULT NULL COMMENT '近3个月咨询不满意工单数',
  `current_complaint_dissatisfy` int DEFAULT NULL COMMENT '本月投诉不满意次数',
  `current_consult_dissatisfy` int DEFAULT NULL COMMENT '本月咨询不满意次数',
  `fault_report_times` int DEFAULT NULL COMMENT '故障申告次数',
  `last3_repeat_fault` int DEFAULT NULL COMMENT '近3个月重复报障次数',
  `last3_repeat_complaint` int DEFAULT NULL COMMENT '近3个月重复投诉次数',
  `current_repeat_call_24h` int DEFAULT NULL COMMENT '本月重复来电次数',
  `current_repeat_complaint_7d` int DEFAULT NULL COMMENT '本月重复投诉工单次数',
  `unsatisfy_call_times` int DEFAULT NULL COMMENT '不满意来电次数',
  `fault_period_call` int DEFAULT NULL COMMENT '故障期间来电次数',
  `fault_period_timeout_call` int DEFAULT NULL COMMENT '超时故障期间来电次数',
  `complaint_period_call` int DEFAULT NULL COMMENT '投诉期间来电次数',
  `complaint_period_timeout_call` int DEFAULT NULL COMMENT '超时投诉期间来电次数',
  `current_consult_times` int DEFAULT NULL COMMENT '本月咨询次数',
  `new_tianyi_card` int DEFAULT NULL COMMENT '新装天翼卡数',
  `new_broadband` int DEFAULT NULL COMMENT '新装宽带数',
  `new_itv` int DEFAULT NULL COMMENT '新装ITV数',
  `new_fixed_phone` int DEFAULT NULL COMMENT '新装固话数',
  `province_id` varchar(64) DEFAULT NULL COMMENT '省份标识',
  `bill_cycle` varchar(7) DEFAULT NULL COMMENT '月账期',
  `local_net_id` varchar(64) DEFAULT NULL COMMENT '本地网标识',
  `total_arrear_money` decimal(10,2) DEFAULT NULL COMMENT '总欠费金额',
  `current_call_out_duration` float DEFAULT NULL COMMENT '本月主叫通话总时长',
  `last_complain_reason` varchar(255) DEFAULT NULL COMMENT '用户最近一次申告原因',
  `sys_proc_time` datetime DEFAULT NULL COMMENT '系统处理时间',
  `pay_mode_code` varchar(64) DEFAULT NULL COMMENT '付费模式编码',
  `customer_code` varchar(64) DEFAULT NULL COMMENT '客户标识',
  `pack_general_traffic` float DEFAULT NULL COMMENT '套内包含通用流量',
  `pack_direct_traffic` float DEFAULT NULL COMMENT '套内包含定向流量',
  `pack_voice` float DEFAULT NULL COMMENT '套内国内语音',
  `current_complaint_times` int DEFAULT NULL COMMENT '本月投诉次数',
  `current_fault_report_times` int DEFAULT NULL COMMENT '本月故障申告次数',
  `current_repeat_fault_times` int DEFAULT NULL COMMENT '本月重复报障次数',
  `current_repeat_complaint_times` int DEFAULT NULL COMMENT '本月重复投诉次数',
  `current_unsatisfy_call` int DEFAULT NULL COMMENT '本月不满意来话次数',
  `current_fault_ticket_ing` int DEFAULT NULL COMMENT '本月在途故障工单次数',
  `current_fault_ticket_timeout` int DEFAULT NULL COMMENT '本月在途超时故障工单次数',
  `current_complaint_ticket_ing` int DEFAULT NULL COMMENT '本月在途投诉次数',
  `current_complaint_ticket_timeout` int DEFAULT NULL COMMENT '本月在途超时投诉次数',
  `current_complain_total` int DEFAULT NULL COMMENT '本月客户申告次数',
  `current_avg_fault_deal_minutes` int DEFAULT NULL COMMENT '本月平均故障处理时长',
  `iphone` varchar(11) DEFAULT NULL COMMENT '手机号',
  `satisfaction_score_before` float DEFAULT NULL COMMENT '满意度评分上次',
  `cost_perception_score` float DEFAULT NULL COMMENT '资费感知评分',
  `network_experience_score` float DEFAULT NULL COMMENT '网络体验评分',
  `satisfaction_score` float DEFAULT NULL COMMENT '满意度评分',
  `service_experience_score` float DEFAULT NULL COMMENT '服务体验评分',
  PRIMARY KEY (`behavior_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='客户行为';


-- ==========================================
-- Table: employee
-- ==========================================
CREATE TABLE `employee` (
  `employee_id` varchar(64) NOT NULL COMMENT '员工ID',
  `service_type_responsible` varchar(100) DEFAULT NULL COMMENT '负责服务类型',
  `name` varchar(100) DEFAULT NULL COMMENT '姓名',
  `position` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '职位',
  `department` varchar(100) DEFAULT NULL COMMENT '所属部门',
  PRIMARY KEY (`employee_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='员工';


-- ==========================================
-- Table: employee_perception_link
-- ==========================================
CREATE TABLE `employee_perception_link` (
  `id` varchar(64) NOT NULL,
  `employee_id` varchar(64) DEFAULT NULL,
  `perception_id` varchar(64) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_e_p_employee` (`employee_id`),
  KEY `fk_e_p_perception` (`perception_id`),
  CONSTRAINT `fk_e_p_employee` FOREIGN KEY (`employee_id`) REFERENCES `employee` (`employee_id`),
  CONSTRAINT `fk_e_p_perception` FOREIGN KEY (`perception_id`) REFERENCES `perception` (`perception_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='员工-感知 分析关系';


-- ==========================================
-- Table: employee_perceptionevaluation
-- ==========================================
CREATE TABLE `employee_perceptionevaluation` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `employee_id` varchar(16) NOT NULL COMMENT '员工ID',
  `perceptionevaluation` varchar(16) NOT NULL COMMENT '感知分析ID',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_ep` (`employee_id`,`perceptionevaluation`)
) ENGINE=InnoDB AUTO_INCREMENT=11 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='员工与感知评估多对多关系表';


-- ==========================================
-- Table: employee_remediationstrategy_link
-- ==========================================
CREATE TABLE `employee_remediationstrategy_link` (
  `id` varchar(64) NOT NULL,
  `employee_id` varchar(64) DEFAULT NULL,
  `strategy_id` varchar(64) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_e_r_employee` (`employee_id`),
  KEY `fk_e_r_strategy` (`strategy_id`),
  CONSTRAINT `fk_e_r_employee` FOREIGN KEY (`employee_id`) REFERENCES `employee` (`employee_id`),
  CONSTRAINT `fk_e_r_strategy` FOREIGN KEY (`strategy_id`) REFERENCES `remediationstrategy` (`strategy_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='员工-修复策略 执行关系';


-- ==========================================
-- Table: employee_terminal_link
-- ==========================================
CREATE TABLE `employee_terminal_link` (
  `id` varchar(64) NOT NULL,
  `employee_id` varchar(64) DEFAULT NULL,
  `terminal_id` varchar(64) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_e_t_employee` (`employee_id`),
  KEY `fk_e_t_terminal` (`terminal_id`),
  CONSTRAINT `fk_e_t_employee` FOREIGN KEY (`employee_id`) REFERENCES `employee` (`employee_id`),
  CONSTRAINT `fk_e_t_terminal` FOREIGN KEY (`terminal_id`) REFERENCES `terminal` (`terminal_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='员工-终端 被指派关系';


-- ==========================================
-- Table: employee_workorder_link
-- ==========================================
CREATE TABLE `employee_workorder_link` (
  `id` varchar(64) NOT NULL,
  `employee_id` varchar(64) DEFAULT NULL,
  `work_order_id` varchar(64) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_e_w_employee` (`employee_id`),
  KEY `fk_e_w_work_order` (`work_order_id`),
  CONSTRAINT `fk_e_w_employee` FOREIGN KEY (`employee_id`) REFERENCES `employee` (`employee_id`),
  CONSTRAINT `fk_e_w_work_order` FOREIGN KEY (`work_order_id`) REFERENCES `workorder` (`work_order_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='员工-工单 执行关系';


-- ==========================================
-- Table: event
-- ==========================================
CREATE TABLE `event` (
  `event_id` varchar(64) NOT NULL COMMENT '事件ID',
  `employee_id` varchar(64) DEFAULT NULL COMMENT '员工ID',
  `status` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '事件状态',
  `description` varchar(1000) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '事件描述',
  `occur_time` datetime DEFAULT NULL COMMENT '发生时间',
  `event_type` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '事件类型ID',
  PRIMARY KEY (`event_id`),
  KEY `fk_event_employee` (`employee_id`),
  KEY `fk_event_service` (`event_type`),
  CONSTRAINT `fk_event_employee` FOREIGN KEY (`employee_id`) REFERENCES `employee` (`employee_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='事件';


-- ==========================================
-- Table: event_perception_link
-- ==========================================
CREATE TABLE `event_perception_link` (
  `id` varchar(64) NOT NULL,
  `event_id` varchar(64) DEFAULT NULL,
  `perception_id` varchar(64) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_ev_p_event` (`event_id`),
  KEY `fk_ev_p_perception` (`perception_id`),
  CONSTRAINT `fk_ev_p_event` FOREIGN KEY (`event_id`) REFERENCES `event` (`event_id`),
  CONSTRAINT `fk_ev_p_perception` FOREIGN KEY (`perception_id`) REFERENCES `perception` (`perception_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='事件-感知 被感知关系';


-- ==========================================
-- Table: event_perceptionevaluation_link
-- ==========================================
CREATE TABLE `event_perceptionevaluation_link` (
  `id` varchar(64) NOT NULL,
  `event_id` varchar(64) DEFAULT NULL,
  `evaluation_id` varchar(64) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_ev_pe_event` (`event_id`),
  KEY `fk_ev_pe_evaluation` (`evaluation_id`),
  CONSTRAINT `fk_ev_pe_evaluation` FOREIGN KEY (`evaluation_id`) REFERENCES `perceptionevaluation` (`evaluation_id`),
  CONSTRAINT `fk_ev_pe_event` FOREIGN KEY (`event_id`) REFERENCES `event` (`event_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='事件-感知评估 关联关系';


-- ==========================================
-- Table: event_workorder_link
-- ==========================================
CREATE TABLE `event_workorder_link` (
  `id` varchar(64) NOT NULL,
  `event_id` varchar(64) DEFAULT NULL,
  `work_order_id` varchar(64) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_ev_wo_event` (`event_id`),
  KEY `fk_ev_wo_work_order` (`work_order_id`),
  CONSTRAINT `fk_ev_wo_event` FOREIGN KEY (`event_id`) REFERENCES `event` (`event_id`),
  CONSTRAINT `fk_ev_wo_work_order` FOREIGN KEY (`work_order_id`) REFERENCES `workorder` (`work_order_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='事件-工单 关联关系';


-- ==========================================
-- Table: packageproduct
-- ==========================================
CREATE TABLE `packageproduct` (
  `product_id` varchar(64) NOT NULL COMMENT '产品ID',
  `package_content` varchar(255) DEFAULT NULL COMMENT '套餐内容',
  `package_name` varchar(255) DEFAULT NULL COMMENT '套餐产品名称',
  `price` decimal(10,2) DEFAULT NULL COMMENT '价格',
  `billing_cycle` varchar(100) DEFAULT NULL COMMENT '计费周期',
  `data_plan` float DEFAULT NULL COMMENT '套餐内的通用流量（单位g）',
  `broadband_bandwidth` double DEFAULT NULL,
  PRIMARY KEY (`product_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='套餐产品';


-- ==========================================
-- Table: packageproduct_broadbandnetwork_link
-- ==========================================
CREATE TABLE `packageproduct_broadbandnetwork_link` (
  `id` varchar(64) NOT NULL,
  `product_id` varchar(64) DEFAULT NULL,
  `network_id` varchar(64) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_pkg_bn_product` (`product_id`),
  KEY `fk_pkg_bn_network` (`network_id`),
  CONSTRAINT `fk_pkg_bn_network` FOREIGN KEY (`network_id`) REFERENCES `broadbandnetwork` (`network_id`),
  CONSTRAINT `fk_pkg_bn_product` FOREIGN KEY (`product_id`) REFERENCES `packageproduct` (`product_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='套餐产品-宽带网络 依赖关系';


-- ==========================================
-- Table: packageproduct_wirelessnetwork_link
-- ==========================================
CREATE TABLE `packageproduct_wirelessnetwork_link` (
  `id` varchar(64) NOT NULL,
  `product_id` varchar(64) DEFAULT NULL,
  `network_id` varchar(64) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_pkg_wn_product` (`product_id`),
  KEY `fk_pkg_wn_network` (`network_id`),
  CONSTRAINT `fk_pkg_wn_network` FOREIGN KEY (`network_id`) REFERENCES `wirelessnetwork` (`network_id`),
  CONSTRAINT `fk_pkg_wn_product` FOREIGN KEY (`product_id`) REFERENCES `packageproduct` (`product_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='套餐产品-无线网络 依赖关系';


-- ==========================================
-- Table: perception
-- ==========================================
CREATE TABLE `perception` (
  `perception_id` varchar(64) NOT NULL COMMENT '感知ID',
  `algorithm` varchar(100) DEFAULT NULL COMMENT '感知分析算法',
  `dimension` varchar(1000) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '感知分析算法原理描述',
  `perception_time` datetime DEFAULT NULL COMMENT '感知时间',
  `weidu` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`perception_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='感知';


-- ==========================================
-- Table: perception_perceptionevaluation_link
-- ==========================================
CREATE TABLE `perception_perceptionevaluation_link` (
  `id` varchar(64) NOT NULL,
  `perception_id` varchar(64) DEFAULT NULL,
  `evaluation_id` varchar(64) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_p_pe_perception` (`perception_id`),
  KEY `fk_p_pe_evaluation` (`evaluation_id`),
  CONSTRAINT `fk_p_pe_evaluation` FOREIGN KEY (`evaluation_id`) REFERENCES `perceptionevaluation` (`evaluation_id`),
  CONSTRAINT `fk_p_pe_perception` FOREIGN KEY (`perception_id`) REFERENCES `perception` (`perception_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='感知-感知评估 调用关系';


-- ==========================================
-- Table: perception_remediationstrategy_link
-- ==========================================
CREATE TABLE `perception_remediationstrategy_link` (
  `id` varchar(64) NOT NULL,
  `perception_id` varchar(64) DEFAULT NULL,
  `strategy_id` varchar(64) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_p_rs_perception` (`perception_id`),
  KEY `fk_p_rs_strategy` (`strategy_id`),
  CONSTRAINT `fk_p_rs_perception` FOREIGN KEY (`perception_id`) REFERENCES `perception` (`perception_id`),
  CONSTRAINT `fk_p_rs_strategy` FOREIGN KEY (`strategy_id`) REFERENCES `remediationstrategy` (`strategy_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='感知-修复策略 决策关系';


-- ==========================================
-- Table: perceptionevaluation
-- ==========================================
CREATE TABLE `perceptionevaluation` (
  `evaluation_id` varchar(64) NOT NULL COMMENT '感知评估ID',
  `work_order_id` varchar(64) DEFAULT NULL COMMENT '工单ID',
  `score` decimal(5,2) DEFAULT NULL COMMENT '评估得分',
  `description` varchar(255) DEFAULT NULL COMMENT '评估描述',
  PRIMARY KEY (`evaluation_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='感知评估';


-- ==========================================
-- Table: remediationstrategy
-- ==========================================
CREATE TABLE `remediationstrategy` (
  `strategy_id` varchar(64) NOT NULL COMMENT '修复策略ID',
  `strategy_name` varchar(255) DEFAULT NULL COMMENT '修复策略名称',
  `strategy_description` varchar(255) DEFAULT NULL COMMENT '修复策略说明',
  `network_id` varchar(64) DEFAULT NULL COMMENT '网络ID',
  `product_id` varchar(64) DEFAULT NULL COMMENT '产品ID',
  PRIMARY KEY (`strategy_id`),
  KEY `fk_rs_network_bn` (`network_id`),
  KEY `fk_rs_product_vap` (`product_id`),
  CONSTRAINT `fk_rs_network_bn` FOREIGN KEY (`network_id`) REFERENCES `broadbandnetwork` (`network_id`),
  CONSTRAINT `fk_rs_product_vap` FOREIGN KEY (`product_id`) REFERENCES `valueaddedproduct` (`product_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='修复策略';


-- ==========================================
-- Table: service
-- ==========================================
CREATE TABLE `service` (
  `service_id` varchar(64) NOT NULL COMMENT '服务ID',
  `service_type` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '服务类型',
  PRIMARY KEY (`service_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='服务';


-- ==========================================
-- Table: service_employee_link
-- ==========================================
CREATE TABLE `service_employee_link` (
  `id` varchar(64) NOT NULL,
  `service_id` varchar(64) DEFAULT NULL,
  `employee_id` varchar(64) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_s_e_service` (`service_id`),
  KEY `fk_s_e_employee` (`employee_id`),
  CONSTRAINT `fk_s_e_employee` FOREIGN KEY (`employee_id`) REFERENCES `employee` (`employee_id`),
  CONSTRAINT `fk_s_e_service` FOREIGN KEY (`service_id`) REFERENCES `service` (`service_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='服务-员工 被提供关系';


-- ==========================================
-- Table: service_event_link
-- ==========================================
CREATE TABLE `service_event_link` (
  `id` varchar(64) NOT NULL,
  `service_id` varchar(64) DEFAULT NULL,
  `event_id` varchar(64) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_s_ev_service` (`service_id`),
  KEY `fk_s_ev_event` (`event_id`),
  CONSTRAINT `fk_s_ev_event` FOREIGN KEY (`event_id`) REFERENCES `event` (`event_id`),
  CONSTRAINT `fk_s_ev_service` FOREIGN KEY (`service_id`) REFERENCES `service` (`service_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='服务-事件 关联关系';


-- ==========================================
-- Table: terminal
-- ==========================================
CREATE TABLE `terminal` (
  `terminal_id` varchar(64) NOT NULL COMMENT '终端ID',
  `terminal_type` varchar(100) DEFAULT NULL COMMENT '终端类型',
  `terminal_number` varchar(100) DEFAULT NULL COMMENT '终端号',
  `customer_id` varchar(64) DEFAULT NULL COMMENT '客户ID',
  `product_id` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '产品ID',
  PRIMARY KEY (`terminal_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='终端';


-- ==========================================
-- Table: terminal_product_link
-- ==========================================
CREATE TABLE `terminal_product_link` (
  `terminal_id` varchar(100) NOT NULL,
  `product_id` varchar(100) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='终端与产品关联表';


-- ==========================================
-- Table: valueaddedproduct
-- ==========================================
CREATE TABLE `valueaddedproduct` (
  `product_id` varchar(64) NOT NULL COMMENT '产品ID',
  `product_name` varchar(255) DEFAULT NULL COMMENT '产品名称',
  `product_description` varchar(255) DEFAULT NULL COMMENT '产品描述',
  `price` decimal(10,2) DEFAULT NULL COMMENT '价格',
  PRIMARY KEY (`product_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='增值产品';


-- ==========================================
-- Table: valueaddedproduct_broadbandnetwork_link
-- ==========================================
CREATE TABLE `valueaddedproduct_broadbandnetwork_link` (
  `id` varchar(64) NOT NULL,
  `product_id` varchar(64) DEFAULT NULL,
  `network_id` varchar(64) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_vap_bn_product` (`product_id`),
  KEY `fk_vap_bn_network` (`network_id`),
  CONSTRAINT `fk_vap_bn_network` FOREIGN KEY (`network_id`) REFERENCES `broadbandnetwork` (`network_id`),
  CONSTRAINT `fk_vap_bn_product` FOREIGN KEY (`product_id`) REFERENCES `valueaddedproduct` (`product_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='增值产品-宽带网络 依赖关系';


-- ==========================================
-- Table: valueaddedproduct_wirelessnetwork_link
-- ==========================================
CREATE TABLE `valueaddedproduct_wirelessnetwork_link` (
  `id` varchar(64) NOT NULL,
  `product_id` varchar(64) DEFAULT NULL,
  `network_id` varchar(64) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_vap_wn_product` (`product_id`),
  KEY `fk_vap_wn_network` (`network_id`),
  CONSTRAINT `fk_vap_wn_network` FOREIGN KEY (`network_id`) REFERENCES `wirelessnetwork` (`network_id`),
  CONSTRAINT `fk_vap_wn_product` FOREIGN KEY (`product_id`) REFERENCES `valueaddedproduct` (`product_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='增值产品-无线网络 依赖关系';


-- ==========================================
-- Table: wirelessnetwork
-- ==========================================
CREATE TABLE `wirelessnetwork` (
  `network_id` varchar(64) NOT NULL COMMENT '网络ID',
  `base_station_type` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '基站类型',
  `work_band` varchar(100) DEFAULT NULL COMMENT '工作频段',
  `RSSI` float NOT NULL COMMENT '网络信号强度单位 dBm',
  `operational_status` varchar(50) DEFAULT NULL COMMENT '运行状态',
  `access_device_count` int DEFAULT '0',
  PRIMARY KEY (`network_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='无线网络';


-- ==========================================
-- Table: wirelessnetwork_service_link
-- ==========================================
CREATE TABLE `wirelessnetwork_service_link` (
  `id` varchar(64) NOT NULL,
  `network_id` varchar(64) DEFAULT NULL,
  `service_id` varchar(64) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_wn_s_network` (`network_id`),
  KEY `fk_wn_s_service` (`service_id`),
  CONSTRAINT `fk_wn_s_network` FOREIGN KEY (`network_id`) REFERENCES `wirelessnetwork` (`network_id`),
  CONSTRAINT `fk_wn_s_service` FOREIGN KEY (`service_id`) REFERENCES `service` (`service_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='无线网络-服务 被优化关系';


-- ==========================================
-- Table: wirelessnetwork_terminal_link
-- ==========================================
CREATE TABLE `wirelessnetwork_terminal_link` (
  `id` varchar(64) NOT NULL,
  `network_id` varchar(64) DEFAULT NULL,
  `terminal_id` varchar(64) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_wn_t_network` (`network_id`),
  KEY `fk_wn_t_terminal` (`terminal_id`),
  CONSTRAINT `fk_wn_t_network` FOREIGN KEY (`network_id`) REFERENCES `wirelessnetwork` (`network_id`),
  CONSTRAINT `fk_wn_t_terminal` FOREIGN KEY (`terminal_id`) REFERENCES `terminal` (`terminal_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='无线网络-终端 关系';


-- ==========================================
-- Table: workorder
-- ==========================================
CREATE TABLE `workorder` (
  `work_order_id` varchar(64) NOT NULL COMMENT '工单ID',
  `content` varchar(255) DEFAULT NULL COMMENT '工单内容',
  `status` varchar(100) DEFAULT NULL COMMENT '工单状态',
  `priority` varchar(50) DEFAULT NULL COMMENT '工单优先级',
  `create_time` datetime DEFAULT NULL COMMENT '创建时间',
  `close_time` datetime DEFAULT NULL COMMENT '关闭时间',
  `contact_phone` varchar(20) DEFAULT NULL COMMENT '联系方式(手机号)',
  `order_type` varchar(100) DEFAULT NULL COMMENT '工单类型(与服务类型一致)',
  `customer_id` varchar(64) DEFAULT NULL COMMENT '客户ID',
  `init_advice` text,
  `ganzhiweidu` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`work_order_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='工单';

