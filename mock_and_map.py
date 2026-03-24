#!/usr/bin/env python3
"""
Mock Data Generator & RDB2RDF Mapper for CEM (Customer Experience Management)

This script:
1. Creates DuckDB database with tables from DDL
2. Inserts meaningful mock data for customer experience scenarios
3. Uses rdflib to generate RDF knowledge graph directly
"""

import os
import duckdb
import uuid
from datetime import datetime, timedelta

from rdflib import Graph, Namespace, URIRef, Literal, BNode
from rdflib.namespace import RDF, RDFS, OWL, XSD

DB_PATH = "cem_data.duckdb"
RDF_OUTPUT = "graph.ttl"
ONTOLOGY_FILE = "Onto/cem.owl"

# Namespaces
EX = Namespace("http://ywyinfo.com/example-owl#")
PROV = Namespace("http://www.w3.org/ns/prov#")


def generate_uuid():
    return str(uuid.uuid4())[:16].replace("-", "").upper()


def create_tables(conn):
    """Create core tables based on DDL"""
    print("Creating tables...")

    # Customer table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS customer (
            customer_id VARCHAR PRIMARY KEY,
            name VARCHAR,
            age INTEGER,
            income_level VARCHAR,
            region VARCHAR,
            education_level VARCHAR,
            preferred_product_type VARCHAR
        )
    """)

    # Customer behavior table (simplified)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS customerbehavior (
            behavior_id VARCHAR PRIMARY KEY,
            customer_id VARCHAR,
            iphone VARCHAR,
            avg_bill FLOAT,
            avg_traffic FLOAT,
            satisfaction_score FLOAT,
            network_experience_score FLOAT,
            service_experience_score FLOAT,
            cost_perception_score FLOAT,
            complain_3m INTEGER,
            web_disp_rate FLOAT,
            video_play_succ_rate FLOAT,
            rsrp_good_ratio FLOAT,
            is_5g_pack BOOLEAN,
            current_complaint_times INTEGER,
            bill_cycle VARCHAR
        )
    """)

    # Event table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS event (
            event_id VARCHAR PRIMARY KEY,
            employee_id VARCHAR,
            status VARCHAR,
            description VARCHAR,
            occur_time TIMESTAMP,
            event_type VARCHAR
        )
    """)

    # Customer-Event link
    conn.execute("""
        CREATE TABLE IF NOT EXISTS customer_event_link (
            id VARCHAR PRIMARY KEY,
            customer_id VARCHAR,
            event_id VARCHAR
        )
    """)

    # WorkOrder table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS workorder (
            work_order_id VARCHAR PRIMARY KEY,
            content VARCHAR,
            status VARCHAR,
            priority VARCHAR,
            create_time TIMESTAMP,
            close_time TIMESTAMP,
            contact_phone VARCHAR,
            order_type VARCHAR,
            customer_id VARCHAR,
            init_advice TEXT,
            ganzhiweidu VARCHAR
        )
    """)

    # Employee table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS employee (
            employee_id VARCHAR PRIMARY KEY,
            service_type_responsible VARCHAR,
            name VARCHAR,
            position VARCHAR,
            department VARCHAR
        )
    """)

    # Perception table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS perception (
            perception_id VARCHAR PRIMARY KEY,
            algorithm VARCHAR,
            dimension VARCHAR,
            perception_time TIMESTAMP,
            weidu VARCHAR
        )
    """)

    # Remediation Strategy table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS remediationstrategy (
            strategy_id VARCHAR PRIMARY KEY,
            strategy_name VARCHAR,
            strategy_description VARCHAR,
            network_id VARCHAR,
            product_id VARCHAR
        )
    """)

    # Broadband Network table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS broadbandnetwork (
            network_id VARCHAR PRIMARY KEY,
            fiber_length DECIMAL(10,2),
            operational_status VARCHAR,
            dependent_device VARCHAR,
            Bandwidth FLOAT,
            access_device_count INTEGER
        )
    """)

    # Wireless Network table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS wirelessnetwork (
            network_id VARCHAR PRIMARY KEY,
            base_station_type VARCHAR,
            work_band VARCHAR,
            RSSI FLOAT,
            operational_status VARCHAR,
            access_device_count INTEGER
        )
    """)

    # Event-WorkOrder link
    conn.execute("""
        CREATE TABLE IF NOT EXISTS event_workorder_link (
            id VARCHAR PRIMARY KEY,
            event_id VARCHAR,
            work_order_id VARCHAR
        )
    """)

    # Perception-Remediation link
    conn.execute("""
        CREATE TABLE IF NOT EXISTS perception_remediationstrategy_link (
            id VARCHAR PRIMARY KEY,
            perception_id VARCHAR,
            strategy_id VARCHAR
        )
    """)

    # Event-Perception link
    conn.execute("""
        CREATE TABLE IF NOT EXISTS event_perception_link (
            id VARCHAR PRIMARY KEY,
            event_id VARCHAR,
            perception_id VARCHAR
        )
    """)

    print("Tables created successfully.")


def insert_mock_data(conn):
    """Insert meaningful mock data for customer experience scenarios"""
    print("Inserting mock data...")

    # Insert customers with different profiles
    customers = [
        ("CUST001", "张伟", 35, "高收入", "北京", "本科", "5G融合套餐"),
        ("CUST002", "李娜", 28, "中等收入", "上海", "硕士", "4G畅享套餐"),
        ("CUST003", "王强", 42, "高收入", "广州", "本科", "5G尊享套餐"),
        ("CUST004", "刘芳", 55, "中等收入", "深圳", "大专", "家庭宽带套餐"),
        ("CUST005", "陈明", 31, "低收入", "成都", "本科", "基础套餐"),
        ("CUST006", "赵敏", 26, "中等收入", "杭州", "硕士", "5G融合套餐"),
        ("CUST007", "孙涛", 48, "高收入", "南京", "本科", "企业套餐"),
        ("CUST008", "周雪", 33, "中等收入", "武汉", "大专", "4G畅享套餐"),
    ]

    for cust in customers:
        conn.execute("""
            INSERT INTO customer (customer_id, name, age, income_level, region, education_level, preferred_product_type)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, cust)

    # Insert employees
    employees = [
        ("EMP001", "网络优化", "李工", "网络工程师", "网络部"),
        ("EMP002", "客户服务", "王客服", "客服专员", "客服部"),
        ("EMP003", "故障处理", "张师傅", "维修工程师", "运维部"),
        ("EMP004", "资费咨询", "陈顾问", "业务顾问", "市场部"),
        ("EMP005", "综合服务", "刘经理", "服务经理", "客户体验部"),
    ]

    for emp in employees:
        conn.execute("""
            INSERT INTO employee (employee_id, service_type_responsible, name, position, department)
            VALUES (?, ?, ?, ?, ?)
        """, emp)

    # Insert customer behaviors - representing different satisfaction scenarios
    behaviors = [
        # CUST001: High satisfaction customer
        ("BEH001", "CUST001", "13800138001", 199.0, 25.5, 4.8, 4.7, 4.9, 4.6, 0, 95.5, 98.2, 92.0, True, 0, "2026-03"),
        # CUST002: Low network satisfaction
        ("BEH002", "CUST002", "13800138002", 99.0, 15.2, 2.5, 2.0, 3.5, 4.0, 2, 65.0, 70.5, 55.0, False, 1, "2026-03"),
        # CUST003: Good overall
        ("BEH003", "CUST003", "13800138003", 299.0, 45.0, 4.5, 4.6, 4.4, 4.3, 1, 88.0, 92.0, 85.0, True, 0, "2026-03"),
        # CUST004: Service complaints
        ("BEH004", "CUST004", "13800138004", 129.0, 8.5, 2.8, 3.5, 2.0, 3.0, 3, 78.0, 85.0, 72.0, False, 2, "2026-03"),
        # CUST005: Cost sensitive, low satisfaction
        ("BEH005", "CUST005", "13800138005", 59.0, 3.2, 2.0, 2.5, 2.2, 1.5, 2, 60.0, 65.0, 58.0, False, 1, "2026-03"),
        # CUST006: Network issues
        ("BEH006", "CUST006", "13800138006", 159.0, 20.0, 3.2, 2.8, 4.0, 3.8, 1, 72.0, 75.0, 68.0, True, 1, "2026-03"),
        # CUST007: Business customer, high demand
        ("BEH007", "CUST007", "13800138007", 599.0, 100.0, 4.2, 4.0, 4.5, 4.0, 0, 90.0, 93.0, 88.0, True, 0, "2026-03"),
        # CUST008: Frequent complaints
        ("BEH008", "CUST008", "13800138008", 89.0, 12.0, 2.2, 2.0, 2.5, 3.0, 4, 55.0, 60.0, 50.0, False, 3, "2026-03"),
    ]

    for beh in behaviors:
        conn.execute("""
            INSERT INTO customerbehavior (
                behavior_id, customer_id, iphone, avg_bill, avg_traffic,
                satisfaction_score, network_experience_score, service_experience_score,
                cost_perception_score, complain_3m, web_disp_rate, video_play_succ_rate,
                rsrp_good_ratio, is_5g_pack, current_complaint_times, bill_cycle
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, beh)

    # Insert events - representing customer issues
    base_time = datetime(2026, 3, 15, 10, 0, 0)
    events = [
        ("EVT001", "EMP002", "已处理", "用户反映网速慢，视频卡顿", base_time, "网络质量投诉"),
        ("EVT002", "EMP002", "处理中", "账单疑问，资费不符", base_time + timedelta(hours=2), "资费争议"),
        ("EVT003", "EMP001", "已处理", "信号弱，通话中断", base_time + timedelta(hours=4), "网络覆盖问题"),
        ("EVT004", "EMP003", "待处理", "宽带无法连接，光猫红灯", base_time + timedelta(days=1), "宽带故障"),
        ("EVT005", "EMP002", "已处理", "客服态度差，未解决问题", base_time + timedelta(days=2), "服务投诉"),
        ("EVT006", "EMP004", "处理中", "希望更换套餐", base_time + timedelta(days=3), "套餐变更"),
        ("EVT007", "EMP001", "待处理", "5G信号不稳定", base_time + timedelta(days=4), "5G网络问题"),
        ("EVT008", "EMP005", "已处理", "综合体验反馈", base_time + timedelta(days=5), "满意度回访"),
    ]

    for evt in events:
        conn.execute("""
            INSERT INTO event (event_id, employee_id, status, description, occur_time, event_type)
            VALUES (?, ?, ?, ?, ?, ?)
        """, evt)

    # Link customers to events
    customer_event_links = [
        ("CEL001", "CUST002", "EVT001"),  # 李娜 - 网速慢
        ("CEL002", "CUST005", "EVT002"),  # 陈明 - 资费疑问
        ("CEL003", "CUST002", "EVT003"),  # 李娜 - 信号弱
        ("CEL004", "CUST004", "EVT004"),  # 刘芳 - 宽带故障
        ("CEL005", "CUST004", "EVT005"),  # 刘芳 - 服务投诉
        ("CEL006", "CUST001", "EVT006"),  # 张伟 - 套餐变更
        ("CEL007", "CUST006", "EVT007"),  # 赵敏 - 5G问题
        ("CEL008", "CUST003", "EVT008"),  # 王强 - 满意度回访
    ]

    for link in customer_event_links:
        conn.execute("""
            INSERT INTO customer_event_link (id, customer_id, event_id)
            VALUES (?, ?, ?)
        """, link)

    # Insert work orders
    workorders = [
        ("WO001", "处理用户网络速度慢问题", "已关闭", "高", base_time, base_time + timedelta(hours=4), "13800138002", "网络优化", "CUST002", "建议进行基站优化，调整用户所在区域信号覆盖", "网优维度"),
        ("WO002", "账单费用核实", "处理中", "中", base_time + timedelta(hours=2), None, "13800138005", "资费查询", "CUST005", "核实套餐资费，解释账单明细", "资费维度"),
        ("WO003", "宽带故障维修", "待分配", "高", base_time + timedelta(days=1), None, "13800138004", "故障维修", "CUST004", "安排上门检修光猫和线路", "服务维度"),
        ("WO004", "5G信号优化", "处理中", "中", base_time + timedelta(days=4), None, "13800138006", "网络优化", "CUST006", "检查5G基站覆盖，建议切换4G网络", "网优维度"),
        ("WO005", "客户满意度提升", "已关闭", "低", base_time + timedelta(days=5), base_time + timedelta(days=6), "13800138003", "满意度调查", "CUST003", "客户满意，无需特殊处理", "服务维度"),
    ]

    for wo in workorders:
        conn.execute("""
            INSERT INTO workorder (work_order_id, content, status, priority, create_time, close_time,
                                   contact_phone, order_type, customer_id, init_advice, ganzhiweidu)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, wo)

    # Link events to work orders
    event_workorder_links = [
        ("EWL001", "EVT001", "WO001"),
        ("EWL002", "EVT002", "WO002"),
        ("EWL003", "EVT004", "WO003"),
        ("EWL004", "EVT007", "WO004"),
        ("EWL005", "EVT008", "WO005"),
    ]

    for link in event_workorder_links:
        conn.execute("""
            INSERT INTO event_workorder_link (id, event_id, work_order_id)
            VALUES (?, ?, ?)
        """, link)

    # Insert perception analyses
    perceptions = [
        ("PER001", "综合满意度评估算法", "基于多维度评分计算综合满意度，包括网络体验、服务体验、资费感知等维度，权重分别为0.4, 0.3, 0.3", base_time, "满意度"),
        ("PER002", "网络质量感知算法", "分析网络指标如RSRP、网页显示成功率、视频播放成功率等，识别网络问题导致的低满意度", base_time, "网优维度"),
        ("PER003", "服务质量感知算法", "分析投诉次数、客服响应时间、工单处理时效等服务指标，识别服务问题", base_time, "服务维度"),
        ("PER004", "资费匹配度感知算法", "分析用户套餐与实际使用量的匹配程度，识别资费过高或套餐不合适问题", base_time, "资费维度"),
    ]

    for per in perceptions:
        conn.execute("""
            INSERT INTO perception (perception_id, algorithm, dimension, perception_time, weidu)
            VALUES (?, ?, ?, ?, ?)
        """, per)

    # Insert remediation strategies
    strategies = [
        ("STR001", "网络优化策略", "针对RSRP低于-105的区域进行基站功率调整或新增微基站，优化信号覆盖", None, None),
        ("STR002", "视频卡顿修复策略", "优化视频缓存策略，增加带宽分配优先级，建议用户更换支持更高频段的终端", None, None),
        ("STR003", "服务补救策略", "主动联系客户致歉，提供话费补偿或增值服务赠送，安排专人跟进", None, None),
        ("STR004", "资费优化策略", "推荐更适合用户使用习惯的套餐，提供限时优惠或流量赠送", None, None),
        ("STR005", "网页体验优化策略", "优化DNS解析，部署CDN加速，减少网页加载时延", None, None),
    ]

    for strat in strategies:
        conn.execute("""
            INSERT INTO remediationstrategy (strategy_id, strategy_name, strategy_description, network_id, product_id)
            VALUES (?, ?, ?, ?, ?)
        """, strat)

    # Link perceptions to strategies
    perception_strategy_links = [
        ("PSL001", "PER002", "STR001"),  # 网络感知 -> 网络优化
        ("PSL002", "PER002", "STR002"),  # 网络感知 -> 视频卡顿修复
        ("PSL003", "PER003", "STR003"),  # 服务感知 -> 服务补救
        ("PSL004", "PER004", "STR004"),  # 资费感知 -> 资费优化
        ("PSL005", "PER002", "STR005"),  # 网络感知 -> 网页体验优化
    ]

    for link in perception_strategy_links:
        conn.execute("""
            INSERT INTO perception_remediationstrategy_link (id, perception_id, strategy_id)
            VALUES (?, ?, ?)
        """, link)

    # Link events to perceptions
    event_perception_links = [
        ("EPL001", "EVT001", "PER002"),  # 网速慢 -> 网络感知
        ("EPL002", "EVT002", "PER004"),  # 资费疑问 -> 资费感知
        ("EPL003", "EVT003", "PER002"),  # 信号弱 -> 网络感知
        ("EPL004", "EVT005", "PER003"),  # 服务投诉 -> 服务感知
        ("EPL005", "EVT007", "PER002"),  # 5G问题 -> 网络感知
    ]

    for link in event_perception_links:
        conn.execute("""
            INSERT INTO event_perception_link (id, event_id, perception_id)
            VALUES (?, ?, ?)
        """, link)

    # Insert network data
    broadband_networks = [
        ("BN001", 2.5, "正常", "光猫ONU001", 1000.0, 128),
        ("BN002", 1.8, "正常", "光猫ONU002", 500.0, 64),
        ("BN003", 3.2, "告警", "光猫ONU003", 1000.0, 256),
    ]

    for bn in broadband_networks:
        conn.execute("""
            INSERT INTO broadbandnetwork (network_id, fiber_length, operational_status, dependent_device, Bandwidth, access_device_count)
            VALUES (?, ?, ?, ?, ?, ?)
        """, bn)

    wireless_networks = [
        ("WN001", "宏基站", "3.5GHz", -95.0, "正常", 512),
        ("WN002", "微基站", "2.6GHz", -105.0, "正常", 128),
        ("WN003", "室分", "4.9GHz", -85.0, "正常", 64),
        ("WN004", "宏基站", "3.5GHz", -115.0, "告警", 256),
    ]

    for wn in wireless_networks:
        conn.execute("""
            INSERT INTO wirelessnetwork (network_id, base_station_type, work_band, RSSI, operational_status, access_device_count)
            VALUES (?, ?, ?, ?, ?, ?)
        """, wn)

    print("Mock data inserted successfully.")


def generate_rdf_from_db(conn):
    """Generate RDF directly from database using rdflib"""
    print("Generating RDF using rdflib...")

    g = Graph()
    g.bind("ex", EX)
    g.bind("rdfs", RDFS)
    g.bind("xsd", XSD)
    g.bind("prov", PROV)

    # Map customers
    print("  Mapping customers...")
    customers = conn.execute("SELECT * FROM customer").fetchall()
    for row in customers:
        customer_uri = EX[f"customer_{row[0]}"]
        g.add((customer_uri, RDF.type, EX.customer))
        g.add((customer_uri, EX.customer_客户ID, Literal(row[0])))
        g.add((customer_uri, EX.customer_姓名, Literal(row[1])))
        if row[2]:
            g.add((customer_uri, EX.customer_年龄, Literal(row[2], datatype=XSD.integer)))
        if row[3]:
            g.add((customer_uri, EX.customer_收入水平, Literal(row[3])))
        if row[4]:
            g.add((customer_uri, EX.customer_地域分布, Literal(row[4])))
        if row[5]:
            g.add((customer_uri, EX.customer_教育水平, Literal(row[5])))
        if row[6]:
            g.add((customer_uri, EX.customer_偏好套餐类型, Literal(row[6])))

    # Map customer behaviors
    print("  Mapping customer behaviors...")
    behaviors = conn.execute("SELECT * FROM customerbehavior").fetchall()
    for row in behaviors:
        behavior_uri = EX[f"customerbehavior_{row[0]}"]
        g.add((behavior_uri, RDF.type, EX.customerbehavior))
        g.add((behavior_uri, EX.customerbehavior_客户行为ID, Literal(row[0])))
        g.add((behavior_uri, EX.customerbehavior_客户ID, Literal(row[1])))
        if row[2]:
            g.add((behavior_uri, EX.customerbehavior_手机号, Literal(row[2])))
        if row[3]:
            g.add((behavior_uri, EX.customerbehavior_月均账单金额, Literal(row[3], datatype=XSD.decimal)))
        if row[4]:
            g.add((behavior_uri, EX.customerbehavior_月均流量使用, Literal(row[4], datatype=XSD.decimal)))
        if row[5]:
            g.add((behavior_uri, EX.customerbehavior_满意度评分, Literal(row[5], datatype=XSD.float)))
        if row[6]:
            g.add((behavior_uri, EX.customerbehavior_网络体验评分, Literal(row[6], datatype=XSD.float)))
        if row[7]:
            g.add((behavior_uri, EX.customerbehavior_服务体验评分, Literal(row[7], datatype=XSD.float)))
        if row[8]:
            g.add((behavior_uri, EX.customerbehavior_资费感知评分, Literal(row[8], datatype=XSD.float)))
        if row[9]:
            g.add((behavior_uri, EX.customerbehavior_近3个月投诉次数, Literal(row[9], datatype=XSD.integer)))
        if row[10]:
            g.add((behavior_uri, EX.customerbehavior_网页显示成功率, Literal(row[10], datatype=XSD.float)))
        if row[11]:
            g.add((behavior_uri, EX.customerbehavior_视频播放成功率, Literal(row[11], datatype=XSD.float)))
        if row[12]:
            g.add((behavior_uri, EX.customerbehavior_RSRP优良率, Literal(row[12], datatype=XSD.float)))
        if row[13] is not None:
            g.add((behavior_uri, EX.customerbehavior_是否5G套餐, Literal(row[13], datatype=XSD.boolean)))
        if row[14]:
            g.add((behavior_uri, EX.customerbehavior_本月投诉次数, Literal(row[14], datatype=XSD.integer)))
        if row[15]:
            g.add((behavior_uri, EX.customerbehavior_月账期, Literal(row[15])))

        # Link behavior to customer
        customer_uri = EX[f"customer_{row[1]}"]
        g.add((behavior_uri, EX.hasCustomer, customer_uri))
        g.add((customer_uri, EX.hasBehavior, behavior_uri))

    # Map events
    print("  Mapping events...")
    events = conn.execute("SELECT * FROM event").fetchall()
    for row in events:
        event_uri = EX[f"event_{row[0]}"]
        g.add((event_uri, RDF.type, EX.event))
        g.add((event_uri, EX.event_event_id, Literal(row[0])))
        if row[1]:
            employee_uri = EX[f"employee_{row[1]}"]
            g.add((event_uri, EX.hasEmployee, employee_uri))
        if row[2]:
            g.add((event_uri, EX.event_事件状态, Literal(row[2])))
        if row[3]:
            g.add((event_uri, EX.event_事件描述, Literal(row[3])))
        if row[4]:
            g.add((event_uri, EX.event_发生时间, Literal(row[4].isoformat(), datatype=XSD.dateTime)))
        if row[5]:
            g.add((event_uri, EX.event_事件类型, Literal(row[5])))

    # Map workorders
    print("  Mapping work orders...")
    workorders = conn.execute("SELECT * FROM workorder").fetchall()
    for row in workorders:
        wo_uri = EX[f"workorder_{row[0]}"]
        g.add((wo_uri, RDF.type, EX.workorder))
        g.add((wo_uri, EX.workorder_工单ID, Literal(row[0])))
        if row[1]:
            g.add((wo_uri, EX.workorder_工单内容, Literal(row[1])))
        if row[2]:
            g.add((wo_uri, EX.workorder_工单状态, Literal(row[2])))
        if row[3]:
            g.add((wo_uri, EX.workorder_工单优先级, Literal(row[3])))
        if row[4]:
            g.add((wo_uri, EX.workorder_创建时间, Literal(row[4].isoformat(), datatype=XSD.dateTime)))
        if row[5]:
            g.add((wo_uri, EX.workorder_关闭时间, Literal(row[5].isoformat(), datatype=XSD.dateTime)))
        if row[6]:
            g.add((wo_uri, EX.workorder_联系方式, Literal(row[6])))
        if row[7]:
            g.add((wo_uri, EX.workorder_工单类型, Literal(row[7])))
        if row[8]:
            customer_uri = EX[f"customer_{row[8]}"]
            g.add((wo_uri, EX.hasCustomer, customer_uri))
        if row[9]:
            g.add((wo_uri, EX.workorder_初始建议, Literal(row[9])))
        if row[10]:
            g.add((wo_uri, EX.workorder_感知维度, Literal(row[10])))

    # Map employees
    print("  Mapping employees...")
    employees = conn.execute("SELECT * FROM employee").fetchall()
    for row in employees:
        emp_uri = EX[f"employee_{row[0]}"]
        g.add((emp_uri, RDF.type, EX.employee))
        g.add((emp_uri, EX.employee_员工ID, Literal(row[0])))
        if row[1]:
            g.add((emp_uri, EX.employee_负责服务类型, Literal(row[1])))
        if row[2]:
            g.add((emp_uri, EX.employee_姓名, Literal(row[2])))
        if row[3]:
            g.add((emp_uri, EX.employee_职位, Literal(row[3])))
        if row[4]:
            g.add((emp_uri, EX.employee_所属部门, Literal(row[4])))

    # Map perceptions
    print("  Mapping perceptions...")
    perceptions = conn.execute("SELECT * FROM perception").fetchall()
    for row in perceptions:
        per_uri = EX[f"perception_{row[0]}"]
        g.add((per_uri, RDF.type, EX.perception))
        g.add((per_uri, EX.perception_感知ID, Literal(row[0])))
        if row[1]:
            g.add((per_uri, EX.perception_感知分析算法, Literal(row[1])))
        if row[2]:
            g.add((per_uri, EX.perception_感知分析算法原理描述, Literal(row[2])))
        if row[3]:
            g.add((per_uri, EX.perception_感知时间, Literal(row[3].isoformat(), datatype=XSD.dateTime)))
        if row[4]:
            g.add((per_uri, EX.perception_感知维度, Literal(row[4])))

    # Map remediation strategies
    print("  Mapping remediation strategies...")
    strategies = conn.execute("SELECT * FROM remediationstrategy").fetchall()
    for row in strategies:
        strat_uri = EX[f"remediationstrategy_{row[0]}"]
        g.add((strat_uri, RDF.type, EX.remediationstrategy))
        g.add((strat_uri, EX.remediationstrategy_修复策略ID, Literal(row[0])))
        if row[1]:
            g.add((strat_uri, EX.remediationstrategy_修复策略名称, Literal(row[1])))
        if row[2]:
            g.add((strat_uri, EX.remediationstrategy_修复策略说明, Literal(row[2])))

    # Map networks
    print("  Mapping networks...")
    bb_networks = conn.execute("SELECT * FROM broadbandnetwork").fetchall()
    for row in bb_networks:
        net_uri = EX[f"broadbandnetwork_{row[0]}"]
        g.add((net_uri, RDF.type, EX.broadbandnetwork))
        g.add((net_uri, EX.broadbandnetwork_网络ID, Literal(row[0])))
        if row[1]:
            g.add((net_uri, EX.broadbandnetwork_光纤长度单位为公里, Literal(row[1], datatype=XSD.decimal)))
        if row[2]:
            g.add((net_uri, EX.broadbandnetwork_运行状态, Literal(row[2])))
        if row[3]:
            g.add((net_uri, EX.broadbandnetwork_依赖设备, Literal(row[3])))
        if row[4]:
            g.add((net_uri, EX.broadbandnetwork_网络带宽, Literal(row[4], datatype=XSD.float)))
        if row[5]:
            g.add((net_uri, EX.broadbandnetwork_接入设备数量, Literal(row[5], datatype=XSD.integer)))

    wl_networks = conn.execute("SELECT * FROM wirelessnetwork").fetchall()
    for row in wl_networks:
        net_uri = EX[f"wirelessnetwork_{row[0]}"]
        g.add((net_uri, RDF.type, EX.wirelessnetwork))
        g.add((net_uri, EX.wirelessnetwork_网络ID, Literal(row[0])))
        if row[1]:
            g.add((net_uri, EX.wirelessnetwork_基站类型, Literal(row[1])))
        if row[2]:
            g.add((net_uri, EX.wirelessnetwork_工作频段, Literal(row[2])))
        if row[3]:
            g.add((net_uri, EX.wirelessnetwork_RSRP信号强度, Literal(row[3], datatype=XSD.float)))
        if row[4]:
            g.add((net_uri, EX.wirelessnetwork_运行状态, Literal(row[4])))
        if row[5]:
            g.add((net_uri, EX.wirelessnetwork_接入设备数量, Literal(row[5], datatype=XSD.integer)))

    # Map relationships
    print("  Mapping relationships...")

    # Customer-Event links
    ce_links = conn.execute("SELECT * FROM customer_event_link").fetchall()
    for row in ce_links:
        link_uri = EX[f"customer_event_{row[0]}"]
        g.add((link_uri, RDF.type, EX.customer_event_yf))
        if row[1]:
            customer_uri = EX[f"customer_{row[1]}"]
            event_uri = EX[f"event_{row[2]}"]
            g.add((link_uri, EX.hasCustomer, customer_uri))
            g.add((link_uri, EX.hasEvent, event_uri))
            g.add((customer_uri, EX.hasEvent, event_uri))
            g.add((event_uri, EX.affectsCustomer, customer_uri))

    # Event-WorkOrder links
    ew_links = conn.execute("SELECT * FROM event_workorder_link").fetchall()
    for row in ew_links:
        link_uri = EX[f"event_workorder_{row[0]}"]
        g.add((link_uri, RDF.type, EX.event_workorder_GS))
        if row[1]:
            event_uri = EX[f"event_{row[1]}"]
            wo_uri = EX[f"workorder_{row[2]}"]
            g.add((link_uri, EX.hasEvent, event_uri))
            g.add((link_uri, EX.hasWorkOrder, wo_uri))
            g.add((event_uri, EX.hasWorkOrder, wo_uri))
            g.add((wo_uri, EX.relatedToEvent, event_uri))

    # Perception-Remediation links
    pr_links = conn.execute("SELECT * FROM perception_remediationstrategy_link").fetchall()
    for row in pr_links:
        link_uri = EX[f"perception_remediation_{row[0]}"]
        g.add((link_uri, RDF.type, EX.perception_remediationstrategy_link))
        if row[1]:
            per_uri = EX[f"perception_{row[1]}"]
            strat_uri = EX[f"remediationstrategy_{row[2]}"]
            g.add((link_uri, EX.hasPerception, per_uri))
            g.add((link_uri, EX.hasRemediationStrategy, strat_uri))
            g.add((per_uri, EX.suggestsStrategy, strat_uri))
            g.add((strat_uri, EX.addressesPerception, per_uri))

    # Event-Perception links
    ep_links = conn.execute("SELECT * FROM event_perception_link").fetchall()
    for row in ep_links:
        link_uri = EX[f"event_perception_{row[0]}"]
        g.add((link_uri, RDF.type, EX.event_perception_link))
        if row[1]:
            event_uri = EX[f"event_{row[1]}"]
            per_uri = EX[f"perception_{row[2]}"]
            g.add((link_uri, EX.hasEvent, event_uri))
            g.add((link_uri, EX.hasPerception, per_uri))
            g.add((event_uri, EX.hasPerception, per_uri))
            g.add((per_uri, EX.detectsEvent, event_uri))

    # Serialize to Turtle
    g.serialize(destination=RDF_OUTPUT, format="turtle")

    print(f"\nRDF graph saved to: {RDF_OUTPUT}")
    print(f"Total triples generated: {len(g)}")

    return g


def main():
    print("=" * 60)
    print("CEM Mock Data Generator & RDB2RDF Mapper")
    print("=" * 60)

    # Remove existing database
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        print(f"Removed existing database: {DB_PATH}")

    # Connect to DuckDB
    conn = duckdb.connect(DB_PATH)

    try:
        # Create tables and insert data
        create_tables(conn)
        insert_mock_data(conn)

        # Verify data
        print("\nData verification:")
        for table in ["customer", "customerbehavior", "event", "workorder", "employee", "perception", "remediationstrategy"]:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"  - {table}: {count} rows")

        # Generate RDF
        generate_rdf_from_db(conn)

        conn.close()

        print("\n" + "=" * 60)
        print("Process completed successfully!")
        print(f"Database: {DB_PATH}")
        print(f"RDF Graph: {RDF_OUTPUT}")
        print("=" * 60)

    except Exception as e:
        conn.close()
        print(f"Error: {e}")
        raise


if __name__ == "__main__":
    main()
