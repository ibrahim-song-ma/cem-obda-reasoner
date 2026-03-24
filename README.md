# CEM OBDA Reasoner

基于本体的客户体验管理数据访问与推理系统 (Customer Experience Management - Ontology-Based Data Access Reasoner)

## 架构概述

```
┌─────────────────────────────────────────────────────────────┐
│                      User Interface                         │
│                    (Claude Code / CLI)                      │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                   Agent Core (Claude)                       │
│  ┌─────────────────┐  ┌─────────────────┐  ┌──────────────┐ │
│  │ Intent Understanding│  │ SPARQL Generation│  │ Result Explanation│ │
│  └─────────────────┘  └─────────────────┘  └──────────────┘ │
└──────────────────────────┬──────────────────────────────────┘
                           │ HTTP
                           ▼
┌─────────────────────────────────────────────────────────────┐
│              Local Reasoning Server (Python)                │
│  ┌─────────────────┐  ┌─────────────────┐  ┌──────────────┐ │
│  │ SPARQL Execution│  │ OWL Reasoning   │  │ Graph Mgmt   │ │
│  │ (rdflib)        │  │ (owlrl)         │  │ (DuckDB+RDF) │ │
│  └─────────────────┘  └─────────────────┘  └──────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

## 核心特性

- **OWL 2 RL 推理**: 使用 `owlrl` 自动推断隐式关系
- **SPARQL 查询**: 基于 RDF 图谱的灵活查询
- **因果链发现**: 客户 → 事件 → 感知 → 修复策略
- **本地推理**: 无需外部 LLM 调用，完全本地化

## 快速开始

### 1. 安装依赖

```bash
python3 -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. 生成数据

```bash
python mock_and_map.py
```

这将创建：
- `cem_data.duckdb` - DuckDB 数据库
- `graph.ttl` - RDF 图谱

### 3. 启动推理服务器

```bash
uvicorn reasoning_server:app --port 8000
```

### 4. 测试查询

```bash
# 健康检查
curl http://localhost:8000/health

# 获取本体 Schema
curl http://localhost:8000/schema

# 执行 SPARQL 查询
curl -X POST http://localhost:8000/sparql \
  -H "Content-Type: application/json" \
  -d '{"query": "PREFIX ex: <http://ywyinfo.com/example-owl#> SELECT * WHERE { ?s a ex:customer } LIMIT 5"}'
```

## API 端点

| 端点 | 方法 | 描述 |
|------|------|------|
| `/health` | GET | 健康检查，返回图谱状态 |
| `/schema` | GET | 获取本体结构（类、属性、关系） |
| `/sparql` | POST | 执行 SPARQL 查询 |
| `/causal/{customer_id}` | GET | 获取客户的因果链路径 |
| `/sample/{class_name}` | GET | 获取某类的样本实例 |

## 因果链模型

```
customer (客户)
    ↓ hasEvent
event (事件)
    ↓ hasPerception
perception (感知分析)
    ↓ suggestsStrategy
remediationstrategy (修复策略)
```

## 示例：查询低满意度客户

```sparql
PREFIX ex: <http://ywyinfo.com/example-owl#>

SELECT ?customer ?name ?phone ?satisfaction ?eventDesc
WHERE {
  ?customer a ex:customer ;
            ex:customer_姓名 ?name ;
            ex:hasBehavior ?behavior .
  ?behavior ex:customerbehavior_手机号 ?phone ;
            ex:customerbehavior_满意度评分 ?satisfaction .
  OPTIONAL {
    ?customer ex:hasEvent ?event .
    ?event ex:event_事件描述 ?eventDesc .
  }
  FILTER(?satisfaction < 3.0)
}
```

## 项目结构

```
.
├── Onto/
│   └── cem.owl                 # OWL 本体定义
├── DDL/
│   └── ctc_data_ddl.sql        # 关系数据库 Schema
├── reasoning_server.py         # FastAPI 推理服务
├── reasoning_agent.py          # 推理 Agent（含 LLM 集成）
├── mock_and_map.py             # 数据生成与映射脚本
├── mapping.yaml                # R2RML 风格映射配置
├── graph.ttl                   # 生成的 RDF 图谱
├── reasoned_graph.ttl          # 推理后的 RDF 图谱
├── CLAUDE.md                   # Claude Code 项目上下文
└── .claude/
    └── skills/
        └── obda-query/         # Claude 技能定义
```

## 技术栈

- **数据库**: DuckDB (本地文件)
- **本体与图谱**: RDFLib
- **推理**: owlrl (OWL 2 RL)
- **服务器**: FastAPI + Uvicorn
- **映射**: 自定义 Python 实现

## 许可证

MIT
