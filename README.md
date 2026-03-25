# CEM OBDA Semantic Prototype

基于 DuckDB、ontology、RDFLib 和 `owlrl` 的本地语义原型，用于客户体验管理数据的 RDF 物化、OWL RL 闭包推理、SPARQL 查询，以及受约束的路径解释。

## 这是什么

当前项目的准确定位是：

- DuckDB -> RDF 映射
- ontology 加载
- OWL 2 RL 闭包推理
- SPARQL 查询
- 可选的 Analyzer 路径解释

它**不是**：

- Ontop 式标准虚拟 OBDA 平台
- Jena 式完整规则推理平台
- 任意业务规则都能自动推出的“全能推理机”

更准确地说，它是一个**本地、小规模、以 ontology 和映射为核心的语义原型**。

## 当前架构

```text
User / Claude Code / Codex
  -> obda-query skill
  -> reasoning_server.py
      - materialize RDF from DuckDB via mapping.yaml
      - load ontology from Onto/cem.owl
      - run owlrl closure
      - expose /sparql /schema /sample /reload /health
      - expose constrained /analysis/... endpoints
```

推荐的逻辑分层：

1. `mapping.yaml`
   负责把 DuckDB 中的事实映射为 RDF
2. `Onto/cem.owl`
   负责定义 ontology 词汇和可推理公理
3. `reasoning_server.py`
   负责物化、推理、查询与受约束分析
4. `obda-query` skill
   负责 schema-first 查询流程、SPARQL 生成和结果解释

## 当前能力边界

### 已具备

- DuckDB 中的数据可实时物化为 RDF
- 服务器启动时会加载 ontology 并执行 `owlrl` 推理
- `/sparql` 查询的是推理后的图
- `/sample` 可用于 grounding
- `/analysis/...` 可做受约束的路径、邻域和推理新增关系查看

### 尚未具备

- 自动从 ontology 注解中发现 analyzer profile
- 大规模图谱场景下的高性能推理平台能力
- 用 OWL RL 自然表达复杂业务规则，如阈值打标、客户分群、综合评分分类

### 一个关键原则

如果某个“隐藏关系”希望由推理自动出现，必须先在 ontology 中写出相应公理，例如：

- `owl:inverseOf`
- `owl:propertyChainAxiom`
- `rdfs:subPropertyOf`

如果 ontology 没有这些公理，就不应期待 reasoner 自动推出该关系。

## 快速开始

### 1. 安装依赖

macOS / Linux:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Windows PowerShell:

```powershell
py -3 -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 2. 生成示例数据

```bash
.venv/bin/python mock_and_map.py
```

Windows:

```powershell
.venv\Scripts\python.exe mock_and_map.py
```

这一步会生成：

- `cem_data.duckdb`
- 示例 RDF 导出文件，例如 `graph.ttl`

注意：

- 服务器当前主路径是**从 DuckDB + mapping.yaml 实时物化**
- `graph.ttl` 不是服务器运行时的必须输入

### 3. 启动服务器

macOS / Linux:

```bash
.venv/bin/python -m uvicorn reasoning_server:app --port 8000
```

Windows PowerShell:

```powershell
.venv\Scripts\python.exe -m uvicorn reasoning_server:app --port 8000
```

启动时服务器会：

1. 从 DuckDB 读取数据并物化 RDF
2. 加载 `Onto/cem.owl`
3. 执行 OWL RL 闭包推理
4. 构建当前的 analyzer 索引

## 主要 API

### 核心接口

| Endpoint | Method | Description |
|---|---|---|
| `/health` | `GET` | 服务状态、triples 数、analyzer profile 列表 |
| `/schema` | `GET` | ontology schema 信息 |
| `/sample/{class_name}` | `GET` | 类样本实例，用于 grounding |
| `/sparql` | `POST` | 在推理后图上执行 SPARQL |
| `/reload` | `POST` | 从 DuckDB 重新物化并重新推理 |

### Analyzer 接口

| Endpoint | Method | Description |
|---|---|---|
| `/analysis/profiles` | `GET` | 当前可用分析档位 |
| `/analysis/paths` | `GET/POST` | 受约束路径分析 |
| `/analysis/neighborhood` | `POST` | 局部邻域子图 |
| `/analysis/inferred-relations` | `POST` | 推理新增关系查看 |
| `/analysis/explain` | `POST` | 将路径转换为可读解释 |
| `/causal/{customer_id}` | `GET` | `causal` profile 的兼容别名 |

说明：

- `/causal/{customer_id}` 是兼容接口，不是长期唯一入口
- `/sample` 是查询工作流的一部分，不是可有可无的调试接口

## 推荐查询流程

对于自然语言问题，推荐始终按以下顺序：

1. 先查 `/schema`
2. 如果按手机号、ID、状态、评分等字段过滤，先确认属性 domain
3. 如果 schema 不够确定，再查 `/sample/{class_name}`
4. 再写主 SPARQL
5. 只有当用户问“为什么”“路径是什么”“有哪些隐藏关系”时，才调用 `/analysis/...`

这也是 `obda-query` skill 当前要求遵守的协议。

## Skill 与客户端

仓库里已经包含本地 skill：

- [`.agents/skills/obda-query/SKILL.md`](.agents/skills/obda-query/SKILL.md)

还包含两个辅助 client：

- Bash client:
  [`.agents/skills/obda-query/scripts/obda_api.sh`](.agents/skills/obda-query/scripts/obda_api.sh)
- Python client:
  [`.agents/skills/obda-query/scripts/obda_api.py`](.agents/skills/obda-query/scripts/obda_api.py)

当前建议：

- Bash / Git Bash / WSL 用户优先用 `obda_api.sh`
- Windows 原生 PowerShell 用户优先用 `obda_api.py`

示例：

macOS / Linux:

```bash
bash .agents/skills/obda-query/scripts/obda_api.sh health
bash .agents/skills/obda-query/scripts/obda_api.sh schema
bash .agents/skills/obda-query/scripts/obda_api.sh causal CUST004
```

Windows PowerShell:

```powershell
.venv\Scripts\python.exe .agents\skills\obda-query\scripts\obda_api.py health
.venv\Scripts\python.exe .agents\skills\obda-query\scripts\obda_api.py schema
.venv\Scripts\python.exe .agents\skills\obda-query\scripts\obda_api.py causal CUST004
```

## SPARQL 示例

### 低满意度客户

```sparql
PREFIX ex: <http://ywyinfo.com/example-owl#>

SELECT ?customer ?name ?phone ?satisfaction
WHERE {
  ?customer a ex:customer ;
            ex:customer_姓名 ?name ;
            ex:hasBehavior ?behavior .
  ?behavior ex:customerbehavior_手机号 ?phone ;
            ex:customerbehavior_满意度评分 ?satisfaction .
  FILTER(?satisfaction < 3.0)
}
```

### 客户相关事件与策略路径

如果你想看“路径解释”，优先使用 analyzer 或 `/causal`，而不是手工多轮试探式查询。

例如：

```bash
bash .agents/skills/obda-query/scripts/obda_api.sh causal CUST004
```

## 项目结构

```text
.
├── AGENTS.md
├── CLAUDE.md
├── OBDA_AGENT_PLAYBOOK.md
├── README.md
├── requirements.txt
├── DDL/
│   └── ctc_data_ddl.sql
├── Onto/
│   └── cem.owl
├── mapping.yaml
├── mock_and_map.py
├── reasoning_server.py
├── reasoning_agent.py
└── .agents/
    └── skills/
        └── obda-query/
            ├── SKILL.md
            ├── references/
            └── scripts/
```

说明：

- `reasoning_server.py` 是当前推荐主路径
- `reasoning_agent.py` 是旧的实验性文件，包含外部 LLM 相关思路，不是当前推荐架构

## 相关文档

- 设计与路线图：
  [`.claude/plans/cozy-imagining-canyon.md`](.claude/plans/cozy-imagining-canyon.md)
- 查询 Skill：
  [`.agents/skills/obda-query/SKILL.md`](.agents/skills/obda-query/SKILL.md)
- 排障与修改规范：
  [`OBDA_AGENT_PLAYBOOK.md`](OBDA_AGENT_PLAYBOOK.md)

## 近中远期路线

### 近期

- 保持 Python 本地原型
- 使用 `mapping.yaml + DuckDB + owlrl`
- 使用少量内置 analyzer profile
- 让 Skill 走 schema-first 和受约束分析

### 中期

- 从 ontology 和推理后图中自动发现 analyzer profile
- 用 ontology annotation 对自动分类做少量校正
- 进一步减少手工 profile 维护成本

### 远期

根据目标选择路线：

- 如果优先标准 OBDA / 查询重写，评估 Ontop
- 如果优先更强规则推理，评估 Jena 或独立规则引擎

## License

MIT
