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
- `/health` 主要用于诊断，不是正常查询的固定前置步骤

## 推荐查询流程

对于自然语言问题，推荐始终按以下顺序：

1. 先查 `/schema`
2. 如果按手机号、ID、状态、评分等字段过滤，先确认属性 domain
3. 再写主 SPARQL
4. 只有当用户问“为什么”“路径是什么”“有哪些隐藏关系”时，才调用 `/analysis/...`
5. 只有当第一次结构化查询空结果或明显歧义时，才补一次定向 `/sample/{class_name}`

这也是 `obda-query` skill 当前要求遵守的协议。

一个重要约束：

- 如果问题同时包含“原因约束”和“动作/状态约束”，主查询必须同时编码这两个条件
- 例如 `因为网络问题，哪些客户投诉了？` 不能被偷偷放宽成“哪些客户有网络相关事件”

对 `causal_enumeration` 这类题，当前约束更严格：

- 正常路径是 `schema -> run`
- 不要把 `/health` 当成默认 preflight
- 不要在第一次 `run` 前先做泛化 `/sample`
- 如果第一次 `run` 返回 `empty_result` 或 `partial_success`，最多只做一次定向 grounding 修复，然后 rerun 一次

换句话说，`因为网络问题，哪些客户投诉了？` 这一类题的首轮不应该走成：

1. `schema`
2. `sample event`
3. `sample customer`
4. `run`

而应该直接走：

1. `schema`
2. `run`

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

### `run` 工作流

`run` 是当前推荐的多步执行入口，但要区分两种模式。

可执行模式：

```bash
bash .agents/skills/obda-query/scripts/obda_api.sh run --json '{
  "template": "causal_enumeration",
  "sparql": {
    "query": "PREFIX ex: <http://ywyinfo.com/example-owl#> SELECT ?customer ?event WHERE { ?customer a ex:customer ; ex:hasEvent ?event . } LIMIT 5"
  },
  "analysis": {
    "kind": "paths-batch",
    "payload": {
      "mode": "paths",
      "profile": "causal",
      "sources": ["http://ywyinfo.com/example-owl#customer_CUST002"],
      "max_depth": 3
    }
  }
}'
```

规划模式：

```bash
bash .agents/skills/obda-query/scripts/obda_api.sh run "因为网络问题，哪些客户投诉了？" --template causal_enumeration
```

仅调试 planner：

```bash
bash .agents/skills/obda-query/scripts/obda_api.sh run "因为网络问题，哪些客户投诉了？" --template causal_enumeration --plan-only
```

说明：

- 正常的 `QUESTION + --template` 会先取 `schema`，再把问句送入 `semantic query planner`，并自动执行锁定后的 `selected_plan`
- 如果 planner 给不出高置信计划，才返回 `planning_required`
- 只有显式传入 `--plan-only` 时，才返回 planner bundle，而不执行查询
- planner 现在会先做 query family routing，并可能把请求模板收敛到更小的 `effective_template`
- 对单锚点的状态/评分/属性判断题，即使外层误传成 `enumeration` / `causal_enumeration`，客户端也会优先收敛回 anchored lookup 路径；`如果有，有什么解决方案` 这类条件后缀不会把它自动变成结果集枚举
- 为兼容 Agent 误用，`run "..." --json --template ...` 也会退化为同样的 question-mode
- 同样地，如果有 Agent 误把 shorthand 包成 `run --json '{"template":"...","question":"..."}'`，客户端也会把它当成 question-mode 处理
- 如果有 Agent 误把 `question` 和手写的 `sparql / analysis / samples` 混在同一个标准模板计划里，客户端现在会忽略这些手写字段，强制改走锁定后的 question-mode
- 对自然语言 question-mode，不要再自行补写 `sparql.builder` 或 raw `sparql.query`；应直接使用客户端锁定后的 planner 计划
- question-mode 的 reroute、fallback、widening 都属于客户端内部实现；调用方不应依赖某个具体问法的特定策略
- 对当前 planner 还不支持的 anchored family，question-mode 会返回 `planning_required`，而不是假装已有稳定能力
- `schema` 命令现在默认只返回 `schema_summary`；只有显式传 `schema --full` 时才返回整份本体
- 执行模式下，`run` 默认只返回 `schema_summary` / `profiles_summary`，避免整份 schema 输出过大而被截断
- 如果你确实需要完整块，可在计划里显式传入 `include_schema: true` 或 `include_profiles: true`
- 执行模式下，`run` 默认只返回压缩后的 `analysis` 摘要；只有显式传入 `include_analysis: true` 时才返回完整原始 analyzer 路径
- 执行后的 question-mode 默认不再返回 `planner_summary` / `planner_attempts` / `execution_variant` 这类调试字段；需要调 planner 时优先使用 `--plan-only`
- 对 `causal_lookup` / `causal_enumeration`，现在优先建议使用 `sparql.builder` 而不是自由书写整条 `sparql.query`
- `sparql.builder` 会在执行前做 schema 约束校验，包括：
  - class / property 是否存在
  - link property 的 domain / range 方向是否匹配
  - 当 ontology schema 未显式声明某个对象关系、但 `mapping.yaml` 已将它映射到运行时图中时，builder 也会参考运行时映射关系做方向校验
  - `source_var` 是否可作为 analyzer 锚点列
- 对 `causal_lookup` / `causal_enumeration`，如果 builder 的 query shape 是明确的 `source_class -> evidence_class`，优先省略 `link_property`，让客户端自动推断
- 自动推断时，客户端会优先选择运行时可执行的 mapping 关系，其次才是 ontology schema 声明关系；若仍有多个候选，再要求显式指定
- 当 ontology schema 未完整声明某些 data property、但 `mapping.yaml` 已映射到运行时图中时，planner / builder / raw-query validator 现在也会参考运行时 data property catalog；例如 `customerbehavior_满意度评分`、`customerbehavior_服务体验评分` 这类字段
- 对 `causal_lookup` / `causal_enumeration`，如果你仍使用自由书写的 `sparql.query`，现在必须显式提供 `sparql.source_var`
- 如果主 SPARQL 已成功、但 analyzer 因缺少 URI 锚点无法继续，`run` 会返回 `status: partial_success` 和 `analysis_error`，而不是整条命令失败
- 对 `causal_lookup` / `causal_enumeration`，当前固定为“先查询，后分析”；主查询 `0` 行时不会继续 analyzer，而是返回 `status: empty_result`
- 对 `causal_lookup` / `causal_enumeration`，当主查询 `0` 行时，返回体还会附带 `recovery_hint`，把恢复路径收敛到“一次定向 grounding + 一次 rerun”
- 对 `causal_lookup` / `causal_enumeration`，`run` 还会额外返回 `presentation` 字段：这是结构化展示模型，不是最终中文话术
- `presentation` 优先提供人类可读字段与分组摘要；原始 `sparql` 保留用于审计，原始 `analysis` 默认会被压缩成摘要以避免重复路径占用过多 token
- 对 `causal_lookup` / `causal_enumeration`，`presentation` 现在还会返回 `answer_contract`，用于约束最终答案的段落顺序和表格/明细结构，减少大模型自由发挥带来的输出漂移
- 从 Agent/Skill 边界看，`run` 应被当成“薄调用入口”。具体的 semantic routing、grounding、plan construction、lowering 和 bounded fallback 属于 client/planner 内部职责，而不是 skill 文案层要复刻的逻辑

一个现实约束：

- 当前 `run "自然语言问题" --template ...` 已经是 question-mode 自动执行入口；只有显式 `--plan-only` 时才只看 planner bundle
- planner 的目标仍然是“高置信就执行，低置信就停”，而不是自动退化成开放探索链

### 问句回归套件

仓库现在包含一套 repo 级的自然语言问句回归：

- 套件文件：
  [`tests/obda_question_regressions.json`](tests/obda_question_regressions.json)
- 主执行器：
  [`tests/run_question_regressions.sh`](tests/run_question_regressions.sh)
- 断言/辅助工具：
  [`tests/run_question_regressions.py`](tests/run_question_regressions.py)

用途：

- 固化当前已支持的主路径
- 固化当前故意 `fail closed` 的路径
- 避免每次改 planner / grounding / lowering 后把旧行为打坏

运行方式：

```bash
bash tests/run_question_regressions.sh
```

只跑单个 case：

```bash
bash tests/run_question_regressions.sh causal_enumeration_network_complaints_run
```

当前套件覆盖了几类代表性问句：

- `因为网络问题，哪些客户投诉了？`
- `13800138004是否存在满意度评分低于3的情况`
- `13800138004是否存在满意度评分低于3的情况?如果有，有什么解决方案`
- `13800138004是否存在满意度评分低于3的情况?如果有，原因是什么？如果有，有什么解决方案`
- `13800138004是否存在满意度评分低于3的情况?如果有，这个客户有什么解决方案`
- `13800138004是否存在满意度评分低于3的情况?如果有，13800138002有什么解决方案`
- `13800138003是否存在满意度评分低于3的情况?如果没有，这个客户有什么解决方案`
- `13800138004是否存在低满意度情况?如果有，有什么解决方案`
- `因为网络问题，哪些客户投诉了？这些客户的投诉原因分别是什么？`
- `引发投诉的都有什么原因`

说明：

- 套件现在同时覆盖：
  - 单问题 `question-template`
  - 多子问题 `question-batch-template`
  - 解释型枚举的 generic reroute
    - `causal_enumeration -> explanation_enumeration -> enumeration`
  - planner 在缺少显式 `Intent IR` 时会先合成单 unit `Intent IR`
    - `routing` / `intent policy` 优先消费 `Intent IR`
    - raw slot 仅作为 bootstrap fallback
  - `build_question_unit_intent_ir` / `derive_intent_profile` / `route_query_family`
    现在开始共用一份 canonical `intent policy`
    - 统一决定 `focus / operators / references / output`
    - 以及 `inheritance` 与 `family bias`
    - routing 相关判定现在优先读取 canonical `constraints` 快照
      而不是直接依赖 bootstrap top-level semantic text
  - `build_semantic_request_ir` 现在开始消费显式 `grounded constraint view`
    - 会同时保留 `requested_text` 与 binding 后的 `effective_text / binding_terms / top_candidate`
    - 让 `request_ir`、candidate term 生成、underconstrained 判定
      不再只靠 raw semantic text 拼装
  - `build_family_slot_inputs` 也开始优先读取 canonical `constraints` 快照
    - lowering 前的 slot text 不再直接依赖 bootstrap top-level semantic text
  - operator 控制位现在开始走 `bootstrap_operator_hints`
    - `bootstrap_signals` 仅作为兼容输出保留，不再是语义决策主输入
  - `extract_question_slots` 里的词法逻辑已开始显式下沉到 `collect_lexical_bootstrap_recall`
    - 词法层保留用于 recall / normalization
    - `Intent policy / grounding / routing / lowering` 不再与散乱 regex 分支直接耦合
    - lexical recall 现在按有序 rule pipeline 执行，后续增删规则不需要再改一个大函数
    - 词法资源与 recall 规则现已独立到 `obda_lexical.py`
    - `obda_api.py` 不再声明或直接持有这些词表/regex，只通过 adapter 函数取词法结果
  - 条件性 follow-up 的 `Execution DAG` 跳过逻辑
  - 显式新锚点覆盖旧上下文
    - 只保留依赖条件，不继承上一题的语义约束
  - 负条件分支
    - `empty_or_false` 只继承焦点，不继承前一步成立时才有意义的状态约束
  - 执行期 `resolved references`
    - 单实体引用：`这个客户`
    - 结果集引用：`这些客户` / `分别`
- 新的 batch 主线要求：
  - `状态判断 -> 如果有 -> 解决方案`
  - 必须拆成 `QuestionUnit[]`
  - 并保持 `planner_suggested / batch_executed / planning_required / skipped` 这些状态稳定

- 默认 runner 只执行当前已验证稳定的 case
- `causal_enumeration_network_complaints_run` 这条自动执行 case 仍保留在 suite 里，但暂时作为手动扩展 case，不纳入默认基线

要求：

- 每次修改 `obda_api.py` 中的 planner、slot binding、lowering、question-mode 执行器后，都要先跑这套回归
- 如果某个 case 的“应答成功 / 应答失败”语义边界被设计性调整，必须同步更新套件和 README，而不是只改代码

### `presentation` 的定位

`presentation` 不是固定中文 formatter。

它的职责是：

- 对原始 `sparql` / `analysis` 结果做分组、去重、计数和路径压缩
- 优先输出适合人类阅读的 `display` 信息
- 保留 `refs` 用于追踪，但不把 ID 当成默认主展示内容

当前第一阶段只支持：

- `causal_enumeration`
- `causal_lookup`

建议：

- Claude 组织最终回答时，优先读 `presentation`
- 只有在需要调试、审计或深挖路径时，再回头读原始 `analysis.paths`

### `sparql.builder` 示例

对于因果模板，推荐使用结构化 builder：

```bash
bash .agents/skills/obda-query/scripts/obda_api.sh run --json '{
  "template": "causal_enumeration",
  "sparql": {
    "source_var": "customer",
    "builder": {
      "source_class": "customer",
      "source_var": "customer",
      "evidence_class": "event",
      "evidence_var": "event",
      "link_property": "hasEvent",
      "select": [
        {"var": "customer", "kind": "uri"},
        {"var": "customerName", "subject": "source", "property": "customer_姓名"},
        {"var": "customerId", "subject": "source", "property": "customer_客户ID"},
        {"var": "event", "kind": "uri"},
        {"var": "eventId", "subject": "evidence", "property": "event_event_id"},
        {"var": "eventType", "subject": "evidence", "property": "event_事件类型"},
        {"var": "eventDesc", "subject": "evidence", "property": "event_事件描述"}
      ],
      "filters": [
        {"var": "eventType", "op": "contains_any", "values": ["网络", "宽带"]},
        {"var": "eventDesc", "op": "contains_any", "values": ["网络", "信号", "宽带"]}
      ],
      "distinct": true,
      "order_by": ["customerId", "eventId"]
    }
  },
  "analysis": {
    "kind": "paths-batch",
    "payload": {"mode": "paths", "profile": "causal", "max_depth": 3}
  }
}'
```

当前 builder 第一阶段支持的能力：

- `source_class` / `evidence_class`
- `link_property` 自动方向校验
  - 优先参考 `/schema` 的 object property 声明
  - 如果 schema 未声明、但 `mapping.yaml` 已映射为运行时关系，也会继续校验
  - 如果未显式提供 `link_property`，客户端会优先选择运行时映射中的 `source -> evidence` 正向关系
- `select`
- `filters`
  - `contains`
  - `contains_any`
  - `contains_all`
  - `equals`
  - `in`
- `distinct`
- `order_by`
- `limit`

### 当前推荐的最短线路

普通枚举或因果枚举题，优先收敛到：

1. `schema`
2. `run --json`

不要默认走成：

1. `schema`
2. `health`
3. 泛化 `sample`
4. 多次试探 `sparql`
5. 多次单实体 `causal`

这种“先摸索再执行”的路径通常才是分钟级慢查询的主要来源。
- 如果直接把自然语言问题丢给 Claude Code，再让它现场补全查询与 analyzer payload，仍可能出现多轮试探

当前最常见的剩余偏差是：

- 已经知道问题属于 `causal_enumeration`
- 但仍然在第一次 `run` 之前先看 `sample event` / `sample customer`

这一步通常不是必须的，而且会把一次正常查询重新拖回“先探索再执行”的老路线。

因此，想要最稳定行为时，应优先把执行计划显式化，而不是只给一个自然语言问题。

### 最稳定执行方式

如果目标是减少试错和 round-trip，推荐固定成下面的顺序：

1. `schema`
2. 一条主 SPARQL，返回最终答案所需字段，并至少带一列实体 URI
3. 如需路径证据，再调用一次 `analysis-paths` 或 `analysis-paths-batch`
4. 最终回答时明确区分“事实结果”和“路径解释”

对于 `causal_lookup` / `causal_enumeration`，当前实现已经固定为：

- 先查询
- 后分析
- 主查询没有命中时，不再继续跑 analyzer
- 枚举类问题优先一次 `analysis-paths-batch`，而不是逐条 `/causal/{id}`

换句话说：

- 自然语言 + planning mode，适合交互探索
- 显式 `run --json`，适合稳定执行
- 对于高频问题，最好沉淀为固定模板或 repo 级 profile，而不是每次让 Agent 临场生成

### Analyzer 正确调用方式

推荐：

```bash
bash .agents/skills/obda-query/scripts/obda_api.sh analysis-paths \
  --json '{"mode":"paths","profile":"causal","source":"http://ywyinfo.com/example-owl#customer_CUST002","max_depth":3}'
```

批量路径：

```bash
bash .agents/skills/obda-query/scripts/obda_api.sh analysis-paths-batch \
  --json '{"mode":"paths","profile":"causal","sources":["http://ywyinfo.com/example-owl#customer_CUST002","http://ywyinfo.com/example-owl#customer_CUST006"],"max_depth":3}'
```

不要这样做：

- 手写 `curl ".../analysis/paths?from=...&to=..."`
- 把类名当成 `source`
- 用非客户 ID 调 `/causal/{customer_id}`

当前约束：

- `/analysis/paths` 的 GET 便利接口需要具体实体 URI，适合少量调试
- 常规场景应优先使用 `POST /analysis/...` 的 JSON payload
- `/causal/{customer_id}` 是兼容入口，只适用于 customer

### 当前 CLI 兼容层

为减少 Agent 的机械性试错，client 现在兼容一些常见误用：

- `sample event 2`
- `sparql 'PREFIX ... SELECT ...'`
- `run "问题" --template ...`

但推荐写法仍然是显式参数：

- `sample event --limit 2`
- `sparql --query 'PREFIX ...'`
- `run --json '{...}'`

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

## 常见排障

### 1. 为什么一个问题会跑几分钟

通常不是推理机慢，而是 Agent 在反复试错：

- 先跑错误的 `run` 形式
- 再用错误命名空间或错误谓词写 SPARQL
- 再用 `/sample` 回头补 grounding
- 再逐个实体调用 analyzer

如果一个问题超过 3 到 4 个 server round-trip，通常已经偏离了推荐协议。

### 2. `run` 返回 `planning_required`

这说明 question-mode 已经进入 planner，但当前 planner 还没有产出高置信、可执行的锁定计划。

处理方式：

- 先看返回的 `request_ir` / planner bundle，确认 query family、anchors 和约束是否被正确识别
- 仅当返回里明确带有 `recovery_hint` 时，才允许做一次有界 grounding 恢复，再重新调用 `run`
- 如果没有 `recovery_hint`，就应 fail closed：停止、报告 planner 当前不支持或需澄清；不要切到手工 `sparql/sample` 探索链
- 如果 `clarification_hint.kind = explicit_metric_or_threshold_required`，不要自行把抽象状态词改写成你猜测的显式阈值问句，例如把“低满意度”擅自改写成“满意度评分低于3分”
- 这种显式 metric/threshold 只能来自用户原问题，或来自 planner 已经成功抽取出的 numeric constraint；否则就应该停下并请求更明确的表述
- 如果同时返回 `next_action = ask_user_for_clarification`，就按该契约执行：直接向用户发澄清问题，并在当前轮停止调查，不要再切到 `/sample`、手写 `sparql`、`schema --full | grep ...`，也不要从 schema 里替用户挑选候选指标
- 如果 question-mode 执行后返回 `status = empty_result` 且带有 `recovery_hint`，这也只是“一次有界恢复”契约：只能围绕同一个问题、同一个指标、同一个阈值、同一个锚点做一次定向 grounding，然后把同一问题 rerun 一次
- 不要把这种 `empty_result + recovery_hint` 当成“去 schema 里找别的相似指标再试一轮”的许可；如果 rerun 之后仍为空，就应报告无匹配并停止
- 对 `13800138004的满意度评分是多少` 这类锚点 + 显式属性查值问题，question-mode 现在可直接走 `fact_lookup -> anchored_fact_lookup`，不需要外层 agent 再手工补一条事实查询
- 对 `13800138004是否存在满意度评分低于3的情况？如果有，有什么解决方案？` 这类数值状态 + 条件性目标追问，成功的 `causal_lookup` 结果现在会直接在 `presentation.target_details` 和 `presentation.related_terminal_details` 里给出终点实体、事件、工单等详情；只有当 parser 明确产出 `remediation` 意图时，结果才会额外填充 `presentation.solution_details`。不要再补 `/sample remediationstrategy` 或手写后续查询
- 不要把 `planning_required` 当成“让 Agent 现场补完整个 `sparql.query` / `analysis.payload`”的信号

这不是报错，而是在明确告诉你：

- 当前还没有进入实际主查询执行阶段
- 也还没有进入实际 analyzer 阶段
- 当前这个 family 还没有被 planner 通用支持，或者 grounding 置信度不足
- 正确处理方式是 fail closed 或一次受限恢复，而不是退化成自由探索链

### 3. Analyzer 报 `sources` 或 `source` 缺失

这通常意味着：

- 你调用了 `paths-batch` 却没有提供 `sources`
- 或主 SPARQL 没有返回可用的 URI 锚点列

当前 client 会尽量从 SPARQL 返回结果中自动提取 URI 列作为 `source/sources`，但前提是结果里确实存在 URI 变量。

因此推荐：

- `causal_lookup` 主查询至少返回一个实体 URI
- `causal_enumeration` 主查询至少返回一列实体 URI，供 batch analyzer 自动提取

### 4. 为什么结果会从“2 个客户”变成“3 个客户”

通常是查询口径漂移，不是数据随机变化。

常见漂移：

- `网络投诉` 被放宽成 `网络相关事件`
- `客户数` 和 `事件行数` 混用

回答时应明确区分：

- 匹配事件数
- 去重后的实体数

### 5. 什么时候该用 `/sample`

`/sample` 只用于 grounding：

- 确认实际 object property 是否真的被映射
- 确认某个字段落在哪个类上
- 确认 local name 是否与 schema 一致

不要用 `/sample`：

- 枚举最终结果集
- 做统计
- 推断“全量都有哪些”

### 6. 为什么明明有 `run`，Claude Code 还是会反复试错

因为目前系统里最不确定的一步，仍然是：

- 从自然语言问题推导出正确 SPARQL
- 决定 analyzer 用哪一种 payload
- 决定 `source/sources` 该从哪一列 URI 提取

这些决策如果仍由 Agent 在运行时临场补，就无法保证一次成功。

当前项目已经做的缓解包括：

- 兼容常见 CLI 误用
- 在 batch analyzer 缺少 `sources` 时尽量自动从 SPARQL 结果提取 URI
- 用 Skill 和 repo 规则约束查询顺序

但这仍然不等于“确定性查询编译器”。

如果后续要进一步压缩时延和试错次数，方向应该是：

- 把高频问法沉淀为固定执行模板
- 或实现真正的自然语言到执行计划的确定性 compiler

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
