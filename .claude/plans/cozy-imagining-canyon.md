# 设计重构：将 OBDA、OWL 推理、业务规则、路径解释彻底分层

## 1. 设计结论

当前设计文档需要整体修正。

核心原因不是“代码写得不够多”，而是几个不同层次的能力被混写成了一个“推理机”：

- OBDA / 映射
- RDF 图查询
- OWL 公理推理
- 业务规则推理
- 路径分析与解释

这几类能力的边界不同，适合的组件也不同。如果继续混写，后续每增加一个需求，就会继续往 `reasoning_server.py` 里堆硬编码，最终既不像 Ontop，也不像 Jena，也不像纯推理机。

本次重构后的结论如下：

1. `reasoning_server.py` 的核心职责应当只包括：
   - 从关系数据生成 RDF 事实
   - 加载 ontology
   - 执行 OWL 级闭包推理
   - 提供 SPARQL 查询能力

2. `/causal/{customer_id}` 这类“客户起点的业务路径解释”不应被称为本体推理机核心能力。

3. 若希望真正利用本体弹性，必须把“关系语义”和“可推理公理”写入 ontology，而不是写死在 server 中。

4. 若目标是标准意义上的 OBDA，应优先考虑 Ontop 方向。

5. 若目标是更强的规则推理与语义推理，应优先考虑 Jena 方向。

6. 当前 `RDFLib + owlrl + morph-kgc` 更适合：
   - 本地原型
   - 小图谱
   - 以显式图 + 轻量 OWL RL 推理为主的方案

它不是 Ontop 的替代，也不是 Jena 的完整替代。

---

## 2. 先把概念分清楚

### 2.1 OBDA 是什么

OBDA 的核心不是“写出很多 RDF 再查”，而是：

- 将关系源映射为语义层
- 让用户用 ontology 词汇查询
- 将语义查询重写为对底层数据源的访问

因此，标准 OBDA 更接近：

```text
SPARQL
  -> 查询重写
  -> SQL / relational access
  -> 返回语义结果
```

Ontop 是这条路线的典型实现。

### 2.2 OWL 推理是什么

OWL 推理不是“沿着我想要的几条边往下找”，而是：

- 基于 ontology 中正式声明的公理
- 自动推出新三元组或新的类归属

例如：

- `owl:inverseOf`
- `owl:propertyChainAxiom`
- `owl:TransitiveProperty`
- `rdfs:subClassOf`
- `rdfs:subPropertyOf`

如果这些公理没有写在 ontology 里，就不应该期待 reasoner 自动推出你脑中想要的“隐藏关系”。

### 2.3 业务规则是什么

例如：

- 满意度评分 `< 3.0` 视为低满意度
- 近 3 个月投诉次数 > 2 视为服务风险
- 同时命中若干指标则归为“网络体验问题客户”

这些通常不是 OWL RL 的强项，更适合：

- SPARQL 查询 / CONSTRUCT
- SHACL Rules
- SWRL
- 应用层规则引擎

### 2.4 路径解释是什么

例如：

```text
客户 -> 事件 -> 感知 -> 策略
```

这是一种图上的路径分析 / explanation，不等价于 OWL 推理。

它可以基于：

- 显式图
- 推理后的图

但它本身通常是“分析器”功能，而不是 reasoner 核心。

---

## 3. 对当前实现的真实评估

### 3.1 当前组件栈

当前项目实际使用：

- `morph-kgc`: 关系到 RDF 的映射与物化
- `RDFLib`: RDF 图与 SPARQL 执行
- `owlrl`: OWL 2 RL / RDFS 前向闭包推理
- `FastAPI`: 对外提供查询接口

### 3.2 当前做对了的部分

当前实现已经具备以下正确能力：

1. 从 DuckDB 物化 RDF 图
2. 加载 ontology
3. 对图执行 `owlrl` 推理
4. 提供 `/sparql` 与 `/schema`

这部分是一个合理的本地原型。

### 3.3 当前做错位的部分

当前 `/causal/{customer_id}` 的实现，本质上是：

- 把起点写死成 `customer`
- 把允许 traversing 的属性写死成 `hasEvent / hasPerception / suggestsStrategy / hasRemediationStrategy`
- 用 DFS 枚举路径

这属于“业务路径分析器”，不属于“本体推理机核心”。

因此当前系统是：

```text
RDF 物化
 + OWL RL 闭包
 + 业务路径遍历
```

而不是：

```text
标准 OBDA 推理平台
```

### 3.4 当前 ontology 的关键缺口

目前 ontology 中虽然定义了很多对象属性和 middle object 语义，但缺少你真正想依赖的推理公理，例如：

- `owl:inverseOf`
- `owl:propertyChainAxiom`
- `owl:TransitiveProperty`
- 明确的推理导向 super-property

这意味着：

```text
customer --event--> event
event --perception--> perception
```

并不会自动推出：

```text
customer --hasPerception--> perception
```

除非 ontology 中明确写出相应公理。

### 3.5 当前技术栈的能力边界

#### `RDFLib + owlrl`

能做：

- 小规模图谱上的 RDF 物化与 SPARQL 查询
- OWL 2 RL 范围内的闭包推理
- 快速本地原型

不能替代：

- Ontop 的 SPARQL-to-SQL 重写型 OBDA
- Jena 的更丰富规则引擎与推理生态

#### `Ontop`

强项：

- 标准 OBDA
- 虚拟知识图谱
- SPARQL 查询重写到底层关系库
- 轻量 ontology 约束下的语义查询

弱项：

- 不是以复杂规则推理为核心
- 不适合把大量业务路径解释逻辑写进去

#### `Jena`

强项：

- 语义 Web 框架更完整
- 推理和规则能力更强
- 更适合 rule-based 推理路线

---

## 4. Claude 执行侧补充约束

下面这部分不是 ontology 设计本身，而是为了让 Claude Code 在当前仓库里少走弯路。

### 4.1 当前真实结论

当前慢查询的主要原因不是：

- `owlrl` 太慢
- `RDFLib` 太慢
- DuckDB 太慢

而是 Claude 在运行时临场试探：

- 先跑错误形态的 `run`
- 再多次空 `sparql`
- 再回退到 `/sample`
- 再单个实体逐个调用 analyzer

因此当前瓶颈首先是“执行编排不确定”，而不是“推理引擎性能不足”。

### 4.2 Claude 当前应该遵守的执行顺序

对于普通自然语言问答，目标应尽量收敛为 3 次 server round-trip：

1. `schema`
2. 一条主 `sparql`
3. 如果问题包含因果/路径语义，再补一次 `analysis`

不要让 Claude 走成：

- `schema`
- 多次试探式 `sparql`
- 多次 `/sample`
- 多次单实体 `/causal`

这类流程会把一个简单问题拖到分钟级。

### 4.3 当前先落地的固定线路

当前先不做 analyzer-first。

先保证下面这条线路稳定：

```text
schema
  -> main sparql
  -> analysis
```

在 Claude 执行层，这应进一步收敛成：

```text
schema
  -> run
```

其中：

- `run` 负责先执行主查询
- 只有主查询命中后，才继续 analyzer
- 主查询 `0` 行时，直接返回 `empty_result`
- analyzer 缺锚点但主查询成功时，返回 `partial_success`

因此当前的默认纪律应当是：

- 不把 `/health` 当成常规 preflight
- 不在第一次 `run` 前先做泛化 `/sample`
- 只在 `empty_result` 或 `partial_success` 后允许一次定向 grounding 修复
- 不允许回到无上限的 sample / grep / sparql 试探循环

当前最常见的残留问题是：

```text
schema
  -> sample event
  -> sample customer
  -> run
```

这比旧流程已经好很多，但仍然不是目标线路。

目标线路仍然应该是：

```text
schema
  -> run
```

也就是说，`sample` 应当是失败恢复工具，而不是 `causal_enumeration` 的首轮习惯动作。

### 4.4 为什么不是先 analysis 再 query

原因不是“理论上不全面”，而是当前 analyzer 的职责不是全图候选发现，而是锚点路径解释。

也就是说，当前 analyzer 更接近：

- explainer
- constrained path tracer

而不是：

- global cause discovery engine

所以对这类问题：

```text
因为 X，哪些实体 Y 了？
```

当前更合理的执行方式是：

1. 先用主查询同时编码 `X` 和 `Y`
2. 得到候选集合
3. 再用 analyzer 解释这些候选为什么连上

如果反过来先做 analysis，会立刻遇到几个问题：

- analyzer 需要 `source` 或 `sources`
- 没有锚点时只能做高噪音全图探索
- “动作/状态约束”通常更适合在 SPARQL 里表达，而不是在路径遍历里表达
- 成本更高，且更容易把“网络相关事件”误当成“网络投诉”

### 4.5 未来能力：analysis-first / candidate discovery

以后如果真的要支持“先分析，再找候选”，那不应该继续复用当前 `paths` 契约，而应该单独设计 discovery 能力，例如：

- `/analysis/discover`
- `/analysis/find-candidates`
- 基于 profile 的候选发现器

这类能力的输入将更像：

- 目标类型
- 约束 profile
- 原因约束
- 动作约束

而不是当前 `paths` 所要求的：

- `source`
- `sources`

所以结论是：

- 当前阶段，固定走 query-first-then-analysis
- 下一阶段，再单独设计 analyzer-first / discovery

并且需要明确：

- 当前 analyzer 是“解释器”，不是“候选发现器”
- 因此 `causal_lookup` / `causal_enumeration` 的当前线路必须是“先查询，后分析”
- 若主查询没有候选结果，Analyzer 不应继续执行

### 4.3 `run` 的定位

当前 `run` 不是“自然语言直接执行器”，而是两种模式：

1. 规划模式
   - `run "问题" --template ...`
   - 返回 planning bundle
   - 不保证自动生成正确 SPARQL

2. 执行模式
   - `run --json` / `run --json-file`
   - 执行明确的计划

因此：

- 想要探索时，可以先用规划模式
- 想要稳定时，应优先用执行模式
- 高频问法不应该每次都靠 Claude 临场补 query

### 4.3.1 当前方案的弹性与短板

当前方案的优点是：

- 不把 CEM 的具体业务链路写死到通用 Skill
- 保留 query-first-then-analysis 的通用协议
- 允许不同本体在同一套 Skill 协议下工作

因此从“跨场景复用”角度看，它比把 `customer -> event -> perception` 之类路径硬编码进 Skill 更有弹性。

但当前短板也很明确：

- 首轮失败最常见的原因不是 analyzer，而是主 SPARQL 写错或写窄
- 一旦主查询 `empty_result`，Claude 仍然容易进入恢复性探索
- 这说明当前问题主要是“查询生成过于自由”，而不是“先查询后分析的方向错了”

所以当前真正的 tradeoff 是：

- 保持 Skill 通用，会降低首轮命中率
- 写死 repo 级业务路径，会提高命中率，但会污染通用 Skill

当前决策是：

- 保留通用 Skill
- 不把 CEM 特化语义回灌到 Skill
- 通过“受约束查询生成”而不是“业务硬编码”来提高首轮命中率

### 4.3.2 当前首轮失败的真实根因

到目前为止，`causal_enumeration` 的首轮失败主要不是因为：

- server 不可用
- analyzer 不可用
- graph 中没有数据

而是因为 Claude 在首轮会自由拼整条 SPARQL，最常见的失败模式包括：

- object property 方向写错
- 过滤条件写得过严
- 谓词名虽然存在，但作用在错误的实体上
- 同时包含“原因约束 + 动作约束”时，只编码了其中一部分，或把两者编码得过窄

因此：

- 当前主失败点在 query shape
- analyzer 只是第二阶段
- `sample` 只能作为有限恢复工具，不能作为默认补救路径

### 4.4 当前最该优化的方向

短期：

- 用 schema-first + 单主查询 + 单 batch analysis 收敛流程
- 尽量避免通过 `/sample` 重新发现 schema 已经给出的关系
- 避免把 `analysis-paths` 和 `analysis-paths-batch` 混用
- 把 SPARQL 生成从“自由文本生成”收紧到“受约束生成”

中期：

- 为高频问法沉淀 repo 级模板
- 让主查询固定返回实体 URI 锚点列，供 batch analyzer 自动使用
- 增加主查询 shape validator，在执行前就检查方向、谓词存在性、URI 锚点列

长期：

- 做真正的自然语言到执行计划的确定性 compiler
- 减少 Claude 在运行时自己“猜 SPARQL / 猜 analyzer payload / 猜 source 列”

### 4.4.1 下一步明确实现项：受约束 Query Compiler / Validator

下一阶段不再优先补提示词，而是实现一个更硬的查询生成层。

目标不是做业务写死模板，而是做“结构受约束、语义仍通用”的 compiler。

建议输入槽位至少包括：

- target entity class
- anchor entity class
- cause constraint
- action/state constraint
- required return columns
- preferred URI anchor column

compiler 生成主查询后，先做本地校验，再决定是否真正执行。

至少需要校验：

- 谓词是否存在于 `/schema`
- object property 方向是否匹配 domain / range
- 主查询是否同时编码了 cause constraint 和 action/state constraint
- `causal_enumeration` 是否返回至少一列 URI 锚点
- 返回列是否足够支撑最终 answer 和 batch analysis

当前明确不做的事：

- 不把 CEM 专属业务词汇固化到通用 Skill
- 不把 analyzer 改成首轮候选发现器
- 不让 `empty_result` 后自动进入无约束 sample/grep/sparql 试探链

### 4.4.2 恢复线路也需要结构化

当首轮 `run` 返回 `empty_result` 或 `partial_success` 时，恢复步骤也应受约束，而不是开放探索。

目标恢复线路应当是：

```text
schema
  -> run
  -> one targeted sample
  -> rerun
```

而不是：

```text
schema
  -> run
  -> sample customer
  -> sample event
  -> sample perception
  -> raw sparql
  -> single analysis
```

恢复阶段至少应满足：

- 只允许一个最相关类做 grounding
- grounding 完成后必须回到 `run`
- 不允许掉回手写 `sparql` 主导流程
- 不允许用单个 `analysis-paths` 结果泛化到整个枚举结果集

### 4.4.3 最终话术不应直接消费原始路径

当前还有一个独立问题：

- 即使主查询和 analyzer 都执行正确
- 最终回答仍然可能把路径直接说成：
  - `customer_CUST002 --hasEvent--> event_EVT001`
  - `perception_PER002`
  - `remediationstrategy_STR001`

这对人类阅读并不友好。

这说明当前的痛点不是“缺少答案”，而是“缺少展示层模型”。

### 4.4.4 不要把最终文案硬编码进 Python

当前明确不建议这样做：

- 在 `obda_api.py` 里直接写死最终中文话术模板
- 让 Python 直接负责完整自然语言回答

原因是：

- 这样会压掉 Claude 的表达能力
- 也会让不同问答类型的文案风格过早固化
- 还会把 repo 当前的一套话术习惯误当成通用协议

因此更合理的分工是三层：

1. 原始证据层
   - `/sparql`
   - `/analysis`

2. 展示模型层
   - Python 生成结构化 `presentation`
   - 做确定性的分组、去重、计数、label 解析、路径压缩

3. 自然语言层
   - Claude 基于 `presentation` 组织最终回答

也就是说：

- Python 负责“压缩和整理”
- Claude 负责“表达和措辞”

### 4.4.5 展示层的目标

展示层不是把原始路径再包一层 JSON。

它的目标是：

- 给 Claude 一个更干净、更人类友好的中间结构
- 让 Claude 不必直接从原始 `paths` 数组里自己猜重点
- 保留机器可追踪锚点，但不把它们当成主展示内容

因此展示层必须区分两类信息：

- `display`
  - 面向人类可读
- `refs`
  - 面向程序追踪和调试

默认话术应优先消费 `display`，而不是 `refs`。

### 4.4.6 可读化优先级

展示层在生成可读字段时，应使用如下优先级：

1. 业务字段
   - 如姓名、事件类型、事件描述
2. `rdfs:label`
3. 类标签 + 计数摘要
   - 如“1 个感知分析节点”“3 个修复策略”
4. ID / local name
   - 仅作为 `refs`
   - 不作为默认主话术

因此：

- `EVT001` 不应作为主展示内容
- `PER002` / `STR001` 如果没有稳定可读 label，不应直接成为主文本
- 没有可读标签时，应退化成“感知分析节点数 / 修复策略数”这类摘要

### 4.4.7 按 template 区分展示模型，而不是按业务场景区分

展示层需要区分问答类型，但不应绑定 CEM 场景。

推荐按 `template` 建模：

- `causal_enumeration`
  - 按实体分组
  - 展示事件证据
  - 展示路径亮点摘要
- `causal_lookup`
  - 展示单实体为什么成立
  - 展示关键路径亮点
- `enumeration`
  - 展示结果集摘要、去重计数、代表字段
- `fact_lookup`
  - 展示对象卡片式摘要
- `hidden_relation`
  - 展示新增/隐含关系摘要

第一阶段只需要实现：

- `causal_enumeration`
- `causal_lookup`

### 4.4.8 `presentation` 的建议契约

第一版不输出最终中文文案，而是输出结构化的 `presentation` 字段。

对 `causal_enumeration`，建议至少包含：

- `template`
- `summary`
  - `entity_count`
  - `record_count`
- `groups`
  - `entity`
    - `display_name`
    - `display_id`
    - `type_label`
  - `evidence`
    - `display_label`
    - `display_description`
    - `refs`
  - `reasoning_summary`
    - `direct_event_count`
    - `mediator_summary`
    - `outcome_summary`
  - `trace_refs`

其中：

- `display_*` 用于 Claude 直接组织答案
- `trace_refs` 用于需要时追踪和引用

### 4.4.9 当前下一步的边界

展示层的下一步不是：

- 做固定中文 formatter
- 做 repo 特化话术模板

而是：

- 先定义 `presentation` 契约
- 让 `run` 在保留原始 JSON 的同时返回 `presentation`
- 再由 Claude 基于 `presentation` 组织最终自然语言

### 4.5 `.claude` 与 `.agents` 的关系

当前 Claude 测试时应理解为：

- `.claude/skills/obda-query/SKILL.md` 是 Claude 侧说明
- 实际调用的客户端脚本仍然是 `.agents/skills/obda-query/scripts/obda_api.sh`
- 因此 client 逻辑修复不需要复制一份到 `.claude/scripts`

换句话说：

- skill 文档要同步
- client 代码只维护一份

弱项：

- 对本地 Python 生态不如当前栈直接
- 成本和系统复杂度更高

---

## 4. 新设计原则

后续设计必须遵守以下原则。

### 原则 1：词汇只能由 ontology 定义

不能先在应用里发明关系，再假装这是 ontology 推理。

正确方式：

1. 在 ontology 中声明类与属性
2. 在 mapping 中按 ontology 词汇产出事实
3. 在 reasoner 中基于 ontology 推理
4. 在 agent 中基于 ontology 词汇生成 SPARQL

### 原则 2：mapping 负责“事实表达”，不负责“想象关系”

mapping 可以做的事：

- 把 link table 映射为 middle object
- 把主实体间的显式关系映射出来
- 复用 ontology 中已声明的属性名

mapping 不应做的事：

- 伪造 ontology 中没有定义的长期核心关系
- 通过瞎猜列名补齐关系

### 原则 3：reasoner 只根据公理推理

如果一个隐藏关系要自动出现，必须能回答：

“它是由 ontology 中哪条公理推出的？”

如果答不上来，就不是推理，而是应用逻辑。

### 原则 4：业务规则和本体推理分开

例如：

- 评分阈值判定
- 风险分层
- 组合指标判断

这些应单独放在：

- SPARQL 层
- SHACL/SWRL 层
- 规则服务层

不要硬塞进 OWL RL 期待自动发生。

### 原则 5：路径解释器是可选分析组件，不是核心推理机

如果需要“客户为什么会得到某个策略”的可解释结果，可以保留一个 analyzer，但必须明确它是：

- 对推理后图的分析器
- 不是 ontology reasoner 本体

---

## 5. 目标架构：四层分离

推荐的新架构如下：

```text
用户问题
  -> Agent
  -> SPARQL / 规则查询
  -> Reasoning Service
  -> 结果解释
```

细化为四层：

### 5.1 数据层

- DuckDB
- 原始业务数据
- link tables

### 5.2 语义映射层

- ontology 词汇由 `Onto/cem.owl` 定义
- `mapping.yaml` 负责把 DuckDB 行映射为 RDF 事实
- middle object 如有必要保留

职责：

- 只输出显式事实
- 不写业务推理代码

### 5.3 推理与查询层

- 加载 RDF
- 加载 ontology
- 做 `owlrl` 或其他 reasoner 推理
- 提供 `/sparql`、`/schema`、`/sample`、`/reload`

职责：

- 做语义推理
- 不写客户特定 DFS 逻辑

### 5.4 分析与解释层

可选组件：

- generic graph analyzer
- path analyzer
- explanation builder
- business classifier

职责：

- 对推理后图做解释
- 生成用户可读路径
- 面向任意实体而不是只面向 `customer`
- 接受外部传入的关系模式、深度限制、起点类型
- 不是 reasoner 核心

---

## 6. 技术路线选择

下面给出三个可行路线。

### 路线 A：继续当前 Python 栈，定位为“小规模原型”

组件：

- DuckDB
- morph-kgc
- RDFLib
- owlrl
- FastAPI

适用场景：

- 小图谱
- 本地验证
- ontology 与 mapping 仍在快速演进
- 需要和 Python 工作流高度耦合

必须接受的限制：

- 不是标准的 Ontop 式虚拟 OBDA
- 不是 Jena 式强规则平台
- 大规模图谱能力有限

建议定位：

```text
语义原型平台 / 本地 reasoning prototype
```

而不是：

```text
完整 OBDA 推理平台
```

### 路线 B：OBDA 优先，转 Ontop

如果你的核心目标是：

- 用 ontology 词汇查关系库
- 尽量不先物化全量 RDF
- 尽量遵循标准 OBDA

那么应考虑：

- Ontop
- R2RML / Ontop mapping
- SPARQL-to-SQL 重写

优点：

- 更符合 OBDA 正统路线
- 对关系库更自然
- 查询层语义更正交

缺点：

- 对复杂规则推理不是最佳路线
- DuckDB 生态兼容性与接入成本需要单独验证

推荐条件：

- 你优先要“语义查询关系库”，不是优先要“复杂规则推理”

### 路线 C：推理优先，转 Jena

如果你的核心目标是：

- 更强的推理能力
- 更灵活的规则表达
- 更成熟的语义推理生态

那么应考虑：

- Apache Jena
- Fuseki / TDB
- Jena rule engine

优点：

- 推理系统更成熟
- 可扩展性与规则层更强
- 语义能力比当前 Python 原型更接近“推理平台”

缺点：

- 系统复杂度更高
- 与当前 Python-only 原型差异大

推荐条件：

- 你优先要“真正的推理平台”，而不是优先要“本地轻量原型”

---

## 7. 推荐决策

### 7.1 推荐的近期策略

近期不建议立刻切换到 Ontop 或 Jena。

更稳妥的做法是：

1. 先把设计纠正
2. 先把 ontology 语义补完整
3. 先把 server 的硬编码路径分析从核心推理职责里剥离
4. 再基于真实需求判断走 Ontop 还是 Jena

### 7.2 为什么不建议现在立刻迁移

因为目前最大问题不是引擎选错，而是：

- ontology 还没表达出要推理的公理
- 关系词汇没有统一
- 业务规则、图路径、OWL 推理还没分层

如果不先解决这些问题，换 Ontop 或 Jena 也只会把混乱搬家。

### 7.3 当前推荐路线

当前推荐路线是：

```text
先按路线 A 整理成“正确分层的 Python 原型”
然后根据核心目标二选一：
  - OBDA 优先 -> Ontop
  - 推理优先 -> Jena
```

---

## 8. 新的服务边界

### 8.1 核心 API

核心 reasoner service 保留：

```text
POST /sparql
GET  /schema
GET  /sample/{class_name}
POST /reload
GET  /health
```

说明：

- `/schema` 是 Skill 生成 SPARQL 的第一步输入
- `/sample/{class_name}` 是 Skill 做 schema grounding 和属性定位校验的关键接口
- 因此 `/sample` 不是可有可无的调试接口，而是 Skill 配合 Server 的必要组成部分

### 8.2 非核心 API

以下不应再被视为“推理机核心”，但建议作为 Optional Analyzer 独立存在：

```text
GET  /analysis/profiles
GET  /analysis/paths
POST /analysis/paths
POST /analysis/explain
POST /analysis/neighborhood
POST /analysis/inferred-relations
```

推荐职责如下：

```text
GET /analysis/profiles
  输入：无
  输出：Server 当前支持的分析档位、默认深度、默认过滤策略

GET/POST /analysis/paths
  输入：source entity, analysis profile/intent, optional target, max depth
  输出：匹配路径

POST /analysis/explain
  输入：一个实体或一组路径
  输出：可读解释

POST /analysis/neighborhood
  输入：entity + hop + predicate filters
  输出：局部子图

POST /analysis/inferred-relations
  输入：predicate filter / entity filter
  输出：只看推理后新增的三元组或候选关系
```

推荐原则：

- analyzer 必须是通用的，不得将起点类型写死为 `customer`
- analyzer 必须是受约束的，不得做无边界图遍历
- analyzer 默认只返回有限结果，并且必须支持自动收敛与可选的谓词白名单或黑名单
- analyzer 默认隐藏 middle object，除非调用方明确要求保留
- analyzer 的目标是“解释与探索”，不是替代 `/sparql`
- Skill 应优先传“分析意图/分析档位”，而不是直接生成低层遍历参数
- 低层参数可以保留为高级 override，但不应成为智能体的默认输入面

并明确说明：

- 这是 explanation/analyzer
- 不是 OWL 推理本身

### 8.2.0 当前 Analyzer 的真实职责

当前 `analysis/paths*` 与 `/causal/{customer_id}` 的职责应明确为：

- 对已给定锚点做受约束路径分析
- 对当前候选结果做解释
- 为 SPARQL 结果补充“为什么连得上”的路径证据

它们当前**不应**被理解为：

- 全图候选发现器
- 自动因果发现引擎
- 先分析、后筛选的通用业务求解器

因此当前推荐执行路线是：

```text
schema
  -> main sparql
  -> analyzer (optional, only if sparql found candidates)
```

而不是：

```text
schema
  -> analyzer first
  -> try to infer final candidate set
```

后者属于未来能力，不属于当前 `paths` 接口的职责边界。

### 8.2.1 Analyzer 推荐输入契约

为了避免把低层图遍历参数暴露给 Skill，推荐将输入拆成两层。

第一层是 Skill 默认使用的“意图驱动输入”：

```json
{
  "mode": "paths",
  "profile": "causal",
  "source": "http://ywyinfo.com/example-owl#customer_CUST004",
  "target": "http://ywyinfo.com/example-owl#remediationstrategy_STR003",
  "max_depth": 4
}
```

说明：

- `mode` 必填，表示要做路径、邻域、推理新增关系还是解释
- `profile` 推荐必填，表示探索意图或探索档位
- `source` 必填
- `target` 可选，若存在则只返回命中目标的路径
- `max_depth` 可由调用方指定，也可由 Server 根据 profile 给默认值并做上限裁剪

第二层是 Server 内部展开的“高级控制参数”，仅作为高级 override 保留：

```json
{
  "allowed_predicates": [
    "http://ywyinfo.com/example-owl#hasEvent",
    "http://ywyinfo.com/example-owl#hasPerception",
    "http://ywyinfo.com/example-owl#suggestsStrategy"
  ],
  "exclude_predicates": [
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
  ],
  "include_middle_objects": false,
  "include_inferred_only": false,
  "include_explicit_only": false,
  "direction": "outgoing",
  "limit": 20
}
```

说明：

- `allowed_predicates` 不应再作为 Skill 的默认生成目标
- `limit` 必须有默认值，避免图爆炸
- `include_middle_objects` 默认 `false`
- `include_inferred_only` 与 `include_explicit_only` 不可同时为 `true`
- 当 ontology 与图统计足够稳定后，这些字段应尽量由 Server 自动决定

### 8.2.2 Analyzer 自动发现与动态收敛

长期目标不是手写 profile 白名单，而是让 Analyzer 由 ontology 和推理后图自动驱动。

推荐将 Analyzer Engine 拆成 4 个内部组件：

```text
Schema Profiler
  扫描 class / object property / data property / domain / range / inverse / chain

Relation Classifier
  基于 ontology 注解和图统计，识别 causal / structural / inferred-relevant / middle-object

Profile Builder
  生成对外可见的分析档位与默认过滤策略

Path Planner
  在请求时按 source/target/profile/mode 做局部规划与搜索
```

推荐运行时机：

- 启动时执行一次
- `/reload` 后重新执行
- ontology 或 mapping 更新后重新构建

自动收敛依据：

- ontology 中声明的 `owl:ObjectProperty`
- `domain/range`
- `inverseOf`
- `subPropertyOf`
- `propertyChainAxiom`
- 图上的扇出统计、连通性、middle object 模式
- 可选的 ontology annotation，例如 `analysisRole=causal`

结论：

- 自动发现是“预处理阶段”的工作
- 动态收敛是“请求规划阶段”的工作
- 这不应再是手工维护 URI 名单的过程

### 8.2.3 Analyzer 模式

推荐提供以下模式：

```text
paths
  在受约束谓词集合内找 source -> target 或 source -> * 的路径

neighborhood
  返回指定实体周围 n-hop 的局部子图

inferred-relations
  只返回推理后新增的三元组，支持谓词和实体过滤

explain
  将给定路径或子图结构转换为可读解释
```

### 8.2.4 Analyzer 默认过滤规则

若调用方未明确要求，默认应排除：

- `rdf:type`
- `rdfs:label`
- 大部分纯展示型 datatype property
- middle object
- 空 domain/range 且无法解释的技术性节点
- 高扇出但低解释价值的 hub-like 关系

否则 analyzer 很容易产生大量无业务意义的结果。

### 8.2.5 Response 结构建议

#### `/analysis/paths`

```json
{
  "mode": "paths",
  "source": "http://ywyinfo.com/example-owl#customer_CUST004",
  "target": "http://ywyinfo.com/example-owl#remediationstrategy_STR003",
  "path_count": 1,
  "truncated": false,
  "paths": [
    [
      {
        "subject": "http://ywyinfo.com/example-owl#customer_CUST004",
        "predicate": "http://ywyinfo.com/example-owl#hasEvent",
        "object": "http://ywyinfo.com/example-owl#event_EVT005",
        "inferred": false
      },
      {
        "subject": "http://ywyinfo.com/example-owl#event_EVT005",
        "predicate": "http://ywyinfo.com/example-owl#hasPerception",
        "object": "http://ywyinfo.com/example-owl#perception_PER003",
        "inferred": false
      }
    ]
  ]
}
```

#### `/analysis/inferred-relations`

```json
{
  "mode": "inferred-relations",
  "count": 5,
  "triples": [
    {
      "subject": "...",
      "predicate": "...",
      "object": "...",
      "inferred": true
    }
  ]
}
```

### 8.2.6 与 Skill 的配合原则

Analyzer 只应在以下场景被 Skill 调用：

- 用户问“为什么”“怎么关联”“路径是什么”“有哪些隐藏关系”
- SPARQL 结果需要进一步解释
- 需要区分显式关系与推理新增关系

Skill 调用 Analyzer 的推荐顺序：

1. 先调用 `/schema`
2. 需要 grounding 时调用 `/sample/{class_name}`
3. 若存在 `/analysis/profiles`，优先读取可用 profile
4. 仅传 `mode/profile/source/target/max_depth`
5. 只有当 Server 明确要求时，才传低层 override 参数

而以下场景应优先使用 `/sparql`：

- 事实查询
- 属性过滤
- 聚合统计
- 简单关系查询

### 8.2.7 未来的 Analyzer-First 方向

如果未来希望支持“先分析，再发现候选”，那需要新增一类独立能力，而不是继续复用当前 `paths` 接口。

推荐未来拆分为：

```text
POST /analysis/discover
POST /analysis/find-candidates
POST /analysis/classify-subgraph
```

其职责可以是：

- 从给定 profile 出发，在受约束图中发现候选实体集合
- 自动识别值得进一步解释的 source 节点
- 对候选集合再交给 `/analysis/paths` 或 `/analysis/explain`

因此未来理想形态可以是两段式：

```text
discover
  -> candidate set
  -> paths/explain
```

但这属于未来能力，不应与当前 `query-first-then-analysis` 的线路混用。

### 8.3 向后兼容 API

为了和现有 Skill 平滑配合，短期内保留：

```text
GET /causal/{customer_id}
```

但它应被视为：

- `analysis/paths` 的一个 customer-friendly alias
- 兼容接口，不是长期唯一入口

换句话说，目标不是删除 `/causal`，而是让它退化为通用 analyzer 之上的便捷包装。

兼容语义建议：

- `/causal/{customer_id}` 内部可转译为一次 `/analysis/paths` 调用
- 默认：
  - `source = customer_{id}`
  - `profile = causal`
  - `max_depth = 3`
  - `include_middle_objects = false`

---

## 9. ontology 改造原则

若继续沿用 Python 原型并希望真正利用本体弹性，ontology 至少需要补以下内容。

### 9.1 统一核心关系词汇

需要选定一组正式关系名，例如：

- `customer_hasBehavior`
- `customer_initiate_event`
- `event_hasPerception`
- `perception_suggestsStrategy`

或者保留现有命名体系，但必须统一。

禁止再出现：

- ontology 里一套关系名
- mapping 里另一套关系名
- server 里第三套关系名

### 9.2 需要的公理类型

如果希望自动推出隐藏关系，应在 ontology 中加入类似公理：

- inverse property
- property chain
- subproperty hierarchy

例如，若你想推出：

```text
customer -> perception
```

可考虑形式上表达为：

```text
customer_initiate_event o event_hasPerception -> customer_hasPerception
```

若你想推出：

```text
customer -> strategy
```

可考虑：

```text
customer_hasPerception o perception_suggestsStrategy -> customer_suggestsStrategy
```

### 9.3 middle object 的原则

如果 ontology 需要忠实表达多对多 link entity，则保留 middle object。

如果主要目标是语义查询与推理，则应同时具备：

- middle object 事实
- 主实体之间可查询的 object property

这样既保留建模精度，也保留查询可用性。

---

## 10. 业务规则应该放哪里

以下内容不应再写成“owlrl 自然就会推出来”：

- 低满意度阈值
- 网络体验问题分类
- 复杂客户分群
- 多指标组合打标

推荐三种实现层次：

### 10.1 最轻量：SPARQL 查询层

直接用 `FILTER`、`BIND`、`CONSTRUCT` 表达。

适合：

- 原型
- 简单分类

### 10.2 中等：规则层

可引入：

- SHACL Rules
- SWRL
- 自定义 rule engine

适合：

- 规则较多
- 需要可维护的业务规则体系

### 10.3 最重：独立业务分析服务

将策略推荐、客户分群、风险判定单独做成分析服务。

适合：

- 业务规则复杂且变化快
- 不希望把业务策略绑死在 ontology 中

---

## 11. 立即执行的重构任务

### 任务 1：修正文档定位

对外不再宣称当前系统是“完整推理机”。

应描述为：

```text
基于 DuckDB -> RDF 映射、OWL RL 闭包、SPARQL 查询的本地语义原型
```

### 任务 2：移除核心设计中的硬编码推理承诺

从设计层删除以下错误表达：

- “owlrl 推理后应自动得到 customer -> perception”
- “owlrl 推理后应自动得到 customer -> strategy”

除非对应公理已写入 ontology。

### 任务 3：统一关系词汇

对以下三处统一词汇：

- `Onto/cem.owl`
- `mapping.yaml`
- agent/query 模板

### 任务 4：将 `/causal` 降级为可选分析器

明确它是：

- explanation API
- 依赖推理后图，但不是推理本身
- 并为通用 analyzer 提供一个向后兼容 alias

### 任务 4.5：保留并强化 `/sample`

明确 `/sample/{class_name}` 是：

- Skill 的 grounding 接口
- schema 错配排查接口
- 生成 SPARQL 前的验证工具

不得在“精简 Server API”的名义下删除

### 任务 4.6：实现 Query Compiler / Validator

目标：

- 保留 query-first-then-analysis
- 不污染通用 Skill
- 提高首轮主查询命中率

最低实现要求：

- 将 `causal_enumeration` 的主查询生成改为槽位化
- 在执行前做 schema 约束校验
- 强制返回 URI 锚点列
- 将恢复路径收敛为“一次定向 sample + 一次 rerun”

这项任务是当前下一轮的优先级最高项，应作为后续恢复工作的直接起点。

### 任务 4.7：设计并实现 Presentation Layer

目标：

- 不把最终话术硬编码到 Python
- 让原始查询/分析结果先经过结构化展示层
- 让 Claude 面向 `presentation` 组织最终回答

最低实现要求：

- 在 `run` 返回中增加 `presentation`
- 第一阶段支持：
  - `causal_enumeration`
  - `causal_lookup`
- 展示层支持：
  - 分组
  - 去重
  - 计数
  - label 解析
  - 路径亮点压缩
  - `display` / `refs` 分离

明确不做：

- 直接在 Python 中输出最终中文答案
- 用 `EVT001 / PER002 / STR001` 作为默认主展示内容
- 把 CEM 专属话术写成通用 formatter

### 任务 5：决定长期路线

做一次明确选择：

- 若优先查询重写与标准 OBDA，进入 Ontop 评估
- 若优先复杂语义与规则推理，进入 Jena 评估

### 任务 6：为 Analyzer 记录近中远期演进计划

避免只讨论“今天怎么做”，必须同时记录：

- 近期可落地的最小实现
- 中期的自动化收敛能力
- 远期的平台迁移或语义增强路线

---

## 12. 近中远期路线

### 12.1 近期：最小可用 Analyzer

近期目标不是追求完全自动化，而是先让 Skill 与 Server 稳定配合。

推荐做法：

- 保留 `/sparql`、`/schema`、`/sample`、`/reload`
- 增加通用 `/analysis/...` 接口
- 保留 `/causal/{customer_id}` 作为兼容 alias
- Server 侧先提供少量内置 profile，例如：
  - `causal`
  - `structural`
  - `inference`
- Skill 只选择 `profile`，不再生成 `allowed_predicates`

这一步允许用“最简单可控”的方式先上线，但必须满足：

- profile 集中配置
- 不把业务推理写死在 path 代码里
- 不要求 Skill 手工拼低层遍历参数

### 12.2 中期：自动发现与自动收敛

当 ontology 关系词汇稳定后，进入自动化阶段：

- 从 ontology 自动提取 object property、domain/range、inverse、chain
- 从推理后图自动计算关系扇出、连通性、middle object 模式
- 自动生成或修正 profile
- 支持按 profile 返回默认深度、默认过滤规则、默认隐藏策略
- 允许用 annotation 对自动分类做少量人工校正

这一阶段的目标是：

- 减少手工维护 profile 的成本
- 保持本体增大后的灵活性

### 12.2.1 下次恢复时的直接入口

如果后续要从本文档继续工作，优先从下面这个问题恢复：

```text
如何在不污染通用 Skill 的前提下，为 causal_enumeration 增加受约束的 SPARQL compiler / validator，
从而减少首轮 empty_result，并把恢复路径收敛为 one targeted sample + one rerun？
```

换句话说，下次工作的起点不是继续讨论“要不要先 analysis”，而是：

- 保留 query-first-then-analysis
- 收紧主查询生成
- 收紧恢复路径
- 让 Skill 只表达意图，不处理图搜索细节

如果后续优先做展示优化，则直接从下面这个问题恢复：

```text
如何为 run 增加 presentation 层，
让 Python 只输出结构化展示模型，而把最终自然语言组织交给 Claude？
```

这一项的第一阶段范围是：

- `causal_enumeration`
- `causal_lookup`

### 12.3 远期：分层迁移与平台升级

远期有两条路线：

- OBDA 优先：
  - 查询重写与虚拟知识图方向
  - 向 Ontop 评估迁移
- 推理优先：
  - 更强规则和可解释推理方向
  - 向 Jena 或规则引擎路线评估迁移

无论走哪条路线，Analyzer 的职责都应保留为：

- explanation
- path planning
- inferred relation inspection

而不是回退成应用层硬编码业务推理。

---

## 13. 推荐的短期目标架构

短期推荐架构如下：

```text
用户
  -> Agent（自然语言理解、SPARQL 生成、结果解释）
  -> Reasoning Service
      - materialize RDF from DuckDB
      - load ontology
      - run OWL RL closure
      - expose /sparql /schema /sample /reload /health
  -> Optional Analyzer Service
      - generic path analysis
      - causal path explanation
      - inferred-triple inspection
      - neighborhood extraction
      - business rule evaluation
      - report generation
```

这比当前设计更真实，也更利于后续迁移。

---

## 14. 最终推荐

最终推荐结论：

1. 先不要继续往 `reasoning_server.py` 里增加硬编码“推理”逻辑。
2. 先把 ontology、mapping、query vocabulary 统一。
3. 先把推理机和分析器拆开。
4. 当前阶段把系统定位为：
   - 小规模、本地、Python 原型
   - 支持 RDF 物化、OWL RL、SPARQL
   - 但不是 Ontop/Jena 级别的平台
5. 等 ontology 真正稳定后，再决定：
   - Ontop 路线
   - 还是 Jena 路线

这才是能持续演进的设计。
