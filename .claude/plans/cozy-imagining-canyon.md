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

更进一步地说，当前首轮失败还暴露出一个更深的边界问题：

- 仅仅“拿到 ontology/schema 结构”并不等于“可以稳定写出正确主查询”
- ontology/schema 本身已经包含类语义和属性语义，而不只是“结构”
- 其中包括类/属性名称、label、domain/range、对象属性方向等关键信息
- 当前缺的不是“属性没有语义”，而是“自然语言问题如何稳定 grounding 到这些已有语义”
- 尤其当语义落在实例值层时，例如事件类型字符串、描述文本模式、值域口径，仍然需要额外的 grounding 过程

因此问题不只是“SPARQL 写错”，而是“自然语言 grounding 与查询生成的责任边界划错了”。

### 4.3.3 LLM / Semantic Query Planner / Ontology 的责任边界

当前设计的一个核心反思是：

- 不应继续让 LLM 直接负责写最终 SPARQL
- 不应把 repo 当前语义再平行复制成一份大型手工 profile 表
- 也不应指望 analyzer 反过来弥补首轮主查询的语义缺失

更合理的责任划分应当是：

1. LLM
   - 识别问答类型，如 `causal_enumeration`、`causal_lookup`
   - 从自然语言中提取语义槽位
   - 判断是否存在歧义、是否需要澄清
   - 基于结构化结果组织最终自然语言回答

2. Ontology
   - 作为第一语义真源
   - 提供类语义、属性语义、label、domain/range、对象属性方向
   - 当业务语义已经被形式化时，优先直接依赖 ontology，而不是再平行造一层外部语义表

3. Semantic Query Planner
   - 接收模板类型和语义槽位
   - 先做 ontology-first grounding，把槽位落到类、属性、值约束候选
   - 再将这些语义约束整理成稳定的 semantic request IR
   - 基于该 IR 构建 node-based query plan，而不是直接拼接查询语句
   - 最后再决定结果粒度、关系方向、返回列、analyzer 路线，并将执行计划 lowering 成 `builder` / `SPARQL`
   - 在执行前做 schema 与运行时 mapping 校验

这里的 `planner` 是总称，不是单一阶段。

其内部至少包括：

- semantic request builder
  - 将“模板 + 槽位 + 锚点 + 输出诉求”整理为中间语义请求
- grounder
  - 将自然语言槽位 grounding 到 ontology 的类、属性、值约束
- planner
  - 基于语义请求构建 node-based query plan
  - 选择结果粒度、analyzer 路线与合法图遍历骨架
- lowerer / compiler
  - 将 node-based plan 降为 `builder` / `SPARQL`
- validator
  - 检查 schema、mapping、锚点列、约束完整性

这里需要特别修正一个之前不严谨的说法：

- `query shape` 不能作为 planner 的核心表示
- `shape` 最多只应作为搜索空间约束，用来表示某类 plan 允许的骨架
- 真正的核心表示应是：
  - semantic request IR
  - node-based query plan
  - SPARQL lowering

这更接近 MetricFlow 的机制：

- 先把请求解析成稳定语义对象
- 再构建 dataflow / node-based plan
- 最后做 lowering 与执行

对当前 repo 而言，planner 的直接 lowering 目标应是 `SPARQL`。
`SQL lowering` 更像底层 OBDA engine 的后续阶段，不应和当前 client planner 的职责混在一起。

对当前系统而言，最关键的改动是：

- LLM 不再直接产出最终 SPARQL
- LLM 只产出“模板 + 槽位 + 歧义判断”
- semantic query planner 基于 ontology-first grounding 把这些槽位落成可执行查询

例如，对问句：

`因为网络问题,哪些客户投诉了?`

LLM 更适合先产出类似这样的中间语义：

- template: `causal_enumeration`
- source entity: `customer`
- evidence entity: `event`
- cause term: `网络问题`
- action term: `投诉`
- result grain: `customer`

随后由 semantic query planner 决定：

- `customer` / `event` 对应的真实 ontology class
- `网络问题` 如何 grounding 到类、属性或值约束
- `投诉` 如何 grounding 到动作/状态约束
- 主查询返回哪些锚点列和展示列

这也是为什么当前阶段仅靠 schema-first + builder/validator 仍然不够：

- 它能保证“查询结构更对”
- 但还不能保证“问句语义完整进入主查询”

### 4.3.4 Revised Planner Pipeline

当前进一步反思后，需要把 `semantic query planner` 的内部形态说得更具体。

它不应再被理解成：

- 一个“更聪明的 SPARQL 生成器”
- 一组不断膨胀的词法扩展规则
- 一个继续把不同问题强行塞进 `causal_lookup / causal_enumeration` 的补丁层

更合理的形态应当是一个分阶段、受约束、带预算的执行器。

建议固定为以下三层流水线：

1. Language Intent Parser
   - 先把用户输入解析成语言层的结构化请求，而不是直接进入 ontology grounding
   - 它负责的是语言控制结构，不是领域语义绑定
   - 它的推荐形态应是：
     - `LLM-first`
     - `schema-first`
     - `deterministic-minimal`
   - 至少应包含：
     - utterance decomposition
     - dependency / conditional parsing
     - reference resolution
     - question act / operator parsing
     - anchor form detection
     - numeric / negation / quantifier / comparison parsing
   - 这层产物应先收敛成 canonical `Intent IR`
   - deterministic 层允许保留的只应是 language-agnostic surface normalization / hints，例如：
     - Unicode / 标点 / 空白归一化
     - 数字字面量
     - 符号比较器：`< > <= >=`
     - URI / local-name / identifier-like literal 形态
     - span / offset 保留
   - 语言相关的 dependency / question-word / reference 解释
     不应成为 deterministic core contract，而应由 LLM-first parser 或 locale-aware adapter 负责
   - `explain / remediation / enumerate / lookup` 这类 operator
     未来应由通用 question-act parser 给出，而不是由领域语义词表硬编码出来
   - 不允许在这里保留领域语义词表，例如：
     - `ROLE_PATTERNS`
     - `SOLUTION_REQUEST_TERMS`
     - `EXPLANATION_REQUEST_TERMS`
     - `SEMANTIC_LABEL_SUFFIXES`
   - 原因是这些词表会把语言层错误地提升成领域语义决策层，违反四原则

2. Ontology Grounding Layer
   - 只负责把开放语义槽 grounding 到 ontology / mapping / value catalog
   - grounding 的输入应来自 `Intent IR` 中的开放槽位，而不是直接消费一整句 raw text
   - 这层至少包括：
     - anchor binding
     - class / attribute / relation / value candidate recall
     - ontology / mapping structural pruning
     - grounded constraint view
   - grounding 不应只依赖词法字符串匹配
   - 最终应采用：
     - schema / manifest label recall
     - 本地语义召回
     - ontology / mapping 结构裁剪

这里需要特别强调一个边界：

- `phone`、`customer ID`、`workorder ID` 这类说法，不应作为 parser 或 planner 核心枚举类型
- 这些最多只应是 manifest binding 后的解释结果
- parser 只负责识别“值的形态”和语言控制关系
- 具体它绑定到哪个 ontology 属性 / class，应由 grounding 层决定

3. Planner / Lowering / Validator
   - 只消费：
     - `Intent IR`
     - grounded constraints
     - resolved references / discourse state
   - planner 内部再分为：
     - family routing
     - semantic request IR builder
     - node-based planner
     - lowerer
     - validator
   - family routing 的职责是把高层意图映射到最小可执行 family，例如：
     - `anchored_fact_lookup`
     - `anchored_status_lookup`
     - `anchored_causal_lookup`
     - `enumeration`
     - `causal_enumeration`
     - `hidden_relation`
   - semantic request IR builder 应显式表达：
     - query family
     - anchors
     - semantic targets
     - predicates / filters
     - output grain
     - analysis / explanation / remediation request
   - planner 基于 semantic request IR 构建 node-based query plan
   - 不在运行时开放式探索
   - 不允许退化成无约束 sample / grep / sparql 试探链
   - 如果需要描述图遍历骨架，可保留 shape 作为 search hint，但不能把它当成最终 plan
   - lowerer 将 node-based plan 优先 lowering 为 `sparql.builder`
   - 只有 builder 无法表达时，才 lowering 为 raw `SPARQL`
   - validator 在执行前做硬校验：
     - 类/属性是否存在
     - domain/range 是否匹配
     - link direction 是否可执行
     - 返回列是否满足 analyzer 与 presentation 所需的锚点要求

这个 revised pipeline 的关键含义是：

- parser 的重点不是“猜 ontology 概念”，而是“稳定解析语言控制结构”
- grounding 的重点不是“硬编码问句词表”，而是“把开放语义槽绑定到 ontology”
- planner 的重点不是“替代 LLM 写查询”，而是“先路由，再编译，再校验，再执行”

因此，`Semantic Query Planner` 的价值不在于保证每次都 top-1 命中，
而在于：

- 让 `Intent IR` 成为稳定输入
- 让 grounding 成为独立层，而不是散落在词法/路由/lowering 各处
- 快速得到少量候选
- 快速验证哪些候选结构合法
- 低置信时快速失败，而不是继续浪费预算做开放探索

继续参考 MetricFlow 时，这里的关键不是“更会猜查询语句”，
而是“先把语言意图稳定化，再做 ontology grounding，再把它编译成 plan nodes，再做 lowering”。

#### 三层职责定义

为了避免 parser / grounding / planner 再次混层，这里需要把三者的职责、输入输出和失败语义固定下来。

1. Language Intent Parser
   - 设计思想：
     - 它是语言前端，不是 ontology 语义解释器
     - 它负责把“用户在说什么结构”解析清楚，而不是把结构直接绑定到图谱
   - 输入：
     - 原始 utterance
     - 轻量 conversation state
     - 已知引用上下文
   - 输出：
     - `QuestionUnit[]`
     - unit 之间的 dependency / DAG
     - canonical `Intent IR`
       - `focus`
       - `operators`
       - `constraints`
       - `references`
       - `output`
       - `anchor_forms`
       - `comparators`
   - 负责：
     - LLM-first 的语言意图解析
     - 分句
     - 条件依赖
     - 指代/引用
     - question act 解析
     - 数值比较、否定、量词、范围
     - 锚点形态识别
     - 错别字 / 少字 / 长尾表达恢复
     - 多语言表述理解
   - 不负责：
     - 选择 ontology class / property / relation / value
     - 判定 `13800138004` 是 phone 还是 customer ID
     - 靠领域词表推出 score/status/solution/explanation property
     - 直接决定最终 SPARQL 或最终 query family plan
   - 失败语义：
     - parser 可以输出不完整 `Intent IR`
     - 但不能编造 ontology 绑定
     - 语言结构不明时，应保留 ambiguity，而不是提前猜 semantic slot

2. Ontology Grounding Layer
   - 设计思想：
     - 它是语义绑定层，不是语言理解层，也不是执行层
     - 它负责回答“这些开放语义槽在 ontology / mapping 里到底对应什么”
   - 输入：
     - `Intent IR`
     - semantic manifest
     - runtime data property catalog
     - value catalog / bounded `ValueNode`
   - 输出：
     - grounded anchors
     - grounded constraints
     - candidate bindings
     - `grounded constraint view`
       - `requested_text`
       - `effective_text`
       - `binding_terms`
       - `top_candidate`
       - ambiguity / unresolved reason
   - 负责：
     - anchor binding
     - class / attribute / relation / value candidate recall
     - domain/range/mapping structural pruning
     - grounded semantic view 生成
   - 不负责：
     - 重新做分句和依赖分析
     - 修改用户问题语义
     - 开放式 sample / grep / sparql 探测
     - 用领域词表硬编码 role/class/property
   - 失败语义：
     - grounding 不足时，应返回 unresolved / low-confidence binding
     - 不能把弱 binding 伪装成可执行语义
     - 不能为了“尽量答出来”越层替 parser 或 planner 做决定

3. Planner / Lowering / Validator
   - 设计思想：
     - 它是受约束的查询编译器，不是语言理解器，也不是 ontology 命名猜测器
     - 它负责回答“已经绑定好的语义请求，如何最小、合法、可执行地落成查询计划”
   - 输入：
     - `Intent IR`
     - grounded constraints
     - resolved references / discourse state
   - 输出：
     - `query_family`
     - semantic request IR
     - node-based query plan
     - lowered builder / SPARQL
     - validator result
     - execution / fail-closed / bounded recovery contract
   - 负责：
     - family routing
     - semantic request IR 收敛
     - node-based plan 构建
     - lowering
     - validator
     - bounded recovery policy
   - 不负责：
     - 重新读取 raw utterance 做语言理解
     - 靠 lexical phrase table 推 ontology 语义
     - 把 `planning_required` 退化成自由探索链
   - 失败语义：
     - plan 不合法、grounding 不足或 validator 不通过时，应 fail closed
     - 最多只允许受控 widening 或一次有界恢复
     - 不能把失败转嫁为 Claude/Agent 的手工 sample / 手写 SPARQL / 自行改写问题

可以把三层的边界记成一句话：

- Parser 解决“用户在说什么结构”
- Grounder 解决“这些结构在 ontology 里对应什么”
- Planner 解决“如何把这些已绑定语义编译成可执行查询”

#### Language Intent Parser 详细设计

在当前架构下，`Language Intent Parser` 不应被实现成“又一层词表 + regex if/else”，
也不应被实现成“LLM 直接自由输出一段半结构化文本”。
更合理的形态是：

- 一个有严格输出 schema 的 parser front-end
- 一个 `LLM-first` 的意图解析主体
- 一个 minimal language-agnostic lossless canonicalizer
- 一个 repo-owned exactness / schema validator
- 一个 backend 可替换的模型接入层

##### 设计原则

1. parser 只产出语言结构，不产出 ontology 绑定
   - 不输出 class/property/value node
   - 不输出 `phone / score / status property` 这类领域 role 判定
   - 只输出结构化语言意图

2. lossless canonicalization 与 language understanding 分开
   - canonicalization：
     - Unicode / 标点归一
     - span / offset 保留
     - exact literal 保真
   - language understanding：
     - utterance decomposition
     - question act
     - operator scope
     - dependency / reference
     - ellipsis / 承接省略
     - typo / omission recovery
     - 多子句之间的语义组合

3. LLM-first for understanding, deterministic-minimal for exactness
   - language understanding 应由 LLM-first parser 主导
   - deterministic 层只保留跨语言稳定的最小 canonicalization / exactness support
   - model 的输出必须受 schema 约束

4. parser 可以不确定，但不能越权
   - 可以输出 ambiguity / confidence
   - 可以要求 clarification
   - 不能越层替 grounder 绑定 ontology
   - 不能越层替 planner 生成 plan

5. strategy / backend / provenance 必须分层命名
   - `Parser Strategy`
     - `DeterministicStrategy`
     - `HybridStrategy`
   - `Model Backend`
     - `NoModelBackend`
     - `AnthropicCompatibleCliBackend`
     - `OpenAICompatibleBackend`
     - `MockModelBackend`
   - `IR Provenance`
     - `deterministic_ir`
     - `hybrid_ir`
     - `llm_generated_ir`
   - 无论 strategy / backend 如何组合，都必须输出同一份 canonical parse schema

##### Parser 输入输出

`ParserInput`

- `utterance`
- `conversation_state`
- `reference_context`
- `strategy`
  - `DeterministicStrategy`
  - `HybridStrategy`
- `model_backend`
  - `NoModelBackend`
  - `AnthropicCompatibleCliBackend`
  - `OpenAICompatibleBackend`
  - `MockModelBackend`

`ParserOutput`

- `question_units`
- `dependency_dag`
- `intent_irs`
- `parser_confidence`
- `ambiguities`
- `clarification_candidates`
- `parser_evidence`
- `ir_provenance`

##### Canonical Parser Schema

每个 `QuestionUnit` 至少应包含：

- `unit_id`
- `raw_text`
- `normalized_text`
- `dependency`
- `reference_markers`
- `anchor_forms`
- `comparators`
- `question_acts`
- `surface_constraints`
- `ambiguities`
- `confidence`

对应的 `Intent IR` 最低应包含：

- `focus`
  - `anchored_entity`
  - `referenced_result_set`
  - `implicit_entity_set`
  - `global_enumeration`
- `operators`
  - `lookup`
  - `enumerate`
  - `status_check`
  - `explain`
  - `remediation`
  - `count`
  - `compare`
  - `relation_discovery`
- `constraints`
  - `comparison`
  - `surface_predicate`
  - `cause_phrase`
  - `target_phrase`
  - `time_scope`
  - `negation`
  - `quantifier`
- `references`
  - `depends_on`
  - `reference_scope`
  - `distribution`
- `output`
  - `shape`
  - `grain`
  - `needs_analysis`
- `ambiguities`
- `confidence`

这里的关键点是：

- parser 可以输出 `surface_predicate = 低满意度`
- parser 可以输出 `comparison = 满意度评分 < 3`
- parser 可以输出 `question_act = remediation`
- 但 parser 不能输出：
  - `customerbehavior_满意度评分`
  - `remediationstrategy`
  - `hasBehavior`
  - `status-like property = customerbehavior_满意度评分`

##### 内部流水线

建议固定为五步：

1. Lossless Canonicalization
   - 统一全角/半角
   - 统一标点
   - 统一空白/Unicode
   - 保留 span offset，便于后续 evidence 回溯
   - 只做 lossless canonicalization，不承担语言理解职责

2. LLM Intent Parse
   - 由 LLM-first parser 主导：
     - utterance decomposition
     - dependency / reference parsing
     - question-act parsing
     - ellipsis resolution
     - typo / omission recovery
     - multilingual understanding
   - 输出必须是 schema constrained 的 typed object

3. Constraint Assembly
   - 把 canonicalized text 和 LLM parse 收敛成 surface constraints
   - 例如：
     - `满意度评分 < 3`
     - `低满意度`
     - `因为网络问题`
     - `这些客户`
   - 这一步仍然不做 ontology binding

4. Exactness + Schema Validation
   - 校验模型输出中的 exact elements 是否可由原始输入或 canonicalized 输入支持
   - 重点包括：
     - 数字字面量
     - 符号比较器
     - identifier-like literal / URI / local-name
     - span / offset 对齐
   - 这一步是校验，不是重新理解语义

5. Intent IR Assembly
   - 组装 canonical `Intent IR`
   - 同时输出：
     - confidence
     - ambiguity set
     - clarification candidate
   - exactness 冲突时以 validator 为准
   - language understanding 以 validator 后的 LLM parse 为准
   - 无法收敛时应保留 ambiguity，而不是越权猜语义

##### Hybrid Parser 设计

最推荐的实现不是纯 deterministic，也不是“让 skill 直接自由发挥”，而是 hybrid parser：

1. `Lossless Canonicalizer`
   - 提供 minimal language-agnostic canonicalization
2. `LLM Intent Parser`
   - 负责主要语言理解
3. `Exactness / Schema Validator`
   - 统一收敛 exactness、schema、confidence、ambiguity

这意味着：

- LLM 可以被充分利用
- 但它不能直接决定 ontology 绑定
- 也不能跳过 schema 输出自由生成

##### 与 LLM 的边界

在 AI 时代，这一层完全可以利用 LLM，但边界必须写死：

- 允许：
  - LLM 在 repo 定义的 schema 之下生成 `ParserOutput` / `Intent IR`
  - 也就是 `LLM-generated Intent IR`
  - 前提是：
    - schema 由 repo 拥有
    - 字段语义由 repo 拥有
    - 结果必须经过 repo 内部校验

- LLM 可以做：
  - question-act parsing
  - discourse/reference interpretation
  - ellipsis resolution
  - ambiguity detection
  - clarification candidate generation

- LLM 不可以做：
  - 直接输出 ontology class/property/relation
  - 直接决定最终 query family lowering
  - 直接写 SPARQL
  - 在 parser 阶段补出 domain-semantic 绑定
  - 拥有 `Intent IR` contract 的定义权
  - 通过 skill prompt 私自扩展字段、改变字段语义或引入隐式 ontology 绑定

更具体地说，LLM 在 parser 层的正确位置是：

- 作为 `Intent IR` 的结构化生成器
- 而不是作为 ontology grounder 或 query planner
- 也就是说：
  - `LLM-generated Intent IR` 是允许的
  - `LLM-owned semantic contract` 是不允许的
  - `LLM-does-grounding/planning by stealth` 也是不允许的

进一步说，如果运行时是 Claude/Agent 客户端：

- 可以把 Claude 作为 parser backend
- 但不能把 skill prompt 当成 `Intent IR` 的最终定义位置
- skill 可以承载 parser backend 调用
- 不能承载 parser contract、grounding contract、planner contract

因此正确形态应是：

- repo 内定义：
  - `ParserInput`
  - `ParserOutput`
  - canonical `Intent IR`
- parser strategy 与 model backend 分层：
  - `DeterministicStrategy + NoModelBackend`
  - `HybridStrategy + AnthropicCompatibleCliBackend`
  - `HybridStrategy + OpenAICompatibleBackend`
  - `HybridStrategy + MockModelBackend`
- 无论 strategy/backend 是什么，最终都必须回到 repo-owned schema

##### 失败语义与澄清策略

parser 应显式区分三种失败：

1. 语言理解失败
   - 分句、依赖、指代、scope 无法稳定解析
   - 应优先请求澄清

2. question-act 歧义
   - 例如同一句可能既像 explain 又像 enumerate
   - 应返回 ambiguity set，而不是擅自选一个

3. semantic under-specification
   - 例如用户只说“低满意度”
   - parser 可以识别为 `status_check`
   - 但不能补 metric/threshold
   - 应把不足留给 clarification / grounding fail-closed 处理

##### 与 Grounder / Planner 的接口约束

parser 交给 grounder 的，不应是 raw question text，而应是：

- `Intent IR`
- `QuestionUnit`
- `surface_constraints`
- `anchor_forms`
- `comparators`
- `references`
- `ambiguities`

grounder 不应重新解释 question act；
planner 不应重新解释 raw utterance。

##### 迁移策略

在本 repo 内，建议按三步演进：

1. 先把当前 `extract_question_slots + bootstrap_*` 收敛成 parser adapter
   - 让现有逻辑先统一输出 canonical parser schema

2. 再把 parser 与 grounder 的边界切开
   - `bootstrap_candidates` 退化为 parser 输出中的 `surface_constraints`
   - grounding 只消费 parser schema，不再回读 raw slot flags

3. 最后引入 `HybridStrategy`
   - deterministic 层只保留 language-agnostic surface normalization / hints
   - 让 `AnthropicCompatibleCliBackend` / `OpenAICompatibleBackend` / `MockModelBackend` 接管 parser backend
   - 确保引入 LLM 后不会破坏 fail-closed 契约

当前实现状态补充：

- parser facade / parser contract / IR contract 已经独立成 repo-owned 模块
- parser 现在已经有通用 `ModelBackend` 层，并受 repo-owned schema 约束
- `ModelBackend` 现在是 protocol-first profile 层
  - backend 的 canonical naming、alias、transport family、env-based activation marker
    已统一收口到 repo-owned profile contract
  - 也就是说，provider/profile 的变化不再回流到 parser core
- 当前 canonical backend 为：
  - `NoModelBackend`
  - `AnthropicCompatibleCliBackend`
  - `OpenAICompatibleBackend`
  - `MockModelBackend`
- legacy alias 仍兼容：
  - `AgentModelBackend -> AnthropicCompatibleCliBackend`
  - `ClaudeCliBackend -> AnthropicCompatibleCliBackend`
  - `StandaloneModelBackend -> OpenAICompatibleBackend`
- parser facade 现在已经支持整句级的 model-backed parse，并能把 utterance-level parser output 投影回 unit-level parser output
- 若没有任何 model-host / model-api marker，默认主路径仍是 `DeterministicStrategy + NoModelBackend`
- 若环境中出现对应 protocol marker，则 parser 现在会自动解析为匹配的 backend，并自动提升到 `HybridStrategy`
  - mock marker -> `MockModelBackend`
  - OpenAI-compatible marker -> `OpenAICompatibleBackend`
  - Anthropic-compatible host marker -> `AnthropicCompatibleCliBackend`
- 当前 model-backed parser 仍不是默认成熟主路径
- `AnthropicCompatibleCliBackend` 适配本机 Anthropic-compatible CLI 宿主
  - 当前具体实现仍以本机 `claude` CLI 为 transport
  - 但 backend 语义上不再绑定 Claude 这个 provider
  - 例如 Kimi Code 通过 Claude Code / Anthropic-compatible 配置接入时，应走这条 backend
  - 当前实现已显式支持 Claude-style wrapper payload 中的 `structured_output`
    - 也就是说，真实宿主如果返回的是 `result + structured_output` 包装对象，parser backend 会优先消费 `structured_output`
  - 当前还具备 host-capability parse ladder
    - 优先尝试 schema mode
    - 若宿主不稳定支持 `--json-schema`，则自动降到 json-only mode
    - 仍由 repo validator 负责最终 schema/exactness 收口
- 真实 Claude Code 宿主 E2E 验证暴露了一条必须收紧的 skill/client 边界：
  - 如果 bundled client 的 `/schema` 首跳返回 `404`
  - 应先视为 base-url / transport mismatch
  - 不应在同一用户问题里退化成读取 repo 源码、手写 `curl`、手写 Python HTTP 请求或硬编码 `127.0.0.1:8000`
  - 也就是说，正常问答 turn 的失败恢复边界仍然必须停留在 client/transport 层，而不是升级成源码调试
- 真实 Claude Code 宿主读取的是 `.claude/skills/obda-query/SKILL.md`
  - 因此这份 host skill copy 必须与 `.agents/skills/obda-query/SKILL.md` 同步
  - 否则 repo 内 skill 文档即使更新，真实宿主 E2E 仍可能继续沿旧协议偏航
- bundled client transport 现在已显式处理 localhost / loopback 宿主场景：
  - 对 `127.0.0.1 / localhost / ::1` 的 HTTP 请求，Python `urllib` 会主动绕过 shell 注入的代理设置
  - `curl` fallback 也会显式附加 `--noproxy *`
  - 这是因为真实 Claude Code / 宿主 shell 中，loopback 请求可能被 `HTTP_PROXY / HTTPS_PROXY / ALL_PROXY` 类环境变量劫持
  - 典型症状是：
    - `curl http://127.0.0.1:8000/schema` 正常
    - 但 bundled client / `urllib` 请求同一地址返回 `404`
  - 当前仓库已补独立 transport regression：
    - `tests/run_transport_regressions.sh`
    - 用 fake proxy + real loopback server 锁住 “loopback 必须直连真实服务，不能穿代理” 这一行为
- 当前仓库已经新增独立的 host E2E regression 面：
  - `tests/obda_host_e2e_regressions.json`
  - `tests/run_host_e2e_regressions.py`
  - `tests/run_host_e2e_regressions.sh`
  - 这套回归会自举本地 reasoning server、向 Claude Code 宿主注入 `OBDA_BASE_URL` / `OBDA_STATE_FILE`，并断言：
    - `obda-query` skill 被选中
    - 使用 bundled client
    - 不退化成 repo 源码读取 / raw `curl` / 硬编码 `127.0.0.1:8000`
  - 当前这套真实宿主 E2E 已覆盖并通过：
    - anchored numeric status + solution follow-up
    - abstract status fail-closed + clarification
    - causal enumeration (`因为网络问题，哪些客户投诉了？`)
  - question-mode 宿主协议现在进一步收紧为：
    - 对自然语言问题，优先直接执行 question-mode `run`
    - 由 `run` 内部完成 `/schema` 获取并满足 schema-first 规则
    - 宿主不应在 normal question turn 中先单独调用一次 `schema` 再进入 `run`
  - host E2E 现在还额外锁定 `planning_required` hard-stop 路径：
    - 新增 `有哪些客户因为5G信号差而投诉了?` 用例
    - 预期宿主只执行一次 `run`
    - 不允许在 `planning_required` 后继续 `schema / schema --full / sample / sparql`
- `run --answer-only` 现在是面向真实宿主 agent 的 compact answer 路径
  - 它会去掉 parser / grounding / planner 的大块调试体，只保留回答用户和执行 stop-contract 必需的字段
  - QUESTION shorthand 现在也默认落到同一条 compact answer 路径
    - `run "question" --template ...`
    - `run "question" --template ... --answer-only`
    - `run "question" --template ... --json`
    都不会在 normal user-answer turn 中返回整块 planner/debug JSON
  - 对 `planning_required`，当前 compact contract 已明确收成硬停：
    - `must_stop = true`
    - 若同时提供 `final_user_reply`，真实宿主应直接返回该文本并停止
    - `must_reply_verbatim = true`
    - host-facing payload 在这种场景下只保留最小字段：
      - `final_user_reply`
      - `must_stop`
      - `must_reply_verbatim`
      - `forbidden_follow_up`
      - `forbidden_reply_additions`
    - 不再向真实宿主暴露 `question / template / query_family / message / recovery_policy / rules / next_action`
    - 明确禁止后续：
      - `run`
      - `--plan-only`
      - `question_rewrite_retry / semantic_rephrase_retry`
      - `schema / schema --full / schema grep`
      - `sample`
      - `sparql`
      - `repo_debugging`
    - 明确禁止追加说明：
      - `extra_explanation`
      - `possible_reasons`
      - `schema_suggestions`
      - `ontology_term_examples`
      - `question_rewrite_suggestions`
  - 也就是说，`--plan-only` 现在只保留给“用户明确要求 planner 调试”的场景，不再是 normal host answer turn 的默认后续动作
  - 为兼容真实 host 的老习惯，bare `run "question" --template ... --json` 现在也会默认落到同一条 compact answer 路径
  - 当 `next_action = ask_user_for_clarification` 时，compact payload 还会显式加入 `agent_contract`
    - 其中会声明 `must_stop`
    - 并列出禁止继续执行的 follow-up 类别，例如 `schema --full`、`sample`、`sparql`、`rerun_with_metric_rewrite`
- `OpenAICompatibleBackend` 用统一的 OpenAI-compatible transport 承接 Kimi / GLM / DeepSeek 这类 provider 的 HTTP 接入形态
- 当 utterance-level model parse 出现 timeout / backend failure 时，parser facade 现在会保留 deterministic multi-unit decomposition，并把每个 unit 安全降回 deterministic bundle，而不是把整句压扁成单 unit
- 也就是说，parser 的结构边界已经基本到位，但 `LLM-first` 仍属于“已接线、未默认、未完全稳定”的阶段

这条路线比“直接让 LLM 替代当前 planner”更符合四原则，因为它把 LLM 放在了正确的位置：

- 理解语言
- 但不直接承诺 ontology 语义与执行计划

### 4.3.4.1 Family / Manifest / Binder 的关系

这里需要把 `family`、`manifest`、`binder` 三者的关系说清楚，否则很容易再次退回到：

- LLM 直接猜 ontology 对象
- planner 直接从一段 text 找一个“最近的 class/property”
- 或继续往 client 里堆 phrase-specific 规则

正确关系应当是：

1. family 先定义 slot schema
   - `family` 不是业务场景标签，而是：
     - 查询意图
     - 返回形态
     - 运算模式
   - `family` 的职责是先定义“这类问题需要哪些语义槽位”
   - 例如：
     - `causal_enumeration` 需要：
       - `subject_text`
       - `cause_text`
       - `action_or_state_text`
       - `output_grain`
     - `anchored_causal_lookup` 需要：
       - `anchor_text`
       - `target_text`
       - `status_or_problem_text`
       - `asks_explanation`
       - `asks_solution`

2. slot extractor 只抽抽象槽位内容
   - 它从问句里抽出的应是：
     - 文本片段
     - 布尔意图
     - 输出粒度
   - 它不应在这个阶段就决定：
     - 哪个 ontology class 被选中
     - 哪个 property 被选中
     - 哪个值域被最终绑定

3. manifest 提供可绑定的 typed semantic space
   - manifest 不是另一份手工语义表，也不是直接做自然语言理解的组件
   - 它应是从：
     - ontology
     - mapping
     - runtime catalog
     - value catalog
     编译出来的 planner 视图
   - 它至少应包含：
     - `ClassNode`
     - `AttributeNode`
     - `RelationNode`
     - `ValueNode`
   - 每个 node 还应带：
     - label / local_name / comment
     - domain / range
     - executable relation info
     - filterability
     - capability metadata
       - identifier-like
       - contact-like
       - score-like
       - status-like
       - description-like

4. binder / grounder 负责把抽象槽位绑定到 manifest nodes
   - 这一步不是“text 直接找最近邻”
   - 而应是：
     - `slot text -> typed semantic retrieval -> structural binding -> plan selection`
   - 也就是说：
     - family 先告诉 binder 这个槽位允许绑定哪些 node types
     - binder 再在对应类型的 manifest nodes 里找候选

例如：

- `subject_text`
  - 优先绑定 `ClassNode`
- `cause_text`
  - 优先绑定 `AttributeNode + ValueNode`
- `action_or_state_text`
  - 优先绑定 `ValueNode + status-like AttributeNode`
- `anchor_text`
  - 优先绑定 `identifier-like / contact-like AttributeNode`

进一步地，slot 本身还可以带约束模式，而不只是裸文本：

- `status_or_problem_text`
  - 如果来自 `是否存在... / 有无... / 是否属于...` 这类状态判断问句
  - 则其 `constraint_mode` 应标记为 `status_check`
  - binder 不应再把它当成任意问题描述词去绑定
  - 只允许优先绑定：
    - `status-like AttributeNode`
    - `score-like AttributeNode`
    - 以及来自这类属性的 `ValueNode`

这意味着：

- `低满意度` 不应因为命中了任意 `event type` 或 `workorder description` 文本而被视为可执行
- 如果当前只绑定到 `score-like numeric attribute`，且问句中没有显式 comparator / threshold
- 正确行为应是：
  - `planning_required`
  - 或 `constraint_grounding_not_executable`
  - 而不是退化成文本 contains 查询
- 如果问句已显式给出 comparator / threshold
  - 例如 `满意度评分低于3`
  - 则应先抽成通用 numeric constraint IR
  - 再走通用 comparator lowering
  - 而不是依赖 phrase-specific status rule

5. binder 必须采用 hybrid retrieval，而不是单一近似策略
   - 不能只做字符串匹配
   - 也不能只做向量最近邻
   - 正确形式应是：
     - lexical retrieval
     - dense retrieval
     - slot-role prior
     - ontology / mapping structural prior

可以表述为：

`candidate_score = lexical_score + semantic_score + slot_type_match_score + structural_prior`

其中：

- lexical score
  - 来自 label / local_name / comment / value text
- semantic score
  - 来自本地 embedding 检索
- slot-type match score
  - 来自 family 定义的槽位类型约束
- structural prior
  - 来自 domain/range、relation reachability、runtime executability

6. 最终选择的不是“每个槽位的单个最近对象”，而是合法 binding 组合
   - binder 先为每个槽位召回 top-k candidates
   - planner 再组合这些 candidates
   - 只有能组成合法 node-based plan 的 binding 才能存活
   - 所以最终选中的不是“某个最近 class”，而是：
     - 一组 mutually compatible bindings
     - 加上它们对应的 executable plan

7. ontology-first 仍是第一原则
   - ontology 是语义真源
   - manifest 只是 ontology / mapping / value catalog 的编译视图
   - 如果 ontology 已充分形式化某个概念，应优先直接命中 ontology class/property
   - 只有语义主要落在属性值或实例值域时，binder 才更多依赖 `ValueNode`

### 4.3.4.1.1 Utterance Decomposer / Conversation State / Execution DAG

仅靠 `family -> slot schema -> binder -> plan` 还不足以支持真实用户输入，
因为用户很可能：

- 一次输入多个问题
- 在同一句里混合：
  - 查对象
  - 判断状态
  - 解释原因
  - 给解决方案
- 在后一句里引用：
  - 这个
  - 这些
  - 上面的客户
  - 如果有
  - 分别是什么

因此在 planner 之上还需要增加一个独立的 Language Intent Parser 层：

- 它不负责 ontology grounding
- 它只负责语言控制解析
- 它的 deterministic lexical 只能服务于 minimal language-agnostic surface normalization：
  - Unicode / 标点 / 空白归一化
  - 数字字面量
  - 符号比较器
  - identifier-like literal / URI / local-name 形态
  - span / offset 保留
- 分句、条件依赖、引用解析不应再被写成 core lexical 规则能力，
  而应主要由 LLM-first parser 负责
- 它不能再用领域语义词表去直接决定：
  - `lookup / explain / remediation`
  - `family routing`
  - `target / status / cause` 的 ontology 绑定
- 如果 parser 需要识别 `lookup / explain / remediation`
  - 也应来自通用 question-act grammar 或更强的语言意图模型
  - 而不是来自 ontology/domain phrase table

这个边界是四原则的核心：

- parser core 只允许最小的 language-agnostic surface normalization
- 语言理解主能力必须由 LLM-first parser 承担
- 业务词表不能成为语义决策真源
- 领域语义必须通过 grounding 进入 `Intent IR` 与 planner 主链

1. Utterance Decomposer
   - 先把一整段输入拆成 `QuestionUnit[]`
   - 这里的拆分不是简单按标点，而是要识别：
     - 顺序问题
     - 条件依赖
     - 并列意图
     - 承接省略
   - 例如：
     - `13800138004是否低满意度？如果有，原因是什么？有什么解决方案？`
   - 应拆成至少三个 units：
     - `q1 = status_check`
     - `q2 = explain`
     - `q3 = remediation`
   - 其中：
     - `q2`、`q3` 依赖 `q1`

2. Intent IR
   - `family` 仍然需要保留，但不应直接承担整句语义
   - 对每个 `QuestionUnit`，应先抽成更高层的 `Intent IR`
   - `Intent IR` 至少应表达：
     - `focus`
     - `operators`
     - `constraints`
     - `output`
     - `references`
   - 这里的 `operators` 应是通用语义算子，而不是业务词：
     - `lookup`
     - `enumerate`
     - `status_check`
     - `explain`
     - `remediation`
     - `relation_discovery`
   - 只有在 `Intent IR` 之后，才将其映射到较低层的 `query family`
   - 在实现层，`Intent IR` 与 planner 之间还应有一个轻量 `intent policy`
     - 这是一个统一的决策层，不负责 grounding，只负责：
       - `focus_kind`
       - `operators`
       - `scope_inheritance_allowed`
       - `semantic_inheritance_allowed`
       - `reference_binding_allowed`
     - 它的目的不是增加新语义，而是让：
       - context inheritance
       - reference binding
       - family routing
       共享同一组判定，而不是分别回头读 raw slots / regex flags
   - 这里还要补一条硬约束：
     - `Intent IR` 的生成首先应由 language intent parser 提供
     - parser 可以输出：
       - `operators`
       - `references`
       - `dependency`
       - `comparators`
       - `anchor forms`
     - 但不应再靠领域语义词表直接输出：
       - `role = score / name / description`
       - `solution-like / explanation-like class`
       - `status-like attribute`
     - 这些都应交给 grounding 层处理

3. Conversation State
   - 系统必须维护一个轻量的会话语义状态，而不是把每一句都当成全新问题
   - 会话状态至少应包含：
     - 上一轮 `Intent IR`
     - 上一轮结果集锚点
       - entities
       - evidence
       - grouping key
     - 当前焦点对象
     - 当前焦点集合
     - 当前输出粒度
     - 可继承约束
   - 这样：
     - `因为网络问题，哪些客户投诉了？`
       先得到一个客户集合
     - 下一句 `这些客户的原因分别是什么？`
       不应重新从全库猜主题
       而应解析为：
       - `focus = previous_result_set`
       - `operator = explain + enumerate`
       - `target = cause`

4. Reference Resolver
   - 在 `Intent IR` 与 `family` 之间，需要一个专门的引用解析层
   - 它负责解析：
     - `这个`
     - `这些`
     - `它`
     - `他们`
     - `上述客户`
     - `如果有`
     - `其中`
     - `分别`
   - 引用解析后的结果应显式写入 `Intent IR.references`

5. Execution DAG
   - 多个 `QuestionUnit` 不应线性硬跑，也不应一股脑并发
   - 应先形成一个带依赖关系的执行 DAG
   - 每个 unit 都可以声明：
     - `depends_on`
     - `condition`
   - 例如：
     - `q2` 只有在 `q1 = true / non-empty` 时才执行
   - 这能避免：
     - 在前置问题失败时继续浪费预算
     - 让 Agent 用自然语言自己记状态

6. Multi-Answer Presentation
   - 多子问题执行后的原始结果，不应直接混成一段自由文本
   - 应先形成：
     - `QuestionBatchResult`
     - 每个 `unit` 各自的：
       - status
       - result
       - skipped / blocked reason
   - 最后再由 presentation 统一决定：
     - 合并为一段
     - 分点
     - 省略未执行项

当前实现状态（2026-03-27）：

- 已实现第一版：
  - `Utterance Decomposer`
  - `QuestionUnit[]`
  - `Intent IR`
  - 轻量 `Conversation State`
  - `Execution DAG`
  - `question-batch-template` / `batch_executed` 输出
  - 执行期按真实前序结果重规划后续 unit，而不是复用 planning-only 的假上下文
  - `resolved references` 会显式写入：
    - `resolved_slots`
    - `Intent IR.references.resolved`
    - `conversation state`
  - 对同类 source 的上一轮结果集，已支持第一版 `source scope` 下推
  - `Intent IR` 之上已落第一版 `intent policy`
    - 现在 `context inheritance` / `semantic inheritance` / `family reroute`
      已开始共用这层策略，而不是分散地直接读取 raw slot flags
  - planner 入口现在在缺少显式 `unit_intent_ir` 时，也会先合成单 unit 的 `Intent IR`
    - `derive_intent_profile` / `route_query_family` 优先读取 `Intent IR`
    - raw slot 仅保留为 bootstrap fallback，而不再作为默认决策真源
  - `slot_inputs` / `request_ir.constraints` / `request_ir.references`
    也已开始优先消费 `Intent IR`
    - raw slot 仍可作为缺省回退
    - 但不再作为这几层的首选语义来源
  - `evidence candidate scoring` 中的 `cause/action/status` 词项来源
    也已开始优先消费 `Intent IR`
    - 继续减少 raw slot 对 candidate planning 的主导作用
  - explanation-style planning 中的 `asks_explanation` / `target_text`
    也已开始优先消费 `Intent IR.operators / Intent IR.constraints`
    - raw slot 仅保留为 bootstrap fallback
  - `conversation state` 中的 `has_anchor / status_check_requested / asks_solution / asks_explanation`
    也已开始优先由 `Intent IR.focus / Intent IR.operators / Intent IR.constraints` 派生
    - 减少 raw slot 状态位在跨 unit 继承中的主导作用
  - `merge_inherited_slots` 也已开始优先读取上一轮 `intent_ir`
    - 语义继承优先来自 `Intent IR.constraints / Intent IR.operators`
    - top-level raw slot 字段仅保留为 fallback
  - `status_check / anchor` 这类控制位在 `routing / slot_inputs / request_ir`
    中也已开始优先由 `Intent IR` 决定
    - 继续压缩 raw slot 在 planner 控制流中的作用面
  - planner 内部已新增一层共享的 `semantic state normalization`
    - 统一从 `Intent IR + raw slot fallback` 归一化：
      `anchors / cause / action / status / target / result_hint / asks_explanation / asks_solution / status_check`
    - `merge_inherited_slots / build_family_slot_inputs / route_query_family / build_semantic_request_ir / build_conversation_state_entry`
      已开始共用这层归一化，而不是各自重复实现一套 `Intent IR 优先 + raw slot 回退`
    - 这样后续继续削弱 bootstrap heuristic 时，可以收敛到单点，而不是在 planner 多层散改
  - `extract_question_slots` 已开始向 `bootstrap candidate extractor` 收敛
    - 现在会先记录 `bootstrap_candidates`
    - 再由 `build_question_unit_intent_ir` 优先消费这些 candidates 来构造 `Intent IR.constraints`
    - top-level `cause_text / action_text / status_or_problem_text / target_text / result_hint`
      目前仅保留为兼容字段，不再是唯一入口语义来源
  - 已新增共享的 `bootstrap intent view`
    - `build_question_unit_intent_ir` 与 `derive_intent_profile`
      现在共用这层 bootstrap 结构，而不是各自重复推 `operators / focus / constraints / references`
    - 这使得入口层从“raw slot 直接推语义”进一步收敛到
      `bootstrap candidates -> bootstrap intent view -> Intent IR / intent profile`
  - `build_question_unit_intent_ir` / `derive_intent_profile` / `route_query_family`
    现在开始共用一份 canonical `intent policy`
    - 同一份策略会统一给出：
      - `focus / operators / references / output`
      - `scope_inheritance_allowed / semantic_inheritance_allowed / reference_binding_allowed`
      - `family_bias / effective_template_bias`
    - 这使 IR 构造、family routing、context inheritance 不再各自重算一套近似判定
    - routing 所需的 `has_target_constraint / has_status_constraints / has_causal_constraints`
      也开始优先从 canonical `constraints` 快照读取
      而不是继续直接吃 bootstrap top-level semantic text
  - `build_semantic_request_ir` 现在开始消费一层显式 `grounded constraint view`
    - 这层会把 `requested_text` 与 manifest binding 后的 `effective_text / binding_terms / top_candidate`
      分开表达
    - 目标是把 `request_ir / candidate plan term generation / underconstrained gating`
      从 raw semantic text 拼装，推进到 “requested semantics + grounded semantics” 双轨输入
  - `build_family_slot_inputs` 也开始优先读取 canonical `constraints` 快照
    - lowering 前的 `target / cause / action / status` slot text
      不再继续直接依赖 bootstrap top-level semantic text
  - `derive_intent_profile` 已开始承担第一版 `intent policy`
    - 现在 family bias / effective template bias / family rationale
      已在 profile 层集中生成
    - `route_query_family` 不再就地重算一套 explanation / causal / target 布尔组合
      而是消费这份集中策略
  - planner 已切断对 top-level 物化语义字段的核心依赖
    - 语义文本现在只从 `Intent IR.constraints` 或 `bootstrap_candidates` 进入主路径
    - `slots.cause_text / action_text / status_or_problem_text / target_text / result_hint`
      已不再是 planner 的语义真源
    - 继承语义也会回写为 `bootstrap_candidates`，而不是只靠 top-level 兼容字段传递
  - `extract_question_slots` 现在已不再物化顶层语义文本
    - 输入阶段只保留：
      `anchors / status_numeric_constraint / bootstrap_candidates / bootstrap_operator_hints / bootstrap_signals`
    - 顶层 `cause_text / action_text / status_or_problem_text / target_text / result_hint`
      已从问题抽取入口移除
    - 词法层现在开始显式收敛到 `collect_lexical_bootstrap_recall`
      - 它负责 recall / normalization
      - 不再直接承担 planner 决策职责
      - recall 规则现在按有序 pipeline 执行，而不是散落在一个大函数里的 if/regex 分支
      - 词法资源与 recall 规则现已独立到 `obda_lexical.py`
      - planner core (`obda_api.py`) 只消费 lexical adapter 接口，不再直接声明这些词表/regex
      - 但这仍然只是过渡态；长期目标不是保留一个“更干净的 lexical recall 模块”
        而是把它继续收敛成独立的 language intent parser
      - 长期允许保留的 deterministic lexical 仅限 minimal language-agnostic surface normalization：
        - Unicode / 标点 / 空白归一化
        - 数字字面量
        - 符号比较器
        - identifier-like literal / URI / local-name 形态
        - span / offset 保留
      - 长期不允许保留的 lexical 是领域语义词表：
        - role patterns
        - solution/explanation term tables
        - semantic label suffix tables
      - 否则即使它们被挪出 planner core，也仍然违反四原则
  - `conversation_state` 现在也显式保存 `bootstrap_candidates`
    - 这样 follow-up / inheritance / re-planning 都能继续基于同一份 bootstrap 语义候选，而不是回退到旧式顶层字段
  - 顶层兼容布尔位也已继续退场
    - `extract_question_slots` 不再物化顶层 `asks_solution / asks_explanation / status_check_requested`
    - 控制位主链现在统一落在 `bootstrap_operator_hints`
    - `bootstrap_signals` 仅作为兼容输出保留，不再是语义决策的主输入
    - `semantic_state_from_sources` / `bootstrap intent view` / `Intent IR` 会优先消费 operator hints，而不是直接读取兼容布尔位
  - `conversation_state` 也已不再保存顶层兼容语义文本或兼容布尔位
    - 不再持久化 `cause_text / action_text / status_or_problem_text / target_text / result_hint`
    - 不再持久化顶层 `asks_solution / asks_explanation / status_check_requested`
    - 继续保留的主链只有：
      `anchors / bootstrap_candidates / bootstrap_operator_hints / bootstrap_signals / status_numeric_constraint / intent_ir / focus`
    - 这使 follow-up 继承真正依赖 `Intent IR + bootstrap_*`，而不是重新吃旧式物化 slot 字段
  - question-mode 执行态的 `planning_required` 现在也已收敛成明确的 fail-closed 契约
    - 正常执行态返回不再暴露可手工补写的 `plan_skeleton / required_fields`
    - 会显式返回：
      - `manual_fallback_allowed = false`
      - `planner_bundle_available_via_plan_only = true`
      - `next_action = stop_or_use_plan_only_for_debug`
      - `recovery_policy.mode = fail_closed`
    - 这一步的目的不是限制调试，而是阻断外层 agent 在普通 question-mode 下把 `planning_required` 误当成“继续手工补 SPARQL/sample”的信号
    - 如果 `clarification_hint.kind = explicit_metric_or_threshold_required`
      - 外层 agent 也不能擅自把抽象状态词重写成自己猜测的显式阈值问句
      - 显式 metric/threshold 只能来自用户原问题，或来自 planner 已经成功抽出的 numeric constraint
      - 否则应停下并要求更明确的重述，而不是通过 `/sample` 或手工 `sparql` 反推阈值
      - question-mode/batch 顶层也会把这类情况显式标成 `next_action = ask_user_for_clarification`
      - 并返回可直接转述给用户的 `user_clarification_prompt`
  - 已新增一条更强的通用 reroute 规则：
    - 单锚点 + 状态/评分判断 + 可选的解释/方案 follow-up
      即使外层误传成 `enumeration` / `causal_enumeration`
      也会优先收敛回 `anchored_causal_lookup -> causal_lookup`
    - `如果有，有什么解决方案` 这类条件后缀不会把单锚点状态题自动升级成结果集枚举
    - 对这类题，若抽象状态词仍无法通用 lowering，则返回 `planning_required`
      而不是执行错误的 `customer -> event contains("低满意度")` 空查询
  - 已增加两条通用上下文规则：
    - `explicit anchor resets context`
      - 后续 unit 一旦显式给出新锚点，只保留依赖条件，不再继承上一题的语义约束或引用绑定
    - `negative dependency branch resets positive constraints`
      - `empty_or_false` 分支只继承焦点，不继承前一步成立时才有意义的状态约束
  - 已补上一条独立的 `anchored_fact_lookup` 属性投影主线：
    - 对 `13800138004的满意度评分是多少` 这类锚点 + 显式属性查值问题，可直接生成可执行 plan
    - 不再要求外层 agent 先去 `/sample customerbehavior` 再手工补一条事实查询
  - 对 `reference explanation` follow-up：
    - 如 `这些客户的投诉原因分别是什么`
    - 当前会优先继承结果集 scope，而不是把上一题的 `cause/result_hint` 继续拖进当前 unit
- 已有稳定回归：
  - 单问题 `question-template`
  - `状态判断 -> 如果有 -> 解决方案` 的两段式 batch 输入
  - `状态判断 -> 原因 -> 解决方案` 的三段式 DAG 输入
  - `这个客户...` 的显式单实体引用
  - 显式新锚点覆盖旧上下文
  - `如果没有 ...` 的负条件分支
  - `这些客户...分别...` 的结果集引用
  - `empty_result -> skipped`
  - `planning_required -> skipped`
- 仍未完成：
  - 更强的 `Reference Resolver`
    - 目前仍主要依赖轻量 discourse marker + DAG dependency
  - 基于上一轮结果集的真正泛化 binding / lowering
    - 当前只支持第一版 source-scope pushdown，还没有 operator-level 结果集重写
  - 多 unit 之间跨 family 的结果集约束下推
    - 目前主要在 source 同类约束上生效，不代表任意 follow-up 都已泛化支持

7. 与四原则的一致性
   - 无特化：
     - 不为“如果有，原因是什么，再给方案”写特例路径
   - 通用性：
     - 通过 `QuestionUnit + Intent IR + Reference Resolver + DAG` 支持各种连问
   - 可扩展：
     - 新的 operator 可增量加入，而不是重写 template 体系
   - 无人工：
     - 不靠持续添加连问 regex / 特殊 prompt 规则维持覆盖

因此，后续系统的总顺序应升级为：

`utterance -> question units -> intent IR -> reference resolution -> family candidates -> manifest binding -> node plan -> lowering -> execution DAG -> multi-answer presentation`

这意味着：

- `family` 只是中层约束，不再是最高层理解入口
- 连问支持的核心不在 template patch，而在：
  - utterance decomposition
  - conversation state
  - execution DAG

### 4.3.4.2 Planner 必须遵守的四条原则

后续的 `Semantic Query Planner` 必须同时满足这四条原则：

1. 无特化
   - 不允许在 planner 核心里写 phrase-specific / case-specific 路径
   - 不允许把：
     - `低满意度`
     - `网络问题`
     - `投诉`
     - 任何 repo 当前高频问法
     写成专门的 planner 成功分支
   - 如果某类问题尚未被通用支持，正确行为是：
     - `planning_required`
     - `need_clarification`
     - 或一次严格受限的 recovery
   - 不允许制造“看起来能答”的虚假能力

2. 通用性
   - planner 的主轴必须是：
     - `family`
     - `slot schema`
     - `manifest binding`
     - `node-based plan`
     - `SPARQL lowering`
   - 不同问题类型应通过统一 family 机制分流，而不是通过堆积问句模式分支
   - 支持范围应表现为：
     - 某个 family 被通用支持
     - 而不是某几个具体问法刚好能命中

3. 可扩展
   - 新 ontology class / property / runtime mapping / value catalog 不应要求继续修改大量 planner 规则
   - 语义覆盖应主要来自：
     - ontology
     - mapping
     - runtime catalog
     - value catalog
   - planner 应优先扩展：
     - manifest
     - binder
     - lowering
     而不是扩展一串新的 hardcoded 问句逻辑

4. 无人工
   - 不允许依赖人工长期维护的业务 semantic profile / 词典 / 特判表
   - 不允许通过持续加 prompt、regex、heuristic 词表来承担主要语义覆盖工作
   - 人工允许做的事应只包括：
     - 改 ontology
     - 改 mapping
     - 改系统架构
   - 人工不应成为语义解析链条中的常驻依赖

这四条原则共同意味着：

- ontology / mapping / data 应自己提供语义空间
- planner 负责自动绑定与编译
- unsupported family 必须诚实失败
- 不再用“多写一点规则”来换取表面可用性

### 4.3.4.3 Bootstrap Heuristics 只允许作为过渡层

当前实现里仍有一些人工定义的 bootstrap 启发式，例如：

- language-specific surface patterns
- bounded lexical recall rules
- 少量历史遗留 regex / matcher

这些组件目前只允许被视为：

- bootstrap only
- 过渡期的 surface hint extraction
- 帮助系统从“完全不会分流”过渡到“有最小 parser schema / family slot schema”

但需要明确：

- 它们不是长期 contract
- 它们不应继续扩大为多语言 parser 的主能力
- 它们最多只能留在 parser adapter 的过渡层
- 长期目标仍然是：
  - minimal language-agnostic surface normalization
  - LLM-first intent parsing
  - repo-owned schema validation

但它们不能被当成目标架构的一部分。

明确约束：

- 不允许继续把新的业务语义覆盖建立在更多 regex / role-pattern 上
- 不允许把这些 heuristics 包装成“通用 semantic understanding”
- 后续应逐步降低它们在核心路径中的权重，把责任迁移到：
  - ontology-first manifest
  - 自动化 value catalog
  - binder retrieval
  - node-plan lowering

8. 低置信时必须 fail closed
   - 如果 top bindings 无法形成高置信合法 plan
   - 不应让 Agent 或 LLM 继续自由试探
   - 应返回：
     - `planning_required`
     - 或 `need_clarification`

### 4.3.5 当前实现已经出现的漂移

当前实现已经出现两个需要明确收回的漂移：

1. planner family 过窄
   - 实际主路径仍主要围绕 `causal_lookup / causal_enumeration`
   - 这会把本应属于 `anchored_status_lookup`、`anchored_fact_lookup` 的问题也硬塞进因果模板

2. grounding 过度依赖词法启发式
   - 当前一些效果改善来自：
     - label/local_name 字符串匹配
     - 受限词面扩展
     - template-specific relaxed widening
   - 这些可以作为过渡补丁，但不是最终架构

因此下一阶段的目标不应是继续扩展更多问题特定的词法规则，
而应是：

- 明确 query family router
- 引入 anchor-aware planning
- 将 grounding 从“词法启发式主导”升级为“语义召回 + 结构裁剪”

### 4.3.6 dbt/MetricFlow 风格的 Skill 边界

这里需要进一步借鉴 dbt 的做法。

对 dbt 而言：

- skill 不是主要的语义引擎
- skill 更像一个薄的 orchestration layer
- 真正负责“理解可查询语义对象并生成可执行计划”的，是 semantic layer / MetricFlow engine

对应到当前 repo，更合理的分工应当是：

1. Skill
   - 负责协议纪律：
     - schema-first
     - 选择最小 query family / template
     - 优先调用 `run`
     - 遵守 bounded recovery
     - 用 `presentation` 组织最终回答
   - 不负责：
     - 重新实现 grounding
     - 暴露 planner 内部 reroute / widening / matching 细节
     - 在文案层承诺某个具体问法会命中特定 planner 路径

2. Client / Semantic Query Planner
   - 负责真正的语义引擎职责：
     - semantic manifest / runtime catalog
     - semantic request IR
     - node-based plan
     - SPARQL lowering
     - validation
     - bounded fallback

3. Server
   - 继续只负责：
     - schema
     - structured query execution
     - analyzer execution
   - 不承载业务问句补丁

因此，skill 的正确设计目标不是“把 planner 解释得越来越细”，
而是：

- 让调用方知道该怎么正确使用系统
- 但不把 planner 的内部策略当成稳定外部契约

这也是为什么后续需要收紧 `obda-query` skill：

- 删除或淡化 phrase-specific planner 承诺
- 删除或淡化某个具体问法会怎样 reroute / widen 的示例
- 保留协议规则与失败纪律
- 把 planner 的真正细节放回设计文档与 client 实现，而不是 skill 文案

### 4.4 当前最该优化的方向

短期：

- 用 schema-first + 单主查询 + 单 batch analysis 收敛流程
- 尽量避免通过 `/sample` 重新发现 schema 已经给出的关系
- 避免把 `analysis-paths` 和 `analysis-paths-batch` 混用
- 把 SPARQL 生成从“自由文本生成”收紧到“受约束生成”

中期：

- 为高频 query family 沉淀稳定的 semantic request IR，而不是继续堆问句补丁
- 让主查询固定返回实体 URI 锚点列，供 batch analyzer 自动使用
- 增加 plan / lowering validator，在执行前就检查方向、谓词存在性、URI 锚点列

长期：

- 做真正的自然语言到 semantic request IR，再到 node-based query plan 的确定性 semantic query planner
- 减少 Claude 在运行时自己“猜 SPARQL / 猜 analyzer payload / 猜 source 列”

### 4.4.1 下一步明确实现项：Semantic Query Planner / Validator

下一阶段不再优先补提示词，而是实现一个更硬的查询生成层。

目标不是做业务写死模板，而是做“结构受约束、以 ontology 语义为第一真源”的 planner。
这里的重点已经不是继续扩展某个具体问法的 relaxed 规则，而是先把 planner 的 query family 与 grounding 入口做对。

建议输入槽位至少包括：

- target_text
- anchor_text
- cause_text
- action_or_state_text
- output_grain
- asks_explanation / asks_solution
- preferred URI anchor requirement

planner 不应直接从这些槽位跳到 raw 查询，而应先生成 semantic request IR，再由 node-based plan 与 lowerer 逐步落到可执行查询。

更具体地说，下一阶段的 planner 最低形态应包括：

- query family router
  - 先区分：
    - `anchored_fact_lookup`
    - `anchored_status_lookup`
    - `anchored_causal_lookup`
    - `enumeration`
    - `causal_enumeration`
    - `hidden_relation`

- anchor detector
  - 用确定性规则识别抽象锚点形态，例如：
    - `resource_uri`
    - `resource_local_name`
    - `structured_literal`
    - `identifier_like_literal`
  - 不再把 `phone / customer ID / workorder ID` 这类业务特定类型写成 planner 核心枚举
  - 这些更具体的语义应由 manifest binding 决定，而不是由 anchor detector 预设
  - 不再把强锚点识别埋在模板补丁中

- grounding
  - 先基于 ontology / mapping / value catalog 做候选召回
  - 再用结构约束裁剪
  - 逐步从纯词法匹配升级到本地语义召回

- semantic request IR builder
  - 将 query family、anchors、filters、output grain、analysis intent 组织成稳定 IR
  - 这是 planner 的真正输入，不应让 `shape` 或手写 query 草稿直接承担这层职责

- family-aware planner
  - 对不同 family 构建少量合法的 node-based plan
  - 例如：
    - `anchored_status_lookup`: `AnchorResolve -> EntityScan -> MetricFilter -> Project`
    - `anchored_causal_lookup`: `AnchorResolve -> EntityScan -> EvidenceTraverse -> AnalysisTraverse -> Project`
    - `causal_enumeration`: `EntitySetScan -> EvidenceFilter -> Aggregate/Group -> AnalyzerBatch`
  - 如果需要描述图遍历骨架，可保留 shape 作为 search hint，但不能把它当成最终 plan

- bounded fallback
  - 如果 planner 低置信或结构不合法，应 fail closed
  - 不允许重新把自由探索交回 Claude
  - 最多只允许 client 内部有限 widening 或一次受控恢复

这里要特别强调：

- 不是让 LLM 继续直接写 raw SPARQL
- 而是让 LLM 只负责抽取槽位
- 再由 semantic query planner 基于 ontology grounding 负责真正的 query 生成

对外部调用形态来说，长期应当把：

- `run "question" --template causal_enumeration`

从“planning-only 入口”演进成：

- “semantic query planner 入口”

也就是说，问句 + template 最终应能直接落成可执行计划，而不是再让 Claude 在中间层自由补一遍 builder/SPARQL。

至少需要校验：

- 谓词是否存在于 `/schema`
- object property 方向是否匹配 domain / range
- 主查询是否同时编码了 cause constraint 和 action/state constraint
- `causal_enumeration` 是否返回至少一列 URI 锚点
- 返回列是否足够支撑最终 answer 和 batch analysis
- semantic request IR 是否完整，能否被 lowering 成合法 node-based plan

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

### 任务 4.6：实现 Semantic Query Planner / Validator

目标：

- 保留 query-first-then-analysis
- 不污染通用 Skill
- 提高首轮主查询命中率
- 明确 LLM / semantic query planner / ontology 的责任边界
- 不再让 LLM 直接写最终 SPARQL
- 不再让不同 query family 共用一套含糊的词法补丁路径

最低实现要求：

- 实现 query family router
- 实现 anchor-aware planning
- 将 `causal_enumeration` / `causal_lookup` 的主查询生成改为“槽位 -> semantic request IR -> node-based plan -> lowering”
- 在执行前做 schema 约束校验
- 强制返回 URI 锚点列
- 将恢复路径收敛为“一次定向 sample + 一次 rerun”
- 让问句中的 cause/action 等语义约束通过 planner 进入可执行计划，而不是留给 Claude 在执行时自由补写
- 对 `anchored_status_lookup` / `anchored_fact_lookup` 这类非枚举问题，提供独立的 query family 路径，而不是继续硬塞进 `causal_enumeration`

补充约束：

- 不再把 `shape` 作为 planner 的主表示
- `shape` 只能作为搜索空间提示或 plan 骨架约束
- planner 的真正核心产物应是 semantic request IR 与 node-based plan
- 当前 repo 的 lowering 目标首先是 `SPARQL`，不是在 client 层直接谈 `SQL lowering`

这项任务是当前下一轮的优先级最高项，应作为后续恢复工作的直接起点。

第一阶段实现状态（已完成）：

- `run` 对 `causal_lookup` / `causal_enumeration` 增加了 `sparql.builder` 编译入口
- builder 会同时参考 `/schema` 与 `mapping.yaml` 推导运行时可查询关系，不再把两者混成一层
- planner / builder / raw-query validator 现在也会参考 `mapping.yaml` 中的运行时 data property catalog，不再只依赖 ontology schema 中显式声明的数据属性
- raw `sparql.query` 在 causal 模板下现在必须显式提供 `sparql.source_var`
- `empty_result` 会附带结构化 `recovery_hint`，把恢复路径收敛为“一次定向 sample + 一次 rerun”
- `run "question" --template ...` 现在会先经过 semantic query planner；高置信时已默认自动执行锁定计划，`--plan-only` 才只返回 planner bundle
- question 模式在高置信时已经能产出并执行真实的 `builder` / lowered plan，而不再只是空占位 skeleton
- planner 现在开始显式输出 `semantic request IR` 与 `node-based plan` 调试视图，作为后续取代 ad-hoc candidate 逻辑的过渡
- semantic manifest 现在已开始显式暴露 typed nodes（`ClassNode` / `AttributeNode` / `RelationNode`）视图
- planner 现在已有第一版 `family slot schema -> slot inputs -> slot bindings` 过渡链路，并会把 `slot_bindings` 放进 `request_ir`
- planner 现在已有第一版 bounded sample-derived `ValueNode` 目录，并把它接入 binder 作为值域候选来源
- planner 现在已把 `enumeration` 纳入同一条 semantic planner 主线，并支持第一版通用 `value-enumeration` lowering 与 `enumeration` presentation
- planner 现在已支持 generic `smaller_family_reroute`：当用户或 Agent 错把 explanation-style 问句塞进更大的 template（例如把“引发投诉的都有什么原因”塞进 `causal_enumeration`）时，router 会先把它收敛到独立的 `explanation_enumeration` family，再以 `enumeration` lowering 执行
- `explanation_enumeration` 当前的 lowering 不是依赖 target slot 直接命中某个“原因字段”，而是通过 generic explanation operator：
  - 从问句抽出现象文本，例如 `投诉`
  - 在 manifest 中绑定 action/support 语义
  - 用少量最强 support classes / support properties 做 source-support 限定
  - 再从 evidence class 的 `type/description` 这类 explanation-capable 属性投影值域
- 这个 family 仍然保留 fail-closed 边界：如果现象文本抽不出来，或 support binding 不足以形成可执行 lowering，就不能再退回旧的 role-fallback 硬凑答案
- 已将“手机号 + 低满意度 + 解决方案”这类 phrase-specific 特化路径从主流程撤出；当前 unsupported anchored family 会回到 `planning_required`，而不是继续伪装成已稳定支持

需要明确承认的偏差：

- 当前实现中仍保留一些 family 级 relaxed / fallback 逻辑，但 phrase-specific 特化路径已不再作为主流程契约
- 现阶段的目标不是“补出更多特化能力”，而是让 unsupported family 明确 fail closed，再逐步用 semantic request IR + node-based plan 的通用机制补上

当前仍未完成的部分：

- 还没有实现完整的 query family router
- 强锚点识别目前已具备第一版抽象 anchor 检测能力，能够识别 URI、ontology resource local name 以及部分 structured literal；但还没有形成完整的 manifest binding 与 anchored family 的通用 lowering
- anchored binder 目前已开始基于抽象 anchor kind 与 attribute capability 做第一版绑定，但 anchored family 仍未具备通用 lowering，因此仍应保持 fail closed
- source 选择已开始吸收“anchor 绑定沿 manifest relation 的一跳传播”这一通用结构先验；也就是说，当锚点先绑定到某个 attribute/value node 所属 class 时，planner 会把与该 class 一跳相连的候选 source class 也纳入排序，而不是只靠孤立的 attribute role hint
- 已补入第一版通用 `status_or_problem_text` 抽取，用于 `是否存在...情况 / 有无...` 这类 anchored 问句的抽象槽位识别
- 已把 `status_or_problem_text` 区分为 `problem_text` 与 `status_check` 两种约束模式；当其属于 `status_check` 时，binder 只允许绑定到 `status-like / score-like` manifest nodes
- 当前 planner 已禁止把 `status_check` 约束错误地降成任意文本 evidence filter；如果只绑定到了 numeric score/status 属性而尚无通用 comparator lowering，则必须 fail closed
- 已实现第一版通用 numeric constraint IR 与 comparator lowering；像 `满意度评分低于3` 这类显式数值约束已不再需要 phrase-specific planner path
- 当前 question-mode 的 gating 已收紧：对于 anchored family，如果约束文本已抽出但 binder 仍无法把该约束绑定到 manifest node，则必须返回 `planning_required`，不能继续自动执行一个低置信空查询
- Value catalog 的 bounded sampling 现在按 `source/evidence/slot binding` 的综合相关度排序，而不是固定按类顺序截断，避免 exact-value anchor 因采样预算顺序而丢失
- planner 已从“只尝试单一 source class”收敛到“在少量 source candidates 上做有界 plan 搜索，再选择第一个高置信可执行 plan”；这比继续调某一个 source heuristic 更符合通用架构
- builder 已支持 `optional display selects`；planner 现在会把非过滤、非排序必需的展示字段标记为可选，避免稀疏 display property 把本可执行的 plan 查空
- `cause_text / action_or_state_text` 这类自由文本槽位现在默认排除 numeric manifest nodes，避免把样本数值误当成语义约束词
- `binding_terms_for_slot()` 已收紧为只吸收高置信 binding term，避免低分弱候选直接污染主查询过滤词
- presentation 层现在会把 row-level metric vars（如 `statusMetric`）稳定汇入 `key_metrics / entity_metrics`，不再因为它们不带 `source*/evidence*` 前缀而在最终回答中丢失
- presentation 已开始把 `"None" / "null" / "nan"` 这类缺失字面量视为 absent，而不是直接当成展示标签
- 属性角色选择已开始使用“generic role hint + class semantic alignment”而不是单纯依赖 `客户ID / 事件ID / 工单ID` 这类类特定词；这使 `customerbehavior_客户行为ID` 这类自标识能自然胜过外键式 `客户ID`
- 已实现第一版 `family slot schema + hybrid binder`，但还没有完成真正的自然语言槽位 grounder
- 已实现第一版独立的 semantic request IR builder，但目前仍主要用于 planner 调试视图和过渡态输出，尚未完全成为 lowering 的唯一输入
- 已开始把 planner 的核心表示从 query shape / ad-hoc candidate 迁移到 node-based plan；目前 lowering 已覆盖 `causal_*` 与第一版 `enumeration/value-enumeration`，但仍未完全由统一 IR 驱动
- manifest 目前已有 typed nodes，并已开始接入 bounded sample-derived `ValueNode` / value catalog
- 已实现 repo-owned grounding contract（grounding bundle），并把 `slot_inputs / slot_bindings / grounded_constraints` 收敛为 planner/request IR 共享的正式接口
- 已开始把 Grounder 的候选资格与传播规则收成独立 policy 层
  - slot admissibility
  - sample-value eligibility
  - abstract status high-confidence gating
  - relation propagation eligibility
  - grounded candidate projection helpers
- source/evidence/projection 选择现在已开始显式走 manifest-first：
  - source class 初选优先吃结构信号，词面只保留 tie-break
  - evidence class 选择优先吃 relation / grounded slot / 属性族兼容性
  - projection / detail-fetch 已开始共享 manifest-aware property profile
- bounded sample-derived `ValueNode` 现在已经显式带 provenance：
  - sample value nodes 会标成低信任 `catalog_source`
  - planner 只在确实需要 literal recall 的槽位才按需加载 sample catalog
  - detail-fetch / projection 辅助链现在也改成 manifest-only first
  - 只有当 property profile 明显太弱、无法稳定支撑展示字段选择时，才会加载一个很小的 bounded sample slice
  - sample-derived value nodes 现在还必须通过更强的 literal-support 门槛才允许进入 binding 主竞争
    - 对 `anchor_text` 基本要求 exact literal match
    - 对 `cause/action/target` 至少要求 exact/overlap literal support，或足够强的 lexical + semantic 支撑
  - sample/value catalog 现在更接近 recall 辅助，而不再是默认的 binding 决策真源
- binder 目前已从纯 lexical 提升到 `lexical + local hashed-vector retrieval + slot-role prior + 结构先验` 的过渡实现
- 当前的 local vector retrieval 是 dependency-light 的 hashed subword / token vector，不应误称为真正的 dense embedding retrieval
- 当前应明确区分两类 lexical：
  - 允许长期保留的：
    - language-agnostic surface normalization
    - 数字字面量
    - 符号比较器
    - identifier-like literal / URI / local-name 形态
    - span / offset 保留
    - 它们的职责只是提供最小 surface hints
  - 不应继续保留的 domain-semantic lexical：
    - `ROLE_PATTERNS`
    - solution / explanation / semantic suffix 词表
    - 任何直接把词面映射成 ontology role / class / property 的硬编码表
- 也就是说，最终目标不是“零 lexical”，而是：
  - `zero domain-semantic lexical in the decision path`
  - `minimal language-agnostic deterministic surface layer`
  - `LLM-first parser for language understanding`
- 当前代码中仍有 language-specific lexical 过渡实现，但不应把它们误写成长期架构目标
- `enumeration` 当前虽然已可通过通用 value-enumeration plan 执行，但 target/value property 的选择仍带有 role fallback（如 `type/description`），这应继续收敛到更强的 manifest binding，而不是扩展新的问句特化
- explanation-style `enumeration` 现在已有第一版独立的 generic family / semantic contract：`explanation_enumeration`
  - 当前已支持从更大的模板收敛到 explanation operator，再 lowering 成 generic value-enumeration
  - 但这个 family 仍然依赖 bootstrap 级的现象抽取与 support-property 排序，距离完全 ontology-first / no-manual 的目标态还有差距
  - 当现象文本抽取失败，或 support binding 仍不足以形成可执行 lowering 时，正确行为依然是 fail closed
- 还没有把“cause constraint / action constraint 是否同时编码”做成更高层语义校验
- ontology-first grounding 虽已开始独立成层，但 binding 质量仍处在过渡态：
  - 目前更像 `manifest-first + lexical/local-vector recall + bounded sample recall`
  - 还没有完全升级成更强的统一 semantic grounding engine
- 实例值层目前仍主要是 bounded sample-derived `ValueNode`
  - 只是它们现在已被收紧为 on-demand、low-trust 的 literal recall 辅助
  - 还没有升级成更完整的自动化 value catalog
- model-backed parser 虽已接入，而且 parser facade 已支持 utterance-level model parse + unit projection
  - 当前已经是 default-path-capable：
    - parser 会按环境自动选择 `MockModelBackend` / `OpenAICompatibleBackend` / `AnthropicCompatibleCliBackend`
    - 使用 `claude` CLI 作为 host，并通过 `ANTHROPIC_BASE_URL / ANTHROPIC_API_KEY` 指向 Kimi Code 时
    - 真实 Claude Code + Kimi Code 宿主链已能稳定跑通 host E2E regression
  - 但它仍然保留 conservative fallback
    - 这不是 provider 绑定特化，而是执行系统的通用 fail-closed 边界
  - 当前 conservative fallback 已具备明确契约：
    - 保留 deterministic surface decomposition
    - 保留 multi-unit dependency DAG
    - 避免在 utterance-level backend failure 后再对每个 unit 重复等待模型超时
- 当前 backend 还具备更强的稳定化保护：
    - parse failure 支持有界重试
    - utterance-level `question_units` 与 deterministic hint 数量不一致时，会触发 `backend_projection_mismatch`
    - mismatch 后会整体回落到 deterministic unit bundles，而不是把错误的 utterance parse 继续投影进 planner
    - utterance-level model parse 即使返回了正确的 unit 数量，如果某些 unit 缺少 `question_acts / surface_constraints` 这类最小结构，也会触发 `backend_underfilled_parse`
    - underfilled parse 同样会整体回落到 deterministic unit bundles，而不是把“结构上过弱”的 multi-unit parse 继续投影进 planner
  - utterance-level parser output 的标准化现在也开始显式吃 `question_units_hint`
    - 也就是说，multi-unit parse 在 normalize 阶段会优先保留 repo-owned decomposition 给出的 `unit_id / raw_text / normalized_text / reference markers`
    - 而不是再把整句 fallback 粗暴地回灌到每个 unit 上
    - 这条 hint-preserving normalize 现在也会保住 follow-up dependency
      - 即使 model parse 漏掉了 `dependency`
      - 只要 deterministic decomposition 已识别出 `如果有 / 这些 / 分别` 这类依赖关系，unit-level parser output 仍会继承它
- 已补独立的 parser regression 面
  - 现在除了 question-mode 端到端回归，还有 parser contract regression
  - 至少覆盖：
    - deterministic numeric/status parse
    - deterministic multi-unit decomposition
    - reference marker preservation
    - Anthropic-compatible CLI backend unavailable / conservative fallback contract
    - Anthropic-compatible CLI backend success / hybrid unit projection
    - Anthropic-compatible CLI backend projection mismatch / conservative fallback contract
    - Anthropic-compatible CLI backend underfilled parse / conservative fallback contract
    - Anthropic-compatible CLI backend schema->json-only fallback contract
    - Mock backend success / hybrid unit projection
    - OpenAI-compatible backend success / hybrid unit projection
    - protocol-marker-based backend autodetect
- 已补独立的 grounding regression 面
  - 不再只依赖 question-mode 端到端回归来间接覆盖 grounding
  - 当前至少覆盖：
    - sample-derived value admissibility
    - abstract status candidate gating
    - literal-overlap support for cause/target recall
- 2026-04-08 这一轮又补了三条对“口语因果枚举”更关键、且仍保持通用性的收口：
  - parser 侧：
    - `有哪些客户因为5G信号差而投诉了` 这类问句现在会稳定拆成
      - `result_hint=客户`
      - `cause_text=5G信号差`
      - `action_text=投诉`
    - 这是 generic causal surface split，不是 telecom 词表特化
  - property-profile 侧：
    - schema/runtime 属性合并现在不会再让 runtime 的空值覆盖 schema 的有效元数据
    - 这避免了 `event_发生时间` 这类 temporal 字段被误降成普通 text，从而错当 description/filter 列
  - sample-value recall / lowering 侧：
    - sample-derived value 现在允许通过 generic identifier-like fragment overlap 进入 recall
      - 例如 `5G信号差` 可以落到 `5G信号不稳定`
    - 对更短、更泛的口语 symptom phrase，grounded slot view 现在会在“弱 sample value 同分近邻”场景下保留 bounded `binding_terms`
      - 例如 `信号差` 不再只保留单个 top sample value
      - 而是可以一起携带 `信号弱，通话中断` / `5G信号不稳定` 这类同域近邻值进入 cause lowering
      - 这依然是通用机制：按同 slot、同 class、同 property、同分近邻收束，而不是写死 telecom 词表
    - 但弱的 action grounding（例如只靠低分 sample value 命中的 `投诉 -> 服务投诉`）不会再被自动当成 strict filter
    - 也就是说，系统现在会区分：
      - 可用于 cause recall 的弱近似命中
      - 不可直接当成 hard constraint 的弱 action lowering
    - 这条边界本质上是“弱 grounding 不应升级成强执行约束”，属于通用 planner discipline，不是业务特例
  - low-information attribute guard 侧：
    - free-text `cause_text / action_or_state_text` 现在会显式排除 key-like attribute
      - 例如 `网络ID / 客户ID / 编码 / 标识 / 序号` 这类字段
    - 这解决了抽象口语症状被 manifest attribute 误绑的问题
      - `网络不好` 之前会被错误绑定到 `remediationstrategy_网络ID`
      - 现在会优先回到 event sample values，再由 bounded multi-candidate recall 生成 `5G网络问题 / 网络覆盖问题 / 网络质量投诉`
    - 这层仍然是通用性约束：限制低信息 key-field 进入 free-text semantic slot，而不是给某个行业写词表特判
  - evidence-class ranking 侧：
    - 弱 sample-value top candidate 不再自动给对应 class 满额 bonus
      - 如果只是低分 sample hit，它只能拿 candidate-level bonus，不能把整条 evidence 主链强行拉偏
    - causal evidence ranking 现在会偏好真正具备 `type + description` 文本角色的 class
      - 并压低 `客户ID / 手机号 / 联系方式` 这类低信息字段冒充证据 role 的情况
    - 这让 `因为网络问题，哪些客户投诉了？` 这类旧主路径稳定回到 `customer -> event`
      - 而不是被 `workorder_工单内容` 或 `customerbehavior_手机号` 这类弱文本字段抢走
- 因此像 `有哪些客户因为5G信号差而投诉了?` 这类问题，当前已经能从：
  - `planning_required`
  - 或 `empty_result`
  稳定提升到：
  - `customer -> event` 主链
  - `event_事件类型 / event_事件描述` 文本过滤
  - 并返回稳定结果（当前样本中命中 `CUST006 / EVT007 / 5G信号不稳定`）
- 同一套 generic grounding 现在也覆盖更泛的 `有哪些客户因为信号差而投诉了?`
  - 当前样本中会稳定返回：
    - `CUST002 / EVT003 / 信号弱，通话中断`
    - `CUST006 / EVT007 / 5G信号不稳定`
  - 这说明系统已经从“只能吃强 literal”推进到“能处理短口语 symptom phrase 的 bounded multi-candidate recall”
- 再往前一步，抽象口语症状 `有哪些客户因为网络不好而投诉了?` 当前也已可执行
  - 当前样本中稳定返回：
    - `CUST002 / EVT001 / 网络质量投诉 / 用户反映网速慢，视频卡顿`
    - `CUST002 / EVT003 / 网络覆盖问题 / 信号弱，通话中断`
    - `CUST006 / EVT007 / 5G网络问题 / 5G信号不稳定`
  - 这说明系统已经不只是“吃到相似 symptom phrase”
    - 还开始能处理更抽象的 quality complaint phrasing
    - 且靠的是 generic slot admissibility + bounded value recall，而不是业务 hardcode
- 同时，原始主路径 `因为网络问题，哪些客户投诉了？` 已恢复为 `customer -> event`
  - 说明这轮增强不是“为了补新口语而牺牲旧主链”
  - 而是把 planner 进一步推向：
    - free-text slot 先过滤低信息 binding
    - 弱 sample hit 只做 bounded recall，不直接统治 class ranking
    - evidence class 由结构化文本角色来决定主证据优先级
- Grounder policy 已经从 `obda_api.py` 中收口到独立模块
  - `slot admissibility / sample eligibility / relation propagation / grounded candidate projection`
    这些规则现在以 repo-owned policy 形式存在
  - `obda_api.py` 主链已不再保留同一套本地重复实现
- Planner compiler helper 已开始独立化
  - `request IR` 装配、`node_plan` 构建、compiled-plan selection、planner summary
    已从 `obda_api.py` 主体中收口到独立 compiler module
  - planner 继续朝“只消费 Intent IR + Grounding Bundle + Request IR”的编译器方向演进
- `build_semantic_query_planner(...)` 的当前执行主路径已切到独立 runtime
  - `obda_api.py` 现在通过 thin wrapper 注入 helper/runtime callback
  - legacy shim 与主 planner wrapper 现在共用 `_semantic_planner_runtime_callbacks()`
    - semantic planner runtime 的 callback wiring 不再在两处重复维护
  - planner 的候选 plan 搜索、enumeration/value-enumeration lowering、compiled-plan 选择
    当前已由 repo-owned semantic planner runtime 承担
  - `obda_api.py` 里的 legacy planner 入口现在也已显式收成 compatibility shim
    - 当前即使经过 legacy 入口，也会直接委托给 semantic planner runtime
    - 因此 legacy planner 已不再拥有独立执行语义
    - `obda_api.py` 中原先那段 dead legacy planner 体已物理删除
    - 当前剩余的 legacy planner 只是一层显式 shim，不再保留第二套 planner 主体
- explanation-style follow-up enumeration 当前已收紧为“value projection first”
  - explanation/result-set reference follow-up 的 `enumeration` lowering 不再默认附带 `paths` analysis
  - 当主答案可由 value-enumeration 直接给出时，应返回 `ok`
  - 不再因为缺少 source anchor 而把本来可回答的 follow-up 降成 `partial_success`
- 已补独立的 planner regression 面
  - 不再只依赖 question-mode 回归来间接观察 planner
  - 当前至少覆盖：
    - family routing / effective template
    - plan executable vs planning_required
    - fail-closed reason
    - batch dependency DAG
  - question-mode 端到端回归当前也已重新确认保持全绿（24 cases）
- 因此当前系统已经明显接近目标架构，但仍不能把它误写成“LLM-first parser + ontology-first grounding 已完全落地”

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
如何在不污染通用 Skill 的前提下，为 causal_enumeration 增加受约束的 semantic query planner / validator，
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
