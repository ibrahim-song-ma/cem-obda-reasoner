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
- 让 Skill 只表达意图，不处理图搜索细节

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
