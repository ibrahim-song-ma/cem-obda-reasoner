# 本体推理原型项目状态报告

**生成日期**: 2026-03-29
**基于设计文档**: `.claude/plans/cozy-imagining-canyon.md`

---

## 核心设计架构

### 1. 四层分离架构

```
用户问题
  -> Agent/Skill（自然语言理解、协议纪律、结果解释）
     - schema-first 查询
     - 选择最小 query family / template
     - 优先调用 `run`
     - 遵守 bounded recovery
     - 用 `presentation` 组织最终回答
  -> Client/Semantic Query Planner（SPARQL生成、语义grounding、校验）
     - semantic manifest / runtime catalog
     - semantic request IR
     - node-based plan
     - SPARQL lowering
     - validation
     - bounded fallback
  -> Reasoning Service
      - 从DuckDB物化RDF
      - 加载ontology
      - 执行OWL RL闭包推理
      - 提供 /sparql /schema /sample /reload /health
  -> Optional Analyzer Service
      - 通用路径分析
      - 因果路径解释
      - 推理新增关系检查
      - 邻域提取
```

#### 各层职责

| 层级 | 核心组件 | 职责 | 不做的职责 |
|------|----------|------|------------|
| **Agent/Skill层** | `obda-query` skill | 协议纪律：schema-first、选择最小template、调用run、bounded recovery、结果展示 | 不重新实现grounding、不暴露planner内部细节 |
| **Client/Planner层** | `obda_api.py` 等 | SPARQL生成、semantic manifest、semantic request IR、node-based plan、validation | 不写客户特定逻辑 |
| **数据层** | DuckDB | 存储原始业务数据、link tables | - |
| **语义映射层** | `mapping.yaml` + Morph-KGC | 将关系数据映射为RDF事实，使用ontology词汇 | 不写业务推理代码 |
| **推理与查询层** | `reasoning_server.py` | RDF物化、OWL RL推理、SPARQL执行 | 不写客户特定DFS逻辑 |
| **分析与解释层** | `/analysis/*` endpoints | 对推理后图做解释、生成可读路径 | 不是OWL推理本身 |

### 2. Semantic Query Planner 三层流水线

```
Utterance
  -> Language Intent Parser
       - 分句 (Utterance Decomposer)
       - 意图解析 (Intent IR)
       - 引用解析 (Reference Resolver)
  -> Ontology Grounding Layer
       - 锚点绑定
       - 类/属性/值候选召回
       - 结构裁剪
  -> Planner / Lowering / Validator
       - Family路由
       - Semantic Request IR构建
       - Node-based Plan构建
       - SPARQL lowering
       - 执行前校验
```

#### 2.1 Parser层

**输入**: 原始用户utterance
**输出**: `QuestionUnit[]` + `Intent IR`

**核心组件**:
- `Utterance Decomposer`: 将输入拆分为多个QuestionUnit，识别依赖关系
- `Intent IR`: 包含focus、operators、constraints、references、output
- `Reference Resolver`: 解析"这个"、"这些"、"上述"等指代

**设计原则**:
- Parser只产出语言结构，不产出ontology绑定
- 允许输出ambiguity/confidence，可以要求澄清
- 不能越权替grounder绑定ontology或替planner生成plan

#### 2.2 Grounding层

**输入**: `Intent IR` + semantic manifest + value catalog
**输出**: `grounded constraints` + candidate bindings

**核心机制**:
- **Semantic Manifest**: 从ontology/mapping编译的typed nodes视图
  - `ClassNode`: 类节点
  - `AttributeNode`: 属性节点
  - `RelationNode`: 关系节点
  - `ValueNode`: 值节点（来自bounded sampling）
- **Hybrid Binder**: 多策略候选召回
  - lexical retrieval (label/local_name匹配)
  - dense retrieval (hashed subword vector)
  - slot-type prior (family定义的槽位类型约束)
  - structural prior (domain/range结构裁剪)

**设计原则**:
- grounding不足时返回unresolved/low-confidence，不伪装成可执行语义
- 不为了"尽量答出来"越层替parser或planner做决定

#### 2.3 Planner层

**输入**: `Intent IR` + grounded constraints
**输出**: `query_family` + semantic request IR + node-based plan + lowered SPARQL

**核心组件**:
- **Family Router**: 将高层意图映射到最小可执行family
  - `anchored_fact_lookup`: 锚点+属性查值
  - `anchored_causal_lookup`: 锚点+因果查询
  - `causal_lookup`: 单实体因果查询
  - `causal_enumeration`: 批量因果枚举
  - `enumeration`: 通用枚举
  - `hidden_relation`: 隐藏关系发现
- **Semantic Request IR Builder**: 显式表达查询意图
- **Node-based Planner**: 构建dataflow plan
  - `AnchorResolve -> EntityScan -> EvidenceFilter -> Project`
  - `EntitySetScan -> EvidenceFilter -> Aggregate/Group -> AnalyzerBatch`
- **Validator**: 执行前硬校验
  - 类/属性是否存在
  - domain/range是否匹配
  - 返回列是否满足anchor要求

**设计原则**:
- plan不合法时fail closed，最多允许一次受控widening
- 不能把失败转嫁为Claude/Agent的手工sample/手写SPARQL

### 3. Query Family 机制

Family是查询意图的抽象，定义了所需的语义槽位和允许绑定的节点类型。

| Family | Slot Schema | 执行模式 |
|--------|-------------|----------|
| `causal_enumeration` | subject_text(cause_text, action_or_state_text) | SPARQL + paths-batch analysis |
| `causal_lookup` | target_text(cause_text, action_or_state_text) | SPARQL + paths analysis |
| `anchored_causal_lookup` | anchor_text, target_text, status_or_problem_text | SPARQL + paths analysis |
| `anchored_fact_lookup` | anchor_text, target_text | SPARQL only |
| `enumeration` | target_text, action_or_state_text | SPARQL only |
| `explanation_enumeration` | target_text, action_or_state_text | SPARQL (value-enumeration lowering) |

### 4. Intent IR 结构

```python
Intent IR = {
    "focus": {
        "anchored_entity": ...,      # 是否有显式锚点
        "referenced_result_set": ..., # 是否引用前序结果
        "implicit_entity_set": ...,   # 隐式实体集合
    },
    "operators": [                   # 算子集合
        "lookup" | "enumerate" | "status_check" |
        "explain" | "remediation" | "count" | ...
    ],
    "constraints": [                 # 约束条件
        {"kind": "comparison", ...},
        {"kind": "surface_predicate", ...},
        {"kind": "cause_phrase", ...},
        {"kind": "time_scope", ...},
    ],
    "references": {                  # 引用解析
        "depends_on": ...,           # 依赖的unit
        "reference_scope": ...,      # 引用范围
    },
    "output": {                      # 输出诉求
        "shape": ...,                # 结果形态
        "grain": ...,                # 粒度
        "needs_analysis": ...,       # 是否需要分析
    }
}
```

### 5. Execution DAG 设计

对于多问题输入（如"13800138004是否低满意度？如果有，原因是什么？"），系统会：

1. **分解为QuestionUnits**:
   ```
   q1: status_check (13800138004是否低满意度)
   q2: explain (原因是什么) → depends_on q1
   ```

2. **构建执行DAG**:
   - 每个unit声明`depends_on`和`condition`
   - q2只在q1返回true/non-empty时执行

3. **会话状态继承**:
   - 上一轮`Intent IR`和结果集锚点
   - 解析"这些"、"分别"等引用
   - `source scope`下推（同类约束继承）

4. **Multi-Answer Presentation**:
   - 形成`QuestionBatchResult`
   - 每个unit独立的status/result/skipped_reason
   - 最后统一决定展示方式

### 6. 与Server的交互协议

#### 推荐执行线路
```
schema
  -> run (主查询)
  -> analysis (可选，仅当主查询有结果)
```

#### 约束
- 不在第一次`run`前做泛化`/sample`
- `sample`只作为失败恢复工具
- 正常问答目标收敛为3次server round-trip
- `causal_enumeration`使用`paths-batch`，不用逐个`/causal/{id}`

#### 失败恢复线路
```
schema
  -> run
  -> one targeted sample  (最多一次)
  -> rerun                (回到run)
```

不允许：
```
schema -> run -> sample customer -> sample event -> raw sparql -> ...
```

### 7. 失败语义与恢复策略

| 失败类型 | 行为 | 恢复策略 |
|----------|------|----------|
| **planning_required** | grounding不足或validator未通过 | 返回`need_clarification`或允许一次targeted sample |
| **empty_result** | 主查询返回0行 | 返回结构化`recovery_hint`，允许一次sample + rerun |
| **partial_success** | 主查询成功但缺少analysis锚点 | 返回已执行结果，标记analysis为skipped |

### 8. 职责边界总结

| 组件 | 负责 | 不负责 |
|------|------|--------|
| **Skill** | 协议纪律、选择最小template、调用`run`、遵循bounded recovery | 不重新实现grounding、不承诺特定planner路径 |
| **Client/Planner** | semantic manifest、request IR、node plan、lowering、validation、bounded fallback | 不承载业务问句补丁 |
| **Server** | schema、structured query execution、analysis execution | 不写客户特定DFS逻辑 |

---

## 项目结构总览

```
reasoner/
├── reasoning_server.py          # Server端：RDF物化、OWL推理、API端点
├── mapping.yaml                 # DuckDB到RDF的映射配置
├── .agents/skills/obda-query/
│   ├── SKILL.md                 # Skill协议文档
│   └── scripts/
│       ├── obda_api.py          # Client主入口（含planner）
│       ├── obda_intent_parser.py      # Parser facade
│       ├── obda_parser_surface.py     # 表面解析（分句、锚点检测）
│       ├── obda_parser_contracts.py   # Parser契约
│       ├── obda_parser_backends.py    # 后端实现
│       ├── obda_lexical.py            # 词法召回
│       ├── obda_ir_contracts.py       # IR契约
│       ├── obda_question_mode_runtime.py      # Batch执行
│       ├── obda_question_mode_single_runtime.py # 单问题执行
│       ├── obda_question_conversation_runtime.py # 会话管理
│       └── obda_run_plan_runtime.py    # Run plan执行
└── tests/
    └── run_question_regressions.py
```

---

## 2. 已实现的组件

### 2.1 Server端 (reasoning_server.py)

| 端点 | 状态 | 说明 |
|------|------|------|
| `/sparql` | ✅ | SPARQL查询 |
| `/schema` | ✅ | Schema信息 |
| `/sample/{class}` | ✅ | 样本查询（grounding接口） |
| `/analysis/profiles` | ✅ | 分析profile列表 |
| `/analysis/paths` | ✅ | 单实体路径分析 |
| `/analysis/paths-batch` | ✅ | 批量路径分析 |
| `/causal/{customer_id}` | ✅ | 向后兼容alias |
| `/reload` | ✅ | 重新加载 |
| `/health` | ✅ | 健康检查 |

**内置Profiles**: `default`, `causal`, `structural`, `inference`

### 2.2 Semantic Query Planner（Client端）

#### 三层架构实现情况

| 层级 | 组件 | 状态 |
|------|------|------|
| **Parser层** | Utterance Decomposer | ✅ 已实现 |
| | Intent IR | ✅ 已定义契约 |
| | Reference Resolver | ⚠️ 基础实现 |
| **Grounding层** | Lexical Recall | ✅ 已实现（regex-based） |
| | Hybrid Binder | ⚠️ 过渡实现（lexical + hash vector） |
| | Value Catalog | ⚠️ bounded sampling |
| **Planner层** | Family Router | ✅ 已实现 |
| | Semantic Request IR | ✅ 已实现 |
| | Node-based Plan | ⚠️ 部分实现 |
| | SPARQL Lowering | ✅ builder模式 |

---

## 3. 与设计文档的符合度

### 3.1 符合设计文档的部分

| 设计目标 | 实现状态 |
|----------|----------|
| `schema -> run` 线路 | ✅ 已实现 |
| query-first-then-analysis | ✅ 已实现 |
| batch analysis（paths-batch） | ✅ 已实现 |
| Execution DAG | ✅ 已实现 |
| Conversation State | ✅ 已实现 |
| Intent IR 契约 | ✅ 已定义 |
| fail-closed gating | ✅ 已实现 |
| 恢复路径收敛（one sample + one rerun） | ✅ 已实现 |
| 7个query family支持 | ✅ 已定义 |
| `/sample`作为grounding接口 | ✅ 已保留 |

### 3.2 仍处过渡状态的部分

| 设计目标 | 当前状态 | 差距说明 |
|----------|----------|----------|
| **LLM-first Parser** | ⚠️ 只有NoModelBackend | 目前仍是deterministic regex为主 |
| **Ontology-first Grounding** | ⚠️ 混合策略 | lexical + 局部hash vector，非完整semantic retrieval |
| **Node-based Plan** | ⚠️ 部分实现 | lowering已覆盖主要family，但非完全由统一IR驱动 |
| **Semantic Manifest** | ⚠️ 过渡实现 | 已有ClassNode/AttributeNode/RelationNode，但binder未完全采用hybrid retrieval |
| **Value Catalog** | ⚠️ bounded sampling | 非完整自动化value catalog |
| **anchored_family lowering** | ❌ fail closed | anchored_causal_lookup/anchored_fact_lookup仍返回planning_required |

### 3.3 需要继续收敛的领域语义词表

根据设计文档第4.3.4节和4.3.5节，以下内容应逐步迁移：

| 当前实现 | 目标状态 |
|----------|----------|
| `obda_lexical.py`中的`CAUSE_PATTERN`/`WHICH_PATTERN`等 | 迁移到LLM-first parser |
| `ROLE_PATTERNS`（如果存在） | 完全移除，用manifest binding替代 |
| 数值比较pattern | 保留为language-agnostic surface normalization |
| `status_check` heuristics | 收敛到Intent IR operator |

---

## 4. Query Family支持矩阵

| Family | 支持状态 | SPARQL | Analysis | 说明 |
|--------|----------|--------|----------|------|
| `fact_lookup` | ✅ | builder | 无 | 单事实查询 |
| `enumeration` | ✅ | builder | 无 | 枚举查询 |
| `causal_lookup` | ✅ | builder | paths | 单实体因果查询 |
| `causal_enumeration` | ✅ | builder | paths-batch | 批量因果枚举 |
| `hidden_relation` | ⚠️ | 无 | inferred-relations | 依赖分析器 |
| `anchored_causal_lookup` | ❌ | N/A | N/A | 返回planning_required |
| `anchored_fact_lookup` | ❌ | N/A | N/A | 返回planning_required |
| `explanation_enumeration` | ✅ | builder | 无 | 解释枚举 |

---

## 5. 当前优势

1. **架构分层清晰**：Parser/Grounding/Planner三层职责已分离
2. **协议纪律严格**：Skill文档明确定义了23条non-negotiable规则
3. **Batch执行完整**：多问题utterance的DAG执行已实现
4. **恢复路径收敛**：empty_result后的恢复已收敛为"一次sample + 一次rerun"
5. **Fail-closed设计**：unsupported family明确返回planning_required而非硬凑答案

---

## 6. 待完成工作

根据设计文档任务4.6和4.7：

### 6.1 高优先级

1. **引入LLM-first Parser**
   - 当前只有NoModelBackend
   - 需要实现AgentModelBackend或StandaloneModelBackend
   - 将语言理解从regex迁移到LLM-first

2. **完成Anchored Family Lowering**
   - `anchored_causal_lookup`
   - `anchored_fact_lookup`
   - 当前返回`planning_required`，需要实现完整lowering

3. **强化Ontology-first Grounding**
   - 从lexical recall升级到完整semantic grounding
   - 实现真正的hybrid retrieval（lexical + dense + structural prior）

### 6.2 中优先级

4. **完整的Node-based Plan**
   - 让所有family的lowering完全由semantic request IR驱动
   - 减少ad-hoc candidate逻辑

5. **Presentation Layer**
   - 完成`causal_enumeration`/`causal_lookup`的结构化presentation
   - 区分`display`和`refs`

6. **自动化Value Catalog**
   - 从bounded sampling升级到完整value catalog

### 6.3 长期目标

7. **移除剩余的领域语义词表**
   - 将`obda_lexical.py`中的业务语义pattern迁移到parser/planner架构
   - 只保留language-agnostic surface normalization

---

## 7. 与设计文档四原则的一致性

| 原则 | 符合度 | 说明 |
|------|--------|------|
| **无特化** | ✅ 高 | phrase-specific路径已基本撤出，unsupported family fail closed |
| **通用性** | ✅ 高 | family/slot schema/manifest binding机制已建立 |
| **可扩展** | ⚠️ 中 | 新ontology概念仍需一定适配，但框架已具备 |
| **无人工** | ⚠️ 中 | 仍有bootstrap heuristics，但明确标记为过渡层 |

---

## 8. 结论

当前实现已完成**Semantic Query Planner的基础框架**，实现了设计文档中定义的大部分核心架构：

- ✅ 三层流水线（Parser/Grounding/Planner）结构已建立
- ✅ Query Family机制已运行
- ✅ Batch执行和会话状态已可用
- ✅ Fail-closed契约已实施

### 下一阶段的主要工作

1. 引入真正的LLM-first Parser（AgentModelBackend）
2. 完成anchored family的通用lowering
3. 将grounding从lexical升级到完整的ontology-first semantic grounding
4. 完善presentation layer

### 下次恢复工作的直接入口

根据设计文档第12.2.1节，下次工作可从以下问题开始：

> 如何在不污染通用Skill的前提下，为causal_enumeration增加受约束的semantic query planner/validator，从而减少首轮empty_result，并把恢复路径收敛为one targeted sample + one rerun？

或从展示优化角度：

> 如何为run增加presentation层，让Python只输出结构化展示模型，而把最终自然语言组织交给Claude？
