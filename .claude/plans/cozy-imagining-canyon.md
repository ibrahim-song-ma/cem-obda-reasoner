# 架构重构：Claude 作为智能体核心直接对接推理机

## 用户的核心需求

**当前痛点**：
- 需要写 Python 脚本做 Text-to-SPARQL
- 需要额外调用 LLM（Kimi/Moonshot）
- 链路长、效率低

**目标架构**：
```
用户问题 → Claude(我) → 本地推理机服务(owlrl+rdflib)
              ↑
         我直接生成SPARQL，
         无需中间LLM
```

**关键洞察**：Claude 本身就具备 Text-to-SPARQL 的能力，不需要再调用外部 LLM！

---

## 新架构设计

### 架构图

```
┌─────────────────────────────────────────────────────────────┐
│                      用户界面层                               │
│                   (Claude Code / CLI)                        │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                   智能体核心 (Claude)                        │
│  ┌─────────────────┐  ┌─────────────────┐  ┌──────────────┐ │
│  │  意图理解        │  │  SPARQL生成     │  │ 结果解释     │ │
│  │  (内置能力)      │  │  (内置能力)     │  │ (内置能力)   │ │
│  └─────────────────┘  └─────────────────┘  └──────────────┘ │
└──────────────────────────┬──────────────────────────────────┘
                           │ HTTP/gRPC
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                  本地推理机服务 (Python)                      │
│  ┌─────────────────┐  ┌─────────────────┐  ┌──────────────┐ │
│  │  SPARQL执行     │  │  owlrl推理      │  │ 图谱管理     │ │
│  │  (rdflib)       │  │  (OWL 2 RL)     │  │ (DuckDB+RDF) │ │
│  └─────────────────┘  └─────────────────┘  └──────────────┘ │
│                                                             │
│  数据来源：                                                   │
│  - cem_data.duckdb (关系数据)                                │
│  - graph.ttl (RDF图谱)                                       │
│  - Onto/cem.owl (本体定义)                        │
└─────────────────────────────────────────────────────────────┘
```

---

## 组件设计

### 1. 本地推理机服务 (Local Reasoning Server)

一个轻量级 Python 服务，只负责：
- 加载图谱和本体
- 执行 owlrl 推理
- 暴露 SPARQL 查询接口
- **无 LLM 调用逻辑**

```python
# reasoning_server.py
from fastapi import FastAPI
from rdflib import Graph
import owlrl

app = FastAPI()

# 启动时加载图谱和推理
graph = Graph()
graph.parse("graph.ttl", format="turtle")
graph.parse("Onto/cem.owl", format="turtle")

# 应用推理
owlrl.DeductiveClosure(owlrl.OWLRL_Semantics).expand(graph)

@app.post("/sparql")
def query_sparql(query: str):
    """纯SPARQL执行，无LLM"""
    results = graph.query(query)
    return {"results": [dict(row) for row in results]}

@app.get("/schema")
def get_schema():
    """返回本体结构，供Claude使用"""
    return {
        "classes": [...],
        "properties": [...]
    }
```

### 2. Claude 侧（智能体核心）

我不再调用外部 LLM，而是：
1. **理解用户意图** → 利用我的推理能力
2. **生成 SPARQL** → 直接输出 SPARQL 文本
3. **调用推理机** → HTTP 请求到本地服务
4. **解释结果** → 利用我的推理能力生成中文解释

**示例交互**：
```
用户：哪些客户有网络体验问题？

Claude:
1. 分析意图：查询低网络体验评分的客户
2. 生成 SPARQL：
   SELECT ?customer ?name ?score WHERE {
     ?customer a ex:customer ;
               ex:hasBehavior ?behavior .
     ?behavior ex:网络体验评分 ?score .
     FILTER(?score < 3.0)
   }
3. HTTP POST 到 localhost:8000/sparql
4. 收到结果 → 生成中文回答
```

---

## 实现步骤

### 阶段1：搭建本地推理机服务

1. **创建 FastAPI 服务**
   - 加载 `graph.ttl` 和 `.owl` 本体
   - 执行 `owlrl` 推理
   - 暴露 REST API

2. **API 设计**
   ```
   POST /sparql      - 执行 SPARQL 查询
   GET  /schema      - 获取本体结构
   GET  /health      - 健康检查
   ```

3. **启动脚本**
   ```bash
   .venv/bin/python reasoning_server.py
   # 服务运行在 localhost:8000
   ```

### 阶段2：Claude 对接推理机

1. **创建 MCP Server 或直接 HTTP 调用**
   - 选项A：使用 MCP 工具（如果环境支持）
   - 选项B：我直接生成 `curl` 命令或 Python 脚本执行

2. **查询流程**
   ```
   用户问题 → 我生成 SPARQL → 调用本地服务 → 我解释结果 → 返回给用户
   ```

### 阶段3：优化体验

1. **Schema 缓存**
   - 我启动时读取 `/schema` 缓存本体结构
   - 避免每次查询都重新获取

2. **对话历史**
   - 维护多轮对话上下文
   - 支持追问和澄清

---

## 技术细节

### 依赖

```txt
fastapi>=0.100.0
uvicorn>=0.23.0
rdflib>=7.0.0
owlrl>=6.0.2
```

### 启动方式

```bash
# 1. 启动推理机服务
.venv/bin/uvicorn reasoning_server:app --port 8000

# 2. Claude 直接对话即可，无需额外配置
```

### Claude 如何调用服务

我可以直接生成 Python 代码执行：

```python
import requests

# 生成 SPARQL
sparql = """
SELECT ?customer ?name ?score
WHERE {
  ?customer a <http://ywyinfo.com/example-owl#customer> ;
            <http://ywyinfo.com/example-owl#hasBehavior> ?behavior .
  ?behavior <http://ywyinfo.com/example-owl#customerbehavior_满意度评分> ?score .
  FILTER(?score < 3.0)
}
"""

# 调用本地服务
response = requests.post("http://localhost:8000/sparql",
                        json={"query": sparql})
results = response.json()

# 解释结果...
```

---

## 优势对比

| 维度 | 原架构 | 新架构 |
|------|--------|--------|
| LLM调用 | 2次/查询 (Text-to-SPARQL + 解释) | 0次 (我用自身能力) |
| 中间层 | Python推理代理 | 纯推理机服务 |
| 延迟 | 2-6s | <500ms (仅SPARQL执行) |
| 代码量 | 2个复杂脚本 | 1个简单服务 |
| 依赖 | openai包 + API key | 无额外LLM依赖 |

---

## OWL 推理与隐藏关系发现

### 1. owlrl 能发现哪些隐藏关系？

OWL 2 RL 推理规则可以自动推断：

| 推理类型 | 示例 | 说明 |
|----------|------|------|
| **传递性** | A→B, B→C ⊢ A→C | 因果链传递 |
| **逆属性** | hasEvent 的逆是 isEventOf | 双向导航 |
| **子类继承** | Customer ⊑ Person | 属性继承 |
| **属性链** | hasEvent ∘ hasPerception | 复合关系 |
| **互斥/补集** | Satisfaction < 3 ⊑ LowSatisfaction | 自动分类 |
| **对称性** | relatedTo(A,B) → relatedTo(B,A) | 双向关系 |

### 2. 当前本体中的因果链

```turtle
# 显式定义（mock_and_map.py 中已创建）
刘芳 --hasEvent--> EVT004
EVT004 --hasPerception--> PER002
PER002 --suggestsStrategy--> STR001

# owlrl 推理后应产生的隐藏关系
刘芳 --hasPerception--> PER002      (通过事件传递)
刘芳 --suggestsStrategy--> STR001    (完整因果链)
EVT004 --suggestsStrategy--> STR001  (事件直接关联策略)
```

### 3. 优雅发现隐藏关系的方案

#### 方案 A：推理后图谱分析（推荐）

在 `owlrl.DeductiveClosure` 之后，对比原始图谱和推理后图谱：

```python
def discover_inferred_relations(original_graph, reasoned_graph):
    """
    发现 owlrl 推理出的隐藏关系
    """
    inferred = []

    for triple in reasoned_graph:
        if triple not in original_graph:
            subject, predicate, obj = triple

            # 只关注因果相关的属性
            if is_causal_property(predicate):
                inferred.append({
                    "from": subject,
                    "relation": predicate,
                    "to": obj,
                    "type": "inferred"
                })

    return inferred

def is_causal_property(prop):
    """判断是否为因果链中的属性"""
    causal_props = [
        "hasEvent", "hasPerception", "suggestsStrategy",
        "hasWorkOrder", "hasRemediationStrategy"
    ]
    return any(p in str(prop) for p in causal_props)
```

#### 方案 B：因果路径探索器

专门探索两个实体间的所有可能因果路径：

```python
class CausalPathExplorer:
    def __init__(self, reasoned_graph):
        self.graph = reasoned_graph
        # 定义因果属性顺序
        self.causal_chain = [
            "hasEvent",
            "hasPerception",
            "suggestsStrategy"
        ]

    def find_causal_paths(self, source, target=None, max_depth=3):
        """
        从源实体出发，发现所有因果路径
        """
        paths = []
        visited = set()

        def dfs(current, path, depth):
            if depth > max_depth:
                return

            # 遍历所有因果属性
            for prop in self.causal_chain:
                for next_node in self.graph.objects(current, EX[prop]):
                    if next_node not in visited:
                        new_path = path + [(prop, next_node)]
                        paths.append(new_path)
                        visited.add(next_node)
                        dfs(next_node, new_path, depth + 1)

        dfs(source, [], 0)
        return paths

    def explain_causality(self, customer_id):
        """
        为特定客户生成因果解释
        """
        customer = EX[customer_id]

        # 找到所有因果链
        paths = self.find_causal_paths(customer)

        explanations = []
        for path in paths:
            # 路径示例: [(hasEvent, EVT004), (hasPerception, PER002), (suggestsStrategy, STR001)]
            if len(path) >= 2:  # 至少有两个节点才构成因果
                explanation = self._format_path(path)
                explanations.append(explanation)

        return explanations

    def _format_path(self, path):
        """将路径格式化为人类可读的因果解释"""
        steps = []
        for prop, node in path:
            prop_name = prop.replace("has", "").replace("suggests", "建议")
            steps.append(f"{prop_name} → {node}")
        return " → ".join(steps)
```

### 4. 在 Skill 中集成

```python
# /obda-query --explain-causality 刘芳
# 输出：
# 发现 2 条因果链：
# 1. 刘芳 → 事件(EVT004:宽带故障) → 感知(PER002:网络质量) → 策略(STR001:网络优化)
# 2. 刘芳 → 事件(EVT005:服务投诉) → 感知(PER003:服务质量) → 策略(STR003:服务补救)
```

### 5. 关键设计决策

| 决策点 | 建议 | 理由 |
|--------|------|------|
| 推理时机 | 服务启动时执行 | 避免每次查询都推理 |
| 隐藏关系存储 | 内存中实时对比 | 不存储，即时发现 |
| 因果属性定义 | 代码中硬编码列表 | 简单可靠，后期可改配置 |
| 路径深度限制 | 最大 3-4 跳 | 防止图爆炸 |

### 6. 用户查询示例

```bash
# 查询并展示隐藏关系
/obda-query --causal-chain "13800138004"

# 输出格式：
# 客户: 刘芳 (13800138004)
# ├─ 直接关联事件: 2个
# │  ├─ EVT004: 宽带故障
# │  │  └─ [推理发现] 关联感知: PER002 (网络质量)
# │  │     └─ [推理发现] 建议策略: STR001 (网络优化)
# │  └─ EVT005: 服务投诉
# │     └─ [推理发现] 关联感知: PER003 (服务质量)
# │        └─ [推理发现] 建议策略: STR003 (服务补救)
# └─ 综合解决方案: 网络优化 + 服务补救
```

---

## 性能分析与优化

### 朴素实现的性能问题

| 操作 | 时间复杂度 | 实际耗时 (6476 三元组) |
|------|-----------|----------------------|
| 集合差 (`reasoned - original`) | O(n) | ~10ms |
| DFS 路径探索 (深度 3) | O(b^d) = O(5³) = 125 | ~50-100ms |
| 每次查询都执行 | - | **不可接受** |

**问题**：如果每次用户查询都做 DFS，延迟会很明显。

### 优化方案：预计算 + 索引

#### 核心思想
**服务启动时一次性计算，查询时只读索引**

```python
class OptimizedReasoningServer:
    def __init__(self):
        # 1. 加载图谱
        self.graph = self._load_and_reason()

        # 2. 预计算因果索引（启动时执行一次）
        self.causal_index = self._build_causal_index()

        # 3. 预计算隐藏关系映射
        self.inferred_relations = self._compute_inferred_map()

    def _build_causal_index(self):
        """
        预计算所有客户的因果路径
        时间：启动时 500ms-2s，查询时 O(1)
        """
        index = {}
        for customer in self._get_all_customers():
            index[customer] = self._find_causal_paths(customer)
        return index

    def query_causal_chain(self, customer_id):
        """
        查询时直接读索引，O(1) 时间
        """
        return self.causal_index.get(customer_id, [])
```

#### 内存占用估算

```
客户数：8
平均路径数/客户：3
路径平均长度：3 个节点

索引大小 ≈ 8 × 3 × 3 × 指针大小 ≈ 几 KB
```

**结论：内存占用极小，可以忽略不计。**

### 渐进式优化策略

| 阶段 | 实现 | 查询延迟 | 适用场景 |
|------|------|----------|----------|
| **MVP** | 实时 DFS | 50-200ms | 演示/测试 |
| **优化** | 预计算索引 | <1ms | 生产环境 |
| **极致** | 索引 + 缓存 | <1ms | 高并发 |

### 推荐的轻量级实现

```python
# reasoning_server.py
from fastapi import FastAPI
from rdflib import Graph
import owlrl
from functools import lru_cache

app = FastAPI()

# 全局状态（服务启动时初始化）
GRAPH = None
CAUSAL_INDEX = {}

def init():
    """服务启动时执行一次"""
    global GRAPH, CAUSAL_INDEX

    # 1. 加载和推理
    GRAPH = Graph()
    GRAPH.parse("graph.ttl", format="turtle")
    GRAPH.parse("Onto/cem.owl", format="turtle")
    owlrl.DeductiveClosure(owlrl.OWLRL_Semantics).expand(GRAPH)

    # 2. 预计算因果索引（可选，也可以按需计算）
    CAUSAL_INDEX = _build_causal_index()

def _build_causal_index():
    """构建因果路径索引"""
    index = {}
    # 只处理客户节点，减少计算量
    for customer in GRAPH.subjects(RDF.type, EX.customer):
        paths = _find_causal_paths_limited(customer, max_depth=3)
        index[str(customer)] = paths
    return index

def _find_causal_paths_limited(source, max_depth=3):
    """带深度限制的 DFS，防止爆炸"""
    paths = []
    causal_props = [EX.hasEvent, EX.hasPerception, EX.suggestsStrategy]

    def dfs(current, path, depth):
        if depth >= max_depth:
            return
        for prop in causal_props:
            for next_node in GRAPH.objects(current, prop):
                new_path = path + [(prop, next_node)]
                paths.append(new_path)
                dfs(next_node, new_path, depth + 1)

    dfs(source, [], 0)
    return paths

@app.on_event("startup")
async def startup():
    init()

@app.get("/causal/{customer_id}")
def get_causal_chain(customer_id: str):
    """O(1) 查询预计算索引"""
    customer_uri = f"http://ywyinfo.com/example-owl#{customer_id}"
    return {
        "customer": customer_id,
        "causal_paths": CAUSAL_INDEX.get(customer_uri, [])
    }

@app.post("/sparql")
def query_sparql(query: str):
    """常规 SPARQL 查询"""
    results = GRAPH.query(query)
    return {"results": [dict(row) for row in results]}
```

### 启动时间 vs 查询时间权衡

```
服务启动：
  - 加载图谱: 100ms
  - owlrl 推理: 500ms-1s
  - 构建索引: 100ms
  ============
  总计: ~2s（一次性）

查询响应：
  - SPARQL 查询: 10-50ms
  - 因果路径查询: <1ms（读索引）
```

**这是可接受的 trade-off**：启动慢 2 秒，查询快 100 倍。

---

## 大图谱方案（百万/千万级三元组）

当前方案（rdflib + owlrl + 内存索引）**仅适用于小规模图谱**（< 10万三元组）。大图谱需要完全不同的架构。

### 大图谱的挑战

| 规模 | 三元组数 | 内存占用 | owlrl 推理时间 | 问题 |
|------|---------|----------|---------------|------|
| 小图 | 6K | ~10MB | ~1s | ✅ 当前方案适用 |
| 中图 | 100K | ~200MB | ~30s | ⚠️ 启动慢但可用 |
| 大图 | 1M | ~2GB | 不可完成 | ❌ 内存溢出 |
| 超大图 | 10M+ | ~20GB+ | 不可完成 | ❌ 完全不可行 |

### 方案一：图数据库（推荐用于大图）

**架构变化**：
```
Claude → SPARQL → GraphDB (Neo4j/Amazon Neptune) → 结果
```

**Neo4j + RDF 插件**：
```python
from neo4j import GraphDatabase

class Neo4jReasoningServer:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        # Neo4j 内置索引，无需预计算

    def query(self, sparql):
        # 使用 Neo4j 的 Cypher 或 RDF 插件
        with self.driver.session() as session:
            result = session.run(sparql_to_cypher(sparql))
            return list(result)
```

**优势**：
- 支持亿级节点
- 内置推理（RDFS/OWL 子集）
- 水平扩展

**劣势**：
- 需要部署 Neo4j
- 配置复杂

### 方案二：分布式推理（Apache Jena/Fuseki）

```python
# 使用 Apache Jena 的 TDB 存储 + 推理引擎
from pyfuseki import Fuseki

class JenaReasoningServer:
    def __init__(self):
        self.fuseki = Fuseki("http://localhost:3030")
        # Jena 支持规则级推理，比 owlrl 高效

    def query(self, sparql):
        return self.fuseki.query(sparql)
```

**优势**：
- 支持 OWL 2 RL 完整语义
- 磁盘存储，内存友好
- SPARQL 1.1 完整支持

**劣势**：
- 推理仍慢（但比 rdflib 快）
- 需要 Java 环境

### 方案三：增量推理（推荐用于生产）

**核心思想**：只对新数据做推理，已有结果缓存。

```python
class IncrementalReasoningServer:
    def __init__(self):
        self.version_graph = {}
        self.inferred_cache = {}

    def add_triples(self, new_triples):
        """增量添加，只推理新增部分"""
        # 1. 找到受影响的三元组
        affected = self._find_affected(new_triples)

        # 2. 只对受影响子图做推理
        subgraph = self._extract_subgraph(affected)
        inferred = owlrl.DeductiveClosure(owlrl.OWLRL_Semantics).expand(subgraph)

        # 3. 合并结果
        self.graph += new_triples
        self.graph += inferred

    def _find_affected(self, new_triples):
        """找到与新增三元组相关的已有三元组"""
        affected = set()
        for s, p, o in new_triples:
            # 找到所有与 s 或 o 相关的三元组
            affected.update(self._get_related(s))
            affected.update(self._get_related(o))
        return affected
```

**优势**：
- 实时更新
- 内存可控
- 适合流式数据

**劣势**：
- 实现复杂
- 需要维护版本

### 方案四：近似查询（GraphRAG）

**放弃精确推理，使用向量近似**：

```python
class GraphRAGServer:
    def __init__(self):
        self.encoder = SentenceTransformer('all-MiniLM-L6-v2')
        self.vector_db = chromadb.Client()

    def index_graph(self):
        """将图谱向量化"""
        for entity in self.graph.subjects():
            # 实体文本描述
            text = self._entity_to_text(entity)
            embedding = self.encoder.encode(text)
            self.vector_db.add(entity, embedding)

    def query(self, question):
        """向量检索相关子图"""
        q_emb = self.encoder.encode(question)
        relevant = self.vector_db.query(q_emb, top_k=10)
        # 在子图上做推理（小图，速度快）
        subgraph = self._extract_subgraph(relevant)
        return self._reason_and_answer(subgraph, question)
```

**优势**：
- 极速查询（毫秒级）
- 支持超大图
- 语义模糊匹配

**劣势**：
- 牺牲精确性
- 可能漏掉关键关系
- 需要向量存储

### 渐进式架构选择

| 数据规模 | 推荐方案 | 启动时间 | 查询延迟 |
|----------|----------|----------|----------|
| < 100K | rdflib + 预计算 | 2-10s | <1ms |
| 100K - 10M | Apache Jena | 30s-2min | 10-100ms |
| 10M - 100M | Neo4j | 5-10min | 10-50ms |
| > 100M | GraphRAG | 1h+ (索引) | <10ms |

### 建议

**当前项目**（6476 三元组）：
- 使用 **rdflib + 预计算索引** ✅
- 简单、快速、无需额外部署

**未来扩展**：
- 先尝试 **Apache Jena**（最标准的语义网方案）
- 如果性能不够，再考虑 **Neo4j**
- 只有需要模糊语义查询时才用 **GraphRAG**

---

## 最终方案总结

| 组件 | 小图（当前） | 大图（未来） |
|------|-------------|-------------|
| 存储 | 内存（rdflib） | 磁盘（Jena/Neo4j） |
| 推理 | owlrl（启动时） | 增量推理或内置推理 |
| 查询 | 预计算索引 | 数据库索引 |
| 部署 | 单进程 | 可水平扩展 |

需要我基于当前小图规模先实现 **rdflib + 预计算** 版本吗？
