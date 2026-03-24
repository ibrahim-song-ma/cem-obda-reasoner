# Common OBDA Query Patterns

This reference contains reusable SPARQL patterns for ontology-based data access.

## Basic Patterns

### 1. Entity Retrieval

Find all instances of a class:
```sparql
SELECT ?instance
WHERE {
  ?instance a ex:ClassName
}
```

With properties:
```sparql
SELECT ?instance ?name ?value
WHERE {
  ?instance a ex:ClassName ;
            ex:hasName ?name ;
            ex:hasValue ?value .
}
```

### 2. Property Filters

Numeric comparison:
```sparql
FILTER(?score < 3.0)
FILTER(?value >= 100)
FILTER(?arpu BETWEEN 100 AND 500)
```

String matching:
```sparql
FILTER(?name = "Exact Match")
FILTER(CONTAINS(?description, "keyword"))
FILTER(STRSTARTS(?id, "PREFIX_"))
```

Multiple conditions:
```sparql
FILTER(?score < 3.0 && ?arpu > 200)
```

### 3. Relationship Traversal

One-hop:
```sparql
?entityA ex:relatesTo ?entityB
```

Two-hop:
```sparql
?entityA ex:relatesTo ?intermediate .
?intermediate ex:relatesTo ?entityB
```

With property chain:
```sparql
?customer ex:hasEvent ?event .
?event ex:hasSeverity ?severity ;
       ex:occurredAt ?time .
```

### 4. Aggregation Queries

Count:
```sparql
SELECT (COUNT(?entity) AS ?count)
WHERE { ?entity a ex:ClassName }
```

Average:
```sparql
SELECT (AVG(?score) AS ?avgScore)
WHERE { ?s ex:hasScore ?score }
```

Group by:
```sparql
SELECT ?category (COUNT(*) AS ?count)
WHERE {
  ?entity a ex:ClassName ;
          ex:hasCategory ?category .
}
GROUP BY ?category
```

### 5. Optional Relationships

Include even if optional property is missing:
```sparql
OPTIONAL { ?entity ex:optionalProperty ?optionalValue }
```

### 6. Causal Chain Pattern

Complete chain traversal:
```sparql
SELECT ?source ?step1 ?step2 ?target
WHERE {
  ?source ex:step1Rel ?step1 .
  ?step1 ex:step2Rel ?step2 .
  ?step2 ex:targetRel ?target .
}
```

## CEM-Specific Examples

These examples assume CEM ontology structure:

### Low Satisfaction Customers
```sparql
PREFIX ex: <http://ywyinfo.com/example-owl#>
SELECT ?customer ?name ?score
WHERE {
  ?customer a ex:customer ;
            ex:customer_姓名 ?name ;
            ex:hasBehavior ?behavior .
  ?behavior ex:customerbehavior_满意度评分 ?score .
  FILTER(?score < 3.0)
}
```

### Network Experience Issues
```sparql
PREFIX ex: <http://ywyinfo.com/example-owl#>
SELECT ?customer ?name ?networkScore
WHERE {
  ?customer a ex:customer ;
            ex:customer_姓名 ?name ;
            ex:hasBehavior ?behavior .
  ?behavior ex:customerbehavior_网络体验评分 ?networkScore .
  FILTER(?networkScore < 3.0)
}
```

### High-Value Customers
```sparql
PREFIX ex: <http://ywyinfo.com/example-owl#>
SELECT ?customer ?name ?arpu
WHERE {
  ?customer a ex:customer ;
            ex:customer_姓名 ?name ;
            ex:customer_客户等级 "高价值" ;
            ex:hasBehavior ?behavior .
  ?behavior ex:customerbehavior_ARPU值 ?arpu .
}
ORDER BY DESC(?arpu)
```

### Full Causal Chain
```sparql
PREFIX ex: <http://ywyinfo.com/example-owl#>
SELECT ?name ?eventDesc ?perceptionDim ?strategyName
WHERE {
  ?customer a ex:customer ;
            ex:customer_姓名 ?name ;
            ex:hasEvent ?event .
  ?event ex:event_事件描述 ?eventDesc ;
         ex:hasPerception ?perception .
  ?perception ex:perception_分析维度 ?perceptionDim .
  OPTIONAL {
    ?perception ex:suggestsStrategy ?strategy .
    ?strategy ex:remediationstrategy_策略名称 ?strategyName .
  }
}
```

## RDB2RDF Mapping Patterns

When relational data is mapped to RDF, understanding the mapping pattern is critical for correct query construction.

### Foreign Key → Object Property

Relational foreign keys become object properties linking entities:

```
Relational: customer.customer_id → customerbehavior.customer_id
RDF: ?customer ex:hasBehavior ?behavior
```

**Query Pattern**:
```sparql
# Find customer through behavior's phone number
?behavior a ex:customerbehavior ;
          ex:customerbehavior_手机号 "13800138004" ;
          ex:hasCustomer ?customer .
?customer ex:customer_姓名 ?name .
```

### Table Attribute → Data Property on Same Class

Simple attributes map to data properties on the corresponding class:

```
Relational: customer.name
RDF: ?customer ex:customer_姓名 ?name
```

**Query Pattern**:
```sparql
?customer a ex:customer ;
          ex:customer_姓名 "张三" .
```

### Cross-Table Attribute → Join via Object Property

When filtering requires attributes from related tables:

```sparql
# Customer with specific behavior attributes
?customer a ex:customer ;
          ex:hasBehavior ?behavior .
?behavior ex:customerbehavior_满意度评分 ?score ;
          ex:customerbehavior_手机号 ?phone .
FILTER(?score < 3.0)
```

### Naming Convention

Properties often follow pattern: `{className}_{attributeName}`

| Relational Table | RDF Class | Property Example |
|-----------------|-----------|------------------|
| customer | ex:customer | ex:customer_姓名 |
| customerbehavior | ex:customerbehavior | ex:customerbehavior_手机号 |
| event | ex:event | ex:event_事件描述 |

### Common Mistakes

❌ **Wrong**: Assuming attribute location
```sparql
# Phone is NOT on customer directly
?customer a ex:customer ;
          ex:customer_手机号 "13800138004" .  # This property doesn't exist!
```

✅ **Correct**: Navigate via relationship
```sparql
?customer a ex:customer ;
          ex:hasBehavior ?behavior .
?behavior ex:customerbehavior_手机号 "13800138004" .
```

### Debugging Tips

1. **Use `/sample/{class}` endpoint** - Inspect actual instances
2. **Check property domain** - Verify which class a property belongs to
3. **Follow the FK chain** - Trace relational foreign keys to RDF object properties
4. **Inspect schema** - Look for object properties that bridge classes
