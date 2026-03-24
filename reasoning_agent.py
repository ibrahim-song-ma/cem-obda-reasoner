#!/usr/bin/env python3
"""
Ontology-Based Reasoning Agent for Customer Experience Management (CEM)

This script:
1. Loads RDF graph and OWL ontology
2. Applies OWL reasoning to infer implicit relationships
3. Provides Text-to-SPARQL conversion using LLM
4. Answers user queries with causal explanations
"""

import os
import json
from typing import Optional, List, Dict, Any
from datetime import datetime

import rdflib
from rdflib import Graph, Namespace, URIRef, Literal, BNode
from rdflib.namespace import RDF, RDFS, OWL, XSD
import owlrl
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
GRAPH_FILE = "graph.ttl"
ONTOLOGY_FILE = "Onto/cem.owl"
REASONED_GRAPH_FILE = "reasoned_graph.ttl"

# Namespaces
EX = Namespace("http://ywyinfo.com/example-owl#")
PROV = Namespace("http://www.w3.org/ns/prov#")


class CEMReasoningAgent:
    """
    Customer Experience Management Reasoning Agent
    Combines ontology reasoning with LLM for intelligent QA
    """

    def __init__(self):
        self.graph = Graph()
        self.reasoned_graph = Graph()
        self.schema_info = {}
        self.llm_client = None

        # Initialize LLM client
        self._init_llm()

    def _init_llm(self):
        """Initialize LLM client for Text-to-SPARQL"""
        try:
            import openai

            api_key = os.getenv("OPENAI_API_KEY")
            api_base = os.getenv("OPENAI_API_BASE", "https://api.moonshot.cn/v1")

            if api_key:
                self.llm_client = openai.OpenAI(
                    api_key=api_key,
                    base_url=api_base
                )
                print(f"LLM client initialized (base_url: {api_base})")
            else:
                print("Warning: OPENAI_API_KEY not set. LLM features will be disabled.")
                print("Set your API key in .env file: OPENAI_API_KEY=your_key_here")
        except Exception as e:
            print(f"Warning: Failed to initialize LLM client: {e}")

    def load_data(self):
        """Load RDF graph and OWL ontology"""
        print("\n" + "=" * 60)
        print("Loading data...")

        # Load generated RDF graph
        if os.path.exists(GRAPH_FILE):
            self.graph.parse(GRAPH_FILE, format="turtle")
            print(f"Loaded RDF graph: {GRAPH_FILE} ({len(self.graph)} triples)")
        else:
            print(f"Warning: {GRAPH_FILE} not found. Run mock_and_map.py first.")

        # Load OWL ontology
        if os.path.exists(ONTOLOGY_FILE):
            self.graph.parse(ONTOLOGY_FILE, format="turtle")
            print(f"Loaded OWL ontology: {ONTOLOGY_FILE} ({len(self.graph)} triples total)")
        else:
            print(f"Warning: {ONTOLOGY_FILE} not found.")

        return self

    def apply_reasoning(self):
        """Apply OWL reasoning to infer implicit relationships"""
        print("\n" + "=" * 60)
        print("Applying OWL reasoning...")

        # Create a copy for reasoning
        self.reasoned_graph = Graph()
        for triple in self.graph:
            self.reasoned_graph.add(triple)

        # Apply OWL 2 RL reasoning
        owlrl.DeductiveClosure(owlrl.OWLRL_Semantics).expand(self.reasoned_graph)

        print(f"Original triples: {len(self.graph)}")
        print(f"After reasoning: {len(self.reasoned_graph)}")
        print(f"Inferred triples: {len(self.reasoned_graph) - len(self.graph)}")

        # Save reasoned graph
        self.reasoned_graph.serialize(destination=REASONED_GRAPH_FILE, format="turtle")
        print(f"Reasoned graph saved to: {REASONED_GRAPH_FILE}")

        return self

    def extract_schema(self) -> Dict[str, Any]:
        """Extract schema information from ontology"""
        print("\n" + "=" * 60)
        print("Extracting schema information...")

        schema = {
            "classes": [],
            "data_properties": [],
            "object_properties": [],
            "class_hierarchy": {},
            "property_domains": {},
            "property_ranges": {}
        }

        # Extract classes
        for cls in self.graph.subjects(RDF.type, OWL.Class):
            label = self.graph.value(cls, RDFS.label)
            if label:
                schema["classes"].append({
                    "uri": str(cls),
                    "label": str(label),
                    "local_name": cls.split("#")[-1] if "#" in str(cls) else str(cls).split("/")[-1]
                })

        # Extract data properties
        for prop in self.graph.subjects(RDF.type, OWL.DatatypeProperty):
            label = self.graph.value(prop, RDFS.label)
            domain = self.graph.value(prop, RDFS.domain)
            range_val = self.graph.value(prop, RDFS.range)

            schema["data_properties"].append({
                "uri": str(prop),
                "label": str(label) if label else None,
                "local_name": prop.split("#")[-1] if "#" in str(prop) else str(prop).split("/")[-1],
                "domain": str(domain) if domain else None,
                "range": str(range_val) if range_val else None
            })

        # Extract object properties
        for prop in self.graph.subjects(RDF.type, OWL.ObjectProperty):
            label = self.graph.value(prop, RDFS.label)
            domain = self.graph.value(prop, RDFS.domain)
            range_val = self.graph.value(prop, RDFS.range)

            schema["object_properties"].append({
                "uri": str(prop),
                "label": str(label) if label else None,
                "local_name": prop.split("#")[-1] if "#" in str(prop) else str(prop).split("/")[-1],
                "domain": str(domain) if domain else None,
                "range": str(range_val) if range_val else None
            })

        # Build class hierarchy
        for cls in self.graph.subjects(RDF.type, OWL.Class):
            parents = list(self.graph.objects(cls, RDFS.subClassOf))
            if parents:
                schema["class_hierarchy"][str(cls)] = [str(p) for p in parents]

        self.schema_info = schema
        print(f"Found {len(schema['classes'])} classes")
        print(f"Found {len(schema['data_properties'])} data properties")
        print(f"Found {len(schema['object_properties'])} object properties")

        return schema

    def text_to_sparql(self, natural_language_query: str) -> Optional[str]:
        """Convert natural language query to SPARQL using LLM"""
        if not self.llm_client:
            print("Error: LLM client not initialized. Cannot convert text to SPARQL.")
            return None

        # Prepare schema context
        classes_str = "\n".join([
            f"  - {c['local_name']} ({c['label']})"
            for c in self.schema_info.get("classes", [])[:20]  # Limit for prompt
        ])

        data_props_str = "\n".join([
            f"  - {p['local_name']} (domain: {p['domain']}, range: {p['range']})"
            for p in self.schema_info.get("data_properties", [])[:30]
        ])

        object_props_str = "\n".join([
            f"  - {p['local_name']} (domain: {p['domain']}, range: {p['range']})"
            for p in self.schema_info.get("object_properties", [])[:20]
        ])

        prompt = f"""You are an expert in SPARQL and Semantic Web technologies.
Your task is to convert natural language queries into SPARQL queries.

## Ontology Schema

### Main Classes:
{classes_str}

### Data Properties (attributes):
{data_props_str}

### Object Properties (relationships):
{object_props_str}

### Namespace Prefixes:
PREFIX ex: <http://ywyinfo.com/example-owl#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

### Key Concepts:
- customer: 客户 (has satisfaction_score, network_experience_score, etc.)
- customerbehavior: 客户行为 (contains metrics like web_disp_rate, video_play_succ_rate)
- event: 客户事件 (客户问题/投诉)
- workorder: 工单
- perception: 感知分析 (分析算法)
- remediationstrategy: 修复策略
- employee: 员工
- broadbandnetwork: 宽带网络
- wirelessnetwork: 无线网络

### Example SPARQL Patterns:

1. Find low satisfaction customers:
```sparql
SELECT ?customer ?name ?score
WHERE {{
  ?customer a ex:customer ;
            ex:customer_姓名 ?name ;
            ex:hasBehavior ?behavior .
  ?behavior ex:customerbehavior_满意度评分 ?score .
  FILTER(?score < 3.0)
}}
```

2. Find events with their remediation strategies:
```sparql
SELECT ?event ?description ?strategy
WHERE {{
  ?event a ex:event ;
         ex:event_事件描述 ?description .
  ?eventLink ex:hasEvent ?event ;
             ex:hasPerception ?perception .
  ?psLink ex:hasPerception ?perception ;
          ex:hasRemediationStrategy ?strategyEntity .
  ?strategyEntity ex:remediationstrategy_修复策略名称 ?strategy .
}}
```

## Natural Language Query:
{natural_language_query}

## Instructions:
1. Generate a SPARQL SELECT query
2. Use only the prefixes and properties defined above
3. Include appropriate FILTER clauses for conditions
4. Return results with meaningful variable names
5. If the query involves causal relationships (e.g., "为什么", "原因"), look for connections through event -> perception -> remediationstrategy

## SPARQL Query:
```sparql
"""

        try:
            response = self.llm_client.chat.completions.create(
                model=os.getenv("LLM_MODEL", "kimi-latest"),
                messages=[
                    {"role": "system", "content": "You are a SPARQL expert. Generate valid SPARQL queries based on the given ontology schema."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.2,
                max_tokens=1000
            )

            sparql_text = response.choices[0].message.content

            # Extract SPARQL from markdown code block
            if "```sparql" in sparql_text:
                sparql_text = sparql_text.split("```sparql")[1].split("```")[0].strip()
            elif "```" in sparql_text:
                sparql_text = sparql_text.split("```")[1].split("```")[0].strip()

            return sparql_text

        except Exception as e:
            print(f"Error calling LLM: {e}")
            return None

    def execute_sparql(self, sparql_query: str, use_reasoned: bool = True) -> List[Dict[str, Any]]:
        """Execute SPARQL query on the graph"""
        graph = self.reasoned_graph if use_reasoned else self.graph

        try:
            results = graph.query(sparql_query)

            # Convert to list of dicts
            output = []
            for row in results:
                row_dict = {}
                for var in results.vars:
                    val = row[var]
                    if isinstance(val, URIRef):
                        row_dict[str(var)] = str(val)
                    elif isinstance(val, Literal):
                        row_dict[str(var)] = val.value
                    else:
                        row_dict[str(var)] = str(val)
                output.append(row_dict)

            return output

        except Exception as e:
            print(f"SPARQL execution error: {e}")
            return []

    def answer_question(self, question: str, use_llm: bool = True) -> Dict[str, Any]:
        """
        Answer a user question using the knowledge graph

        Returns:
            dict with keys: question, sparql_query, results, explanation, recommendations
        """
        print("\n" + "=" * 60)
        print(f"Question: {question}")
        print("=" * 60)

        result = {
            "question": question,
            "sparql_query": None,
            "results": [],
            "explanation": None,
            "recommendations": []
        }

        # Generate SPARQL
        if use_llm and self.llm_client:
            sparql = self.text_to_sparql(question)
            result["sparql_query"] = sparql

            if sparql:
                print(f"\nGenerated SPARQL:\n{sparql}")

                # Execute query
                query_results = self.execute_sparql(sparql)
                result["results"] = query_results

                print(f"\nQuery returned {len(query_results)} results:")
                for i, row in enumerate(query_results[:5], 1):
                    print(f"  {i}. {row}")

                # Generate explanation and recommendations
                if query_results:
                    explanation, recommendations = self._generate_explanation(
                        question, query_results, sparql
                    )
                    result["explanation"] = explanation
                    result["recommendations"] = recommendations
        else:
            # Use predefined queries if LLM not available
            result["results"] = self._execute_predefined_query(question)

        return result

    def _generate_explanation(self, question: str, results: List[Dict], sparql: str) -> tuple:
        """Generate natural language explanation and recommendations"""
        if not self.llm_client:
            return "LLM not available", []

        # Prepare results summary
        results_json = json.dumps(results[:5], ensure_ascii=False, indent=2)

        prompt = f"""Based on the following SPARQL query results from a Customer Experience Management system, provide:
1. A natural language explanation of the findings (in Chinese)
2. Specific recommendations for improvement (in Chinese)

User Question: {question}

Query Results:
{results_json}

SPARQL Query Used:
{sparql}

## Response Format:
### 原因分析 (Root Cause Analysis):
[Explain the causal relationships found in the data]

### 解决方案 (Recommended Solutions):
1. [First recommendation]
2. [Second recommendation]
3. [Third recommendation if applicable]
"""

        try:
            response = self.llm_client.chat.completions.create(
                model=os.getenv("LLM_MODEL", "kimi-latest"),
                messages=[
                    {"role": "system", "content": "You are a customer experience expert. Analyze data and provide actionable insights."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=1500
            )

            content = response.choices[0].message.content

            # Parse explanation and recommendations
            explanation = content
            recommendations = []

            # Extract recommendations if in list format
            if "解决方案" in content or "Recommended Solutions" in content:
                lines = content.split("\n")
                in_recommendations = False
                for line in lines:
                    if "解决方案" in line or "Recommended Solutions" in line:
                        in_recommendations = True
                    elif in_recommendations and line.strip().startswith(("1.", "2.", "3.", "4.", "5.", "-")):
                        recommendations.append(line.strip().lstrip("12345.- "))

            return explanation, recommendations

        except Exception as e:
            print(f"Error generating explanation: {e}")
            return f"Data found: {results}", []

    def _execute_predefined_query(self, question: str) -> List[Dict]:
        """Execute predefined queries when LLM is not available"""

        # Map common questions to SPARQL queries
        queries = {
            "低满意度": """
                PREFIX ex: <http://ywyinfo.com/example-owl#>
                SELECT ?customer ?name ?score
                WHERE {
                    ?customer a ex:customer ;
                              ex:customer_姓名 ?name .
                    ?behavior a ex:customerbehavior ;
                              ex:customerbehavior_客户ID ?cid ;
                              ex:customerbehavior_满意度评分 ?score .
                    FILTER(?score < 3.0)
                }
            """,
            "网络问题": """
                PREFIX ex: <http://ywyinfo.com/example-owl#>
                SELECT ?event ?description ?customer
                WHERE {
                    ?event a ex:event ;
                           ex:event_事件描述 ?description .
                    FILTER(CONTAINS(LCASE(STR(?description)), "网络") ||
                           CONTAINS(LCASE(STR(?description)), "信号") ||
                           CONTAINS(LCASE(STR(?description)), "网速"))
                }
            """
        }

        # Find matching query
        for keyword, sparql in queries.items():
            if keyword in question:
                return self.execute_sparql(sparql)

        return []

    def interactive_mode(self):
        """Run interactive query mode"""
        print("\n" + "=" * 60)
        print("CEM Ontology-Based Reasoning Agent")
        print("Interactive Query Mode")
        print("=" * 60)
        print("\nAvailable commands:")
        print("  ask <question>  - Ask a natural language question")
        print("  sparql <query>  - Execute raw SPARQL query")
        print("  schema          - Show ontology schema summary")
        print("  exit            - Exit interactive mode")
        print("-" * 60)

        while True:
            try:
                user_input = input("\n> ").strip()

                if not user_input:
                    continue

                if user_input.lower() == "exit":
                    print("Goodbye!")
                    break

                if user_input.lower() == "schema":
                    self._print_schema_summary()
                    continue

                if user_input.lower().startswith("sparql "):
                    sparql = user_input[7:].strip()
                    results = self.execute_sparql(sparql)
                    print(f"\nResults ({len(results)} rows):")
                    for i, row in enumerate(results[:10], 1):
                        print(f"  {i}. {row}")
                    continue

                if user_input.lower().startswith("ask "):
                    question = user_input[4:].strip()
                else:
                    question = user_input

                # Process question
                result = self.answer_question(question)

                if result["explanation"]:
                    print("\n" + "-" * 60)
                    print("ANSWER:")
                    print("-" * 60)
                    print(result["explanation"])

            except KeyboardInterrupt:
                print("\n\nInterrupted. Type 'exit' to quit.")
            except Exception as e:
                print(f"Error: {e}")

    def _print_schema_summary(self):
        """Print a summary of the ontology schema"""
        print("\n" + "=" * 60)
        print("Ontology Schema Summary")
        print("=" * 60)

        print("\nClasses:")
        for cls in self.schema_info.get("classes", [])[:15]:
            print(f"  - {cls['local_name']}: {cls['label']}")

        print("\nKey Data Properties:")
        for prop in self.schema_info.get("data_properties", [])[:20]:
            print(f"  - {prop['local_name']}")

        print("\nKey Object Properties:")
        for prop in self.schema_info.get("object_properties", [])[:10]:
            print(f"  - {prop['local_name']}")


def main():
    """Main entry point"""
    print("=" * 60)
    print("CEM Ontology-Based Reasoning Agent")
    print("=" * 60)

    # Create agent
    agent = CEMReasoningAgent()

    # Load data
    agent.load_data()

    # Apply reasoning
    agent.apply_reasoning()

    # Extract schema
    agent.extract_schema()

    # Run interactive mode
    agent.interactive_mode()


if __name__ == "__main__":
    main()
