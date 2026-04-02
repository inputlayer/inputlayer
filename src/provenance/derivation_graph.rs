//! Derivation Graph - a DAG representing how derived facts were computed.
//!
//! Each node represents a single derivation step (base fact, rule application,
//! aggregate computation, etc.) and points to its premise nodes. Shared
//! sub-derivations are stored once (DAG, not tree).

use crate::value::Value;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// --- Custom serialization for Value as plain JSON ---
// Value's default serde is a tagged enum ({"type":"String","value":"x"}).
// For the derivation graph wire format, we want plain JSON values ("x", 5, true, null).

fn value_to_json(v: &Value) -> serde_json::Value {
    match v {
        Value::Int32(n) => serde_json::Value::Number((*n).into()),
        Value::Int64(n) => serde_json::Value::Number((*n).into()),
        Value::Float64(f) => serde_json::Number::from_f64(*f)
            .map_or(serde_json::Value::Null, serde_json::Value::Number),
        Value::String(s) => serde_json::Value::String(s.to_string()),
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Null => serde_json::Value::Null,
        Value::Timestamp(ts) => serde_json::Value::Number((*ts).into()),
        Value::Vector(v) => {
            let arr: Vec<serde_json::Value> = v
                .iter()
                .map(|f| {
                    serde_json::Number::from_f64(f64::from(*f))
                        .map_or(serde_json::Value::Null, serde_json::Value::Number)
                })
                .collect();
            serde_json::Value::Array(arr)
        }
        Value::VectorInt8(v) => {
            let arr: Vec<serde_json::Value> = v
                .iter()
                .map(|b| serde_json::Value::Number((*b).into()))
                .collect();
            serde_json::Value::Array(arr)
        }
    }
}

fn json_to_value(j: &serde_json::Value) -> Value {
    match j {
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                if let Ok(i32_val) = i32::try_from(i) {
                    Value::Int32(i32_val)
                } else {
                    Value::Int64(i)
                }
            } else if let Some(f) = n.as_f64() {
                Value::Float64(f)
            } else {
                Value::Null
            }
        }
        serde_json::Value::String(s) => Value::string(s),
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Array(arr) => {
            let floats: Vec<f32> = arr
                .iter()
                .filter_map(|v| v.as_f64().map(|f| f as f32))
                .collect();
            Value::vector(floats)
        }
        _ => Value::Null,
    }
}

fn serialize_values<S: serde::Serializer>(values: &[Value], s: S) -> Result<S::Ok, S::Error> {
    use serde::ser::SerializeSeq;
    let mut seq = s.serialize_seq(Some(values.len()))?;
    for v in values {
        seq.serialize_element(&value_to_json(v))?;
    }
    seq.end()
}

fn deserialize_values<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Vec<Value>, D::Error> {
    let json_vals: Vec<serde_json::Value> = Vec::deserialize(d)?;
    Ok(json_vals.iter().map(json_to_value).collect())
}

#[allow(clippy::ref_option)]
fn serialize_opt_bindings<S: serde::Serializer>(
    bindings: &Option<HashMap<String, Value>>,
    s: S,
) -> Result<S::Ok, S::Error> {
    match bindings {
        Some(map) => {
            use serde::ser::SerializeMap;
            let mut m = s.serialize_map(Some(map.len()))?;
            for (k, v) in map {
                m.serialize_entry(k, &value_to_json(v))?;
            }
            m.end()
        }
        None => s.serialize_none(),
    }
}

fn deserialize_opt_bindings<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<Option<HashMap<String, Value>>, D::Error> {
    let opt: Option<HashMap<String, serde_json::Value>> = Option::deserialize(d)?;
    Ok(opt.map(|map| {
        map.iter()
            .map(|(k, v)| (k.clone(), json_to_value(v)))
            .collect()
    }))
}

fn serialize_value<S: serde::Serializer>(v: &Value, s: S) -> Result<S::Ok, S::Error> {
    value_to_json(v).serialize(s)
}

fn deserialize_value<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Value, D::Error> {
    let j = serde_json::Value::deserialize(d)?;
    Ok(json_to_value(&j))
}

#[allow(clippy::ref_option)]
fn serialize_opt_values_2d<S: serde::Serializer>(
    opt: &Option<Vec<Vec<Value>>>,
    s: S,
) -> Result<S::Ok, S::Error> {
    match opt {
        Some(rows) => {
            use serde::ser::SerializeSeq;
            let mut seq = s.serialize_seq(Some(rows.len()))?;
            for row in rows {
                let json_row: Vec<serde_json::Value> = row.iter().map(value_to_json).collect();
                seq.serialize_element(&json_row)?;
            }
            seq.end()
        }
        None => s.serialize_none(),
    }
}

fn deserialize_opt_values_2d<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<Option<Vec<Vec<Value>>>, D::Error> {
    let opt: Option<Vec<Vec<serde_json::Value>>> = Option::deserialize(d)?;
    Ok(opt.map(|rows| {
        rows.iter()
            .map(|row| row.iter().map(json_to_value).collect())
            .collect()
    }))
}

/// A unique identifier for a derivation node.
pub type NodeId = String;

/// The complete derivation graph: a flat, deduplicated DAG.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DerivationGraph {
    /// Wire format version.
    pub version: u32,
    /// The query that produced this graph (for self-documenting exports).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query: Option<String>,
    /// Root node IDs (entry points - one per queried result tuple).
    pub roots: Vec<NodeId>,
    /// All nodes keyed by their unique ID.
    pub nodes: HashMap<NodeId, DerivationNode>,
}

impl DerivationGraph {
    /// Create an empty graph.
    pub fn new() -> Self {
        Self {
            version: 1,
            query: None,
            roots: Vec::new(),
            nodes: HashMap::new(),
        }
    }

    /// Total number of nodes.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Count of fact-kind leaf nodes.
    pub fn fact_count(&self) -> usize {
        self.nodes
            .values()
            .filter(|n| n.kind == NodeKind::Fact)
            .count()
    }

    /// Count of rule-kind nodes.
    pub fn rule_count(&self) -> usize {
        self.nodes
            .values()
            .filter(|n| n.kind == NodeKind::Rule)
            .count()
    }

    /// Check if any node is truncated.
    pub fn has_truncated(&self) -> bool {
        self.nodes.values().any(|n| n.kind == NodeKind::Truncated)
    }

    /// Maximum depth from any root to a leaf.
    pub fn max_depth(&self) -> usize {
        let mut memo: HashMap<&str, usize> = HashMap::new();
        self.roots
            .iter()
            .map(|r| self.node_depth(r, &mut memo))
            .max()
            .unwrap_or(0)
    }

    fn node_depth<'a>(&'a self, id: &'a str, memo: &mut HashMap<&'a str, usize>) -> usize {
        if let Some(&d) = memo.get(id) {
            return d;
        }
        let d = match self.nodes.get(id) {
            Some(node) if !node.children.is_empty() => {
                1 + node
                    .children
                    .iter()
                    .map(|c| self.node_depth(c, memo))
                    .max()
                    .unwrap_or(0)
            }
            Some(_) => 1,
            None => 0,
        };
        memo.insert(id, d);
        d
    }

    /// Serialize to JSON.
    pub fn to_json(&self) -> Result<serde_json::Value, serde_json::Error> {
        serde_json::to_value(self)
    }

    /// Format as a human-readable tree string for CLI output.
    pub fn format_tree(&self) -> String {
        let mut output = String::new();
        for root_id in &self.roots {
            self.format_node_recursive(root_id, &mut output, "", true, &mut Vec::new());
        }
        output
    }

    fn format_node_recursive(
        &self,
        id: &str,
        output: &mut String,
        prefix: &str,
        is_last: bool,
        visited: &mut Vec<String>,
    ) {
        let node = match self.nodes.get(id) {
            Some(n) => n,
            None => return,
        };

        let connector = if prefix.is_empty() {
            ""
        } else if is_last {
            "└── "
        } else {
            "├── "
        };
        let child_prefix = if prefix.is_empty() {
            String::new()
        } else if is_last {
            format!("{prefix}    ")
        } else {
            format!("{prefix}│   ")
        };

        let label = match node.kind {
            NodeKind::Fact => "base",
            NodeKind::Rule => "rule",
            NodeKind::Negation => "negation",
            NodeKind::VectorSearch => "vector_search",
            NodeKind::Aggregate => "aggregate",
            NodeKind::Truncated => "truncated",
            NodeKind::WhyNot => "why_not",
        };

        let conclusion_str = format!(
            "{}({})",
            node.conclusion.pred,
            node.conclusion
                .args
                .iter()
                .map(|v| format!("{v}"))
                .collect::<Vec<_>>()
                .join(", ")
        );

        output.push_str(&format!("{prefix}{connector}[{label}] {conclusion_str}\n"));

        if let Some(rule_id) = &node.rule_id {
            output.push_str(&format!("{child_prefix}  rule: {rule_id}\n"));
        }
        if let Some(bindings) = &node.bindings {
            if !bindings.is_empty() {
                let bind_str: Vec<String> =
                    bindings.iter().map(|(k, v)| format!("{k}={v}")).collect();
                output.push_str(&format!(
                    "{child_prefix}  bindings: {}\n",
                    bind_str.join(", ")
                ));
            }
        }
        if let Some(agg) = &node.aggregate {
            output.push_str(&format!(
                "{child_prefix}  {}({}) = {} ({} inputs)\n",
                agg.func, agg.value_var, agg.result, agg.contributing_count
            ));
        }
        if let Some(trunc) = &node.truncated {
            output.push_str(&format!(
                "{child_prefix}  depth limit: {}\n",
                trunc.depth_limit
            ));
        }
        if let Some(why_not) = &node.why_not {
            output.push_str(&format!("{child_prefix}  blocker: {}\n", why_not.blocker));
        }

        // Cycle detection for display
        if visited.contains(&id.to_string()) {
            output.push_str(&format!("{child_prefix}  (see above)\n"));
            return;
        }
        visited.push(id.to_string());

        for (i, child_id) in node.children.iter().enumerate() {
            let last = i == node.children.len() - 1;
            self.format_node_recursive(child_id, output, &child_prefix, last, visited);
        }

        visited.pop();
    }
}

impl Default for DerivationGraph {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for DerivationGraph {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.format_tree())
    }
}

/// What kind of derivation step this node represents.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NodeKind {
    /// Base fact from an EDB relation.
    Fact,
    /// Rule application producing a derived fact.
    Rule,
    /// Negation evidence: no matching tuple exists.
    Negation,
    /// Vector similarity search result.
    VectorSearch,
    /// Aggregate computation (count, sum, min, max, avg).
    Aggregate,
    /// Proof was truncated at the configured depth limit.
    Truncated,
    /// Negative explanation: why a tuple was NOT derived.
    WhyNot,
}

/// Where a fact node's data comes from.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FactSource {
    /// Base fact from an extensional database relation (user-inserted data).
    Edb,
    /// Derived fact that the engine materialized but we can't trace further.
    Derived,
}

/// A single node in the derivation graph.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DerivationNode {
    /// What kind of derivation step.
    pub kind: NodeKind,
    /// The concluded tuple: predicate name + argument values.
    pub conclusion: Conclusion,
    /// For Fact nodes: whether this is base data (edb) or engine-derived.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<FactSource>,
    /// The rule clause text that produced this derivation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rule_id: Option<String>,
    /// Variable bindings used in this derivation step.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(
        default,
        serialize_with = "serialize_opt_bindings",
        deserialize_with = "deserialize_opt_bindings"
    )]
    pub bindings: Option<HashMap<String, Value>>,
    /// Aggregate metadata (only for Aggregate kind).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aggregate: Option<AggregateInfo>,
    /// Negation metadata (only for Negation kind).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub negation: Option<NegationInfo>,
    /// Vector search metadata (only for VectorSearch kind).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_search: Option<VectorSearchInfo>,
    /// Truncation metadata (only for Truncated kind).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub truncated: Option<TruncatedInfo>,
    /// Why-not blocker metadata (only for WhyNot kind).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub why_not: Option<WhyNotInfo>,
    /// Child node IDs (premises that support this conclusion).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub children: Vec<NodeId>,
}

/// The concluded tuple of a derivation step.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Conclusion {
    /// Relation/predicate name.
    pub pred: String,
    /// Argument values (serialized as plain JSON values, not tagged enums).
    #[serde(
        serialize_with = "serialize_values",
        deserialize_with = "deserialize_values"
    )]
    pub args: Vec<Value>,
}

/// Metadata for aggregate derivation nodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregateInfo {
    /// Aggregate function name (count, sum, min, max, avg).
    #[serde(rename = "fn")]
    pub func: String,
    /// The variable being aggregated over.
    pub value_var: String,
    /// The computed aggregate result.
    #[serde(
        serialize_with = "serialize_value",
        deserialize_with = "deserialize_value"
    )]
    pub result: Value,
    /// Number of contributing input tuples.
    pub contributing_count: usize,
    /// Sample of contributing input tuples (capped for display).
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(
        default,
        serialize_with = "serialize_opt_values_2d",
        deserialize_with = "deserialize_opt_values_2d"
    )]
    pub sample_inputs: Option<Vec<Vec<Value>>>,
    /// Full contributing inputs (only in export/full mode).
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(
        default,
        serialize_with = "serialize_opt_values_2d",
        deserialize_with = "deserialize_opt_values_2d"
    )]
    pub full_inputs: Option<Vec<Vec<Value>>>,
}

/// Metadata for negation proof nodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NegationInfo {
    /// The pattern that was checked and found absent.
    pub pattern: String,
}

/// Metadata for vector search proof nodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorSearchInfo {
    pub index_name: String,
    pub metric: String,
    pub query_vector: Vec<f32>,
    pub result_id: i64,
    pub distance: f64,
    pub k: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ef_search: Option<usize>,
}

/// Metadata for truncated nodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TruncatedInfo {
    pub depth_limit: usize,
}

/// Why-not blocker metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WhyNotInfo {
    /// Which rule clause was attempted.
    pub rule_name: String,
    pub clause_index: usize,
    pub clause_text: String,
    /// The specific blocker that prevented derivation.
    pub blocker: super::Blocker,
}

/// Accumulates nodes during graph construction, deduplicating by (relation, values).
pub struct GraphBuilder {
    nodes: HashMap<NodeId, DerivationNode>,
    /// Dedup key: (relation, values) -> existing node ID
    seen: HashMap<(String, Vec<Value>), NodeId>,
    next_id: usize,
}

impl GraphBuilder {
    /// Create a new empty builder.
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            seen: HashMap::new(),
            next_id: 0,
        }
    }

    /// Allocate a fresh node ID.
    fn fresh_id(&mut self) -> NodeId {
        let id = format!("n{}", self.next_id);
        self.next_id += 1;
        id
    }

    /// Check if a node for this (relation, values) already exists, returning its ID.
    pub fn get_existing(&self, pred: &str, args: &[Value]) -> Option<&NodeId> {
        self.seen.get(&(pred.to_string(), args.to_vec()))
    }

    /// Insert a node. If a dedup key (pred, args) is provided and a node with
    /// the same key already exists, return the existing ID.
    pub fn insert(&mut self, node: DerivationNode) -> NodeId {
        let dedup_key = (node.conclusion.pred.clone(), node.conclusion.args.clone());

        // For fact nodes, deduplicate by conclusion
        if node.kind == NodeKind::Fact {
            if let Some(existing_id) = self.seen.get(&dedup_key) {
                return existing_id.clone();
            }
        }

        let id = self.fresh_id();
        self.seen.insert(dedup_key, id.clone());
        self.nodes.insert(id.clone(), node);
        id
    }

    /// Insert a node without deduplication (for why-not, truncated, etc.).
    pub fn insert_unique(&mut self, node: DerivationNode) -> NodeId {
        let id = self.fresh_id();
        self.nodes.insert(id.clone(), node);
        id
    }

    /// Finalize into a DerivationGraph with the given root IDs.
    pub fn finish(self, roots: Vec<NodeId>) -> DerivationGraph {
        DerivationGraph {
            version: 1,
            query: None,
            roots,
            nodes: self.nodes,
        }
    }
}

impl Default for GraphBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::Value;

    fn int(v: i32) -> Value {
        Value::Int32(v)
    }

    fn str_val(s: &str) -> Value {
        Value::string(s)
    }

    #[test]
    fn test_empty_graph() {
        let g = DerivationGraph::new();
        assert_eq!(g.version, 1);
        assert!(g.roots.is_empty());
        assert_eq!(g.node_count(), 0);
        assert_eq!(g.max_depth(), 0);
    }

    #[test]
    fn test_single_fact_node() {
        let mut builder = GraphBuilder::new();
        let id = builder.insert(DerivationNode {
            kind: NodeKind::Fact,
            conclusion: Conclusion {
                pred: "edge".into(),
                args: vec![int(1), int(2)],
            },
            rule_id: None,
            bindings: None,
            aggregate: None,
            negation: None,
            vector_search: None,
            truncated: None,
            why_not: None,
            source: None,
            children: vec![],
        });
        let graph = builder.finish(vec![id]);
        assert_eq!(graph.node_count(), 1);
        assert_eq!(graph.fact_count(), 1);
        assert_eq!(graph.rule_count(), 0);
        assert_eq!(graph.max_depth(), 1);
        assert!(!graph.has_truncated());
    }

    #[test]
    fn test_fact_deduplication() {
        let mut builder = GraphBuilder::new();
        let id1 = builder.insert(DerivationNode {
            kind: NodeKind::Fact,
            conclusion: Conclusion {
                pred: "edge".into(),
                args: vec![int(1), int(2)],
            },
            rule_id: None,
            bindings: None,
            aggregate: None,
            negation: None,
            vector_search: None,
            truncated: None,
            why_not: None,
            source: None,
            children: vec![],
        });
        let id2 = builder.insert(DerivationNode {
            kind: NodeKind::Fact,
            conclusion: Conclusion {
                pred: "edge".into(),
                args: vec![int(1), int(2)],
            },
            rule_id: None,
            bindings: None,
            aggregate: None,
            negation: None,
            vector_search: None,
            truncated: None,
            why_not: None,
            source: None,
            children: vec![],
        });
        assert_eq!(id1, id2, "same fact should deduplicate");
        let graph = builder.finish(vec![id1]);
        assert_eq!(graph.node_count(), 1, "only one node for deduplicated fact");
    }

    #[test]
    fn test_rule_with_children() {
        let mut builder = GraphBuilder::new();
        let fact_id = builder.insert(DerivationNode {
            kind: NodeKind::Fact,
            conclusion: Conclusion {
                pred: "edge".into(),
                args: vec![int(1), int(2)],
            },
            rule_id: None,
            bindings: None,
            aggregate: None,
            negation: None,
            vector_search: None,
            truncated: None,
            why_not: None,
            source: None,
            children: vec![],
        });
        let rule_id = builder.insert(DerivationNode {
            kind: NodeKind::Rule,
            conclusion: Conclusion {
                pred: "path".into(),
                args: vec![int(1), int(2)],
            },
            rule_id: Some("path(X, Y) <- edge(X, Y)".into()),
            bindings: Some(
                [("X".into(), int(1)), ("Y".into(), int(2))]
                    .into_iter()
                    .collect(),
            ),
            aggregate: None,
            negation: None,
            vector_search: None,
            truncated: None,
            why_not: None,
            source: None,
            children: vec![fact_id],
        });
        let graph = builder.finish(vec![rule_id]);
        assert_eq!(graph.node_count(), 2);
        assert_eq!(graph.fact_count(), 1);
        assert_eq!(graph.rule_count(), 1);
        assert_eq!(graph.max_depth(), 2);
    }

    #[test]
    fn test_diamond_deduplication() {
        // A depends on B and C, both B and C depend on D
        // D should only appear once in the graph
        let mut builder = GraphBuilder::new();
        let d = builder.insert(DerivationNode {
            kind: NodeKind::Fact,
            conclusion: Conclusion {
                pred: "base".into(),
                args: vec![int(1)],
            },
            rule_id: None,
            bindings: None,
            aggregate: None,
            negation: None,
            vector_search: None,
            truncated: None,
            why_not: None,
            source: None,
            children: vec![],
        });
        let b = builder.insert(DerivationNode {
            kind: NodeKind::Rule,
            conclusion: Conclusion {
                pred: "mid_b".into(),
                args: vec![int(1)],
            },
            rule_id: Some("mid_b(X) <- base(X)".into()),
            bindings: None,
            aggregate: None,
            negation: None,
            vector_search: None,
            truncated: None,
            why_not: None,
            source: None,
            children: vec![d.clone()],
        });
        // Insert D again - should return same ID
        let d2 = builder.insert(DerivationNode {
            kind: NodeKind::Fact,
            conclusion: Conclusion {
                pred: "base".into(),
                args: vec![int(1)],
            },
            rule_id: None,
            bindings: None,
            aggregate: None,
            negation: None,
            vector_search: None,
            truncated: None,
            why_not: None,
            source: None,
            children: vec![],
        });
        assert_eq!(d, d2, "D should be deduplicated");
        let c = builder.insert(DerivationNode {
            kind: NodeKind::Rule,
            conclusion: Conclusion {
                pred: "mid_c".into(),
                args: vec![int(1)],
            },
            rule_id: Some("mid_c(X) <- base(X)".into()),
            bindings: None,
            aggregate: None,
            negation: None,
            vector_search: None,
            truncated: None,
            why_not: None,
            source: None,
            children: vec![d2],
        });
        let a = builder.insert(DerivationNode {
            kind: NodeKind::Rule,
            conclusion: Conclusion {
                pred: "top".into(),
                args: vec![int(1)],
            },
            rule_id: Some("top(X) <- mid_b(X), mid_c(X)".into()),
            bindings: None,
            aggregate: None,
            negation: None,
            vector_search: None,
            truncated: None,
            why_not: None,
            source: None,
            children: vec![b, c],
        });
        let graph = builder.finish(vec![a]);
        // 4 nodes: top, mid_b, mid_c, base (D only once)
        assert_eq!(graph.node_count(), 4, "diamond should have 4 nodes, not 5");
        assert_eq!(graph.fact_count(), 1);
    }

    #[test]
    fn test_aggregate_node() {
        let mut builder = GraphBuilder::new();
        let child1 = builder.insert(DerivationNode {
            kind: NodeKind::Fact,
            conclusion: Conclusion {
                pred: "can_reach".into(),
                args: vec![str_val("berlin"), str_val("dubai")],
            },
            rule_id: None,
            bindings: None,
            aggregate: None,
            negation: None,
            vector_search: None,
            truncated: None,
            why_not: None,
            source: None,
            children: vec![],
        });
        let child2 = builder.insert(DerivationNode {
            kind: NodeKind::Fact,
            conclusion: Conclusion {
                pred: "can_reach".into(),
                args: vec![str_val("berlin"), str_val("tokyo")],
            },
            rule_id: None,
            bindings: None,
            aggregate: None,
            negation: None,
            vector_search: None,
            truncated: None,
            why_not: None,
            source: None,
            children: vec![],
        });
        let agg = builder.insert(DerivationNode {
            kind: NodeKind::Aggregate,
            conclusion: Conclusion {
                pred: "reachable_count".into(),
                args: vec![str_val("berlin"), Value::Int64(2)],
            },
            rule_id: Some("reachable_count(City, count<Dest>) <- can_reach(City, Dest)".into()),
            bindings: Some([("City".into(), str_val("berlin"))].into_iter().collect()),
            aggregate: Some(AggregateInfo {
                func: "count".into(),
                value_var: "Dest".into(),
                result: Value::Int64(2),
                contributing_count: 2,
                sample_inputs: None,
                full_inputs: None,
            }),
            negation: None,
            vector_search: None,
            truncated: None,
            why_not: None,
            source: None,
            children: vec![child1, child2],
        });
        let graph = builder.finish(vec![agg]);
        assert_eq!(graph.node_count(), 3);
        let root = &graph.nodes[&graph.roots[0]];
        assert_eq!(root.kind, NodeKind::Aggregate);
        assert_eq!(root.children.len(), 2);
        assert!(root.aggregate.is_some());
    }

    #[test]
    fn test_json_roundtrip() {
        let mut builder = GraphBuilder::new();
        let fact = builder.insert(DerivationNode {
            kind: NodeKind::Fact,
            conclusion: Conclusion {
                pred: "edge".into(),
                args: vec![int(1), int(2)],
            },
            rule_id: None,
            bindings: None,
            aggregate: None,
            negation: None,
            vector_search: None,
            truncated: None,
            why_not: None,
            source: None,
            children: vec![],
        });
        let graph = builder.finish(vec![fact]);
        let json = graph.to_json().expect("serialize");
        assert_eq!(json["version"], 1);
        assert!(json["roots"].is_array());
        assert!(json["nodes"].is_object());

        // Roundtrip
        let json_str = serde_json::to_string(&graph).expect("serialize string");
        let restored: DerivationGraph = serde_json::from_str(&json_str).expect("deserialize");
        assert_eq!(restored.node_count(), graph.node_count());
        assert_eq!(restored.roots.len(), graph.roots.len());
    }

    #[test]
    fn test_json_shape_matches_spec() {
        let mut builder = GraphBuilder::new();
        let fact = builder.insert(DerivationNode {
            kind: NodeKind::Fact,
            conclusion: Conclusion {
                pred: "flight".into(),
                args: vec![
                    str_val("berlin"),
                    str_val("dubai"),
                    str_val("euro_air"),
                    Value::Float64(5.5),
                ],
            },
            rule_id: None,
            bindings: None,
            aggregate: None,
            negation: None,
            vector_search: None,
            truncated: None,
            why_not: None,
            source: None,
            children: vec![],
        });
        let rule = builder.insert(DerivationNode {
            kind: NodeKind::Rule,
            conclusion: Conclusion {
                pred: "can_reach".into(),
                args: vec![str_val("berlin"), str_val("dubai")],
            },
            rule_id: Some("can_reach(A, B) <- direct_flight(A, B, _, _)".into()),
            bindings: Some(
                [
                    ("A".into(), str_val("berlin")),
                    ("B".into(), str_val("dubai")),
                ]
                .into_iter()
                .collect(),
            ),
            aggregate: None,
            negation: None,
            vector_search: None,
            truncated: None,
            why_not: None,
            source: None,
            children: vec![fact],
        });
        let graph = builder.finish(vec![rule]);
        let json = graph.to_json().expect("serialize");

        // Verify spec shape
        assert_eq!(json["version"], 1);
        let root_id = json["roots"][0].as_str().unwrap();
        let root_node = &json["nodes"][root_id];
        assert_eq!(root_node["kind"], "rule");
        assert_eq!(root_node["conclusion"]["pred"], "can_reach");
        assert!(root_node["conclusion"]["args"].is_array());
        assert!(root_node["rule_id"].is_string());
        assert!(root_node["bindings"].is_object());
        assert!(root_node["children"].is_array());

        let child_id = root_node["children"][0].as_str().unwrap();
        let child_node = &json["nodes"][child_id];
        assert_eq!(child_node["kind"], "fact");
        assert_eq!(child_node["conclusion"]["pred"], "flight");
    }

    #[test]
    fn test_format_tree_output() {
        let mut builder = GraphBuilder::new();
        let fact = builder.insert(DerivationNode {
            kind: NodeKind::Fact,
            conclusion: Conclusion {
                pred: "edge".into(),
                args: vec![int(1), int(2)],
            },
            rule_id: None,
            bindings: None,
            aggregate: None,
            negation: None,
            vector_search: None,
            truncated: None,
            why_not: None,
            source: None,
            children: vec![],
        });
        let rule = builder.insert(DerivationNode {
            kind: NodeKind::Rule,
            conclusion: Conclusion {
                pred: "path".into(),
                args: vec![int(1), int(2)],
            },
            rule_id: Some("path(X, Y) <- edge(X, Y)".into()),
            bindings: None,
            aggregate: None,
            negation: None,
            vector_search: None,
            truncated: None,
            why_not: None,
            source: None,
            children: vec![fact],
        });
        let graph = builder.finish(vec![rule]);
        let output = graph.format_tree();
        assert!(output.contains("[rule] path(1, 2)"), "got: {output}");
        assert!(output.contains("[base] edge(1, 2)"), "got: {output}");
    }

    #[test]
    fn test_why_not_node() {
        use super::super::Blocker;
        let mut builder = GraphBuilder::new();
        let id = builder.insert_unique(DerivationNode {
            kind: NodeKind::WhyNot,
            conclusion: Conclusion {
                pred: "path".into(),
                args: vec![int(1), int(99)],
            },
            source: None,
            rule_id: Some("path(X, Y) <- edge(X, Y)".into()),
            bindings: None,
            aggregate: None,
            negation: None,
            vector_search: None,
            truncated: None,
            why_not: Some(WhyNotInfo {
                rule_name: "path".into(),
                clause_index: 0,
                clause_text: "path(X, Y) <- edge(X, Y)".into(),
                blocker: Blocker::BodyAtomFailed {
                    predicate_index: 0,
                    predicate_text: "edge(1, 99)".into(),
                    reason: "No matching tuples".into(),
                },
            }),
            children: vec![],
        });
        let graph = builder.finish(vec![id]);
        assert_eq!(graph.node_count(), 1);
        let root = &graph.nodes[&graph.roots[0]];
        assert_eq!(root.kind, NodeKind::WhyNot);
        assert!(root.why_not.is_some());
    }

    #[test]
    fn test_truncated_node() {
        let mut builder = GraphBuilder::new();
        let id = builder.insert_unique(DerivationNode {
            kind: NodeKind::Truncated,
            conclusion: Conclusion {
                pred: "chain".into(),
                args: vec![int(0), int(100)],
            },
            rule_id: None,
            bindings: None,
            aggregate: None,
            negation: None,
            vector_search: None,
            truncated: Some(TruncatedInfo { depth_limit: 50 }),
            why_not: None,
            source: None,
            children: vec![],
        });
        let graph = builder.finish(vec![id]);
        assert!(graph.has_truncated());
    }
}
