//! Meta command parsing for `InputLayer`.
//!
//! Meta commands are dot-prefixed: .kg, .rel, .rule, .session, etc.

/// Meta commands for knowledge graph/relation/rule management
#[derive(Debug, Clone, PartialEq)]
pub enum MetaCommand {
    // Knowledge graph commands
    KgShow,
    KgList,
    KgCreate(String),
    KgUse(String),
    KgDrop(String),

    // Relation commands
    RelList,
    RelDescribe(String),

    // Rule commands (persistent derived relations)
    RuleList,
    RuleQuery(String),   // .rule <name> - query the rule and show results
    RuleShowDef(String), // .rule def <name> - show rule definition
    RuleDrop(String),
    RuleEdit {
        // .rule edit <name> <index> <rule> - edit specific rule
        name: String,
        index: usize,
        rule_text: String,
    },
    RuleClear(String), // .rule clear <name> - clear all rules for re-registration
    RuleRemove {
        // .rule remove <name> <index> - remove specific clause by index
        name: String,
        index: usize,
    },

    // Session commands (transient rules)
    SessionList,        // .session - list session rules
    SessionClear,       // .session clear - clear all session rules
    SessionDrop(usize), // .session drop <n> - remove rule #n (0-based internally)

    // Index commands (HNSW and other indexes)
    IndexList,                       // .index list - list all indexes
    IndexCreate(IndexCreateOptions), // .index create <name> on <relation>(<column>) [options]
    IndexDrop(String),               // .index drop <name> - drop an index
    IndexStats(String),              // .index stats <name> - show index statistics
    IndexRebuild(String),            // .index rebuild <name> - force rebuild index

    // System commands
    Compact,
    Status,
    Help,
    Quit,

    // Load command: .load <file> [--replace|--merge]
    Load {
        path: String,
        mode: LoadMode,
    },
}

/// Options for creating an index
#[derive(Debug, Clone, PartialEq)]
pub struct IndexCreateOptions {
    /// Name of the index
    pub name: String,
    /// Relation to index
    pub relation: String,
    /// Column to index (for vector indexes)
    pub column: String,
    /// Index type (hnsw, btree, etc.)
    pub index_type: String,
    /// Distance metric for vector indexes (cosine, euclidean, dot_product, manhattan)
    pub metric: Option<String>,
    /// HNSW M parameter (connections per layer)
    pub m: Option<usize>,
    /// HNSW ef_construction parameter
    pub ef_construction: Option<usize>,
    /// HNSW ef_search parameter (default search quality)
    pub ef_search: Option<usize>,
}

/// Mode for loading files
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum LoadMode {
    /// Default: parse and execute (strict mode.clone())
    #[default]
    Default,
    /// Replace: atomically replace rules/relations
    Replace,
    /// Merge: add to existing definitions
    Merge,
}

/// Parse a meta command
