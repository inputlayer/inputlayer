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
    /// Default: parse and execute (strict mode)
    #[default]
    Default,
    /// Replace: atomically replace rules/relations
    Replace,
    /// Merge: add to existing definitions
    Merge,
}

/// Parse a meta command
pub fn parse_meta_command(input: &str) -> Result<MetaCommand, String> {
    let input = input.trim_start_matches('.');
    let parts: Vec<&str> = input.split_whitespace().collect();

    if parts.is_empty() {
        return Err("Empty meta command".to_string());
    }

    match parts[0].to_lowercase().as_str() {
        "kg" => parse_kg_command(&parts),
        "rel" | "relation" => parse_rel_command(&parts),
        "rule" => parse_rule_command(&parts, input),
        "session" | "rules" => parse_session_command(&parts),
        "index" | "idx" => parse_index_command(&parts, input),
        "compact" => Ok(MetaCommand::Compact),
        "status" => Ok(MetaCommand::Status),
        "help" | "?" => Ok(MetaCommand::Help),
        "quit" | "exit" | "q" => Ok(MetaCommand::Quit),
        "load" => parse_load_command(&parts),
        _ => Err(format!("Unknown meta command: .{}", parts[0])),
    }
}

fn parse_kg_command(parts: &[&str]) -> Result<MetaCommand, String> {
    if parts.len() == 1 {
        Ok(MetaCommand::KgShow)
    } else {
        match parts[1].to_lowercase().as_str() {
            "list" => Ok(MetaCommand::KgList),
            "create" => {
                if parts.len() < 3 {
                    Err("Usage: .kg create <name>".to_string())
                } else {
                    Ok(MetaCommand::KgCreate(parts[2].to_string()))
                }
            }
            "use" => {
                if parts.len() < 3 {
                    Err("Usage: .kg use <name>".to_string())
                } else {
                    Ok(MetaCommand::KgUse(parts[2].to_string()))
                }
            }
            "drop" => {
                if parts.len() < 3 {
                    Err("Usage: .kg drop <name>".to_string())
                } else {
                    Ok(MetaCommand::KgDrop(parts[2].to_string()))
                }
            }
            _ => Err(format!("Unknown kg subcommand: {}", parts[1])),
        }
    }
}

fn parse_rel_command(parts: &[&str]) -> Result<MetaCommand, String> {
    if parts.len() == 1 {
        Ok(MetaCommand::RelList)
    } else {
        Ok(MetaCommand::RelDescribe(parts[1].to_string()))
    }
}

fn parse_rule_command(parts: &[&str], input: &str) -> Result<MetaCommand, String> {
    if parts.len() == 1 {
        Ok(MetaCommand::RuleList)
    } else if parts[1].to_lowercase() == "list" {
        Ok(MetaCommand::RuleList)
    } else if parts[1].to_lowercase() == "drop" {
        if parts.len() < 3 {
            Err("Usage: .rule drop <name>".to_string())
        } else {
            Ok(MetaCommand::RuleDrop(parts[2].to_string()))
        }
    } else if parts[1].to_lowercase() == "def" {
        if parts.len() < 3 {
            Err("Usage: .rule def <name>".to_string())
        } else {
            Ok(MetaCommand::RuleShowDef(parts[2].to_string()))
        }
    } else if parts[1].to_lowercase() == "edit" {
        // .rule edit <name> <index> <rule>
        if parts.len() < 5 {
            Err("Usage: .rule edit <name> <index> <rule>\nExample: .rule edit connected 2 rule connected(x: int, z: int) :- edge(x, y), connected(y, z).".to_string())
        } else {
            let name = parts[2].to_string();
            let index: usize = parts[3]
                .parse()
                .map_err(|_| format!("Invalid index '{}': must be a number (1-based)", parts[3]))?;
            if index == 0 {
                return Err("Index must be 1 or greater (1-based indexing)".to_string());
            }
            // The rule is everything after the index
            let rule_start = input
                .find(parts[3])
                .ok_or_else(|| format!("Internal error: could not find '{}' in input", parts[3]))?
                + parts[3].len();
            let rule_text = input[rule_start..].trim().to_string();
            if rule_text.is_empty() {
                return Err("Missing rule definition".to_string());
            }
            Ok(MetaCommand::RuleEdit {
                name,
                index: index - 1,
                rule_text,
            }) // Convert to 0-based
        }
    } else if parts[1].to_lowercase() == "clear" {
        // .rule clear <name> - clear all rules
        if parts.len() < 3 {
            Err("Usage: .rule clear <name>".to_string())
        } else {
            Ok(MetaCommand::RuleClear(parts[2].to_string()))
        }
    } else if parts[1].to_lowercase() == "remove" {
        // .rule remove <name> <index> - remove specific clause
        if parts.len() < 4 {
            Err("Usage: .rule remove <name> <index>".to_string())
        } else {
            let name = parts[2].to_string();
            let index: usize = parts[3]
                .parse()
                .map_err(|_| format!("Invalid index '{}': must be a number (1-based)", parts[3]))?;
            if index == 0 {
                return Err("Index must be 1 or greater (1-based indexing)".to_string());
            }
            Ok(MetaCommand::RuleRemove {
                name,
                index: index - 1,
            }) // Convert to 0-based
        }
    } else {
        // .rule <name> - query the rule and show computed results
        Ok(MetaCommand::RuleQuery(parts[1].to_string()))
    }
}

