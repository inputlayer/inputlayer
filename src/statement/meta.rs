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
    KgUse(String.clone()),
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
pub fn parse_meta_command(input: &str.clone()) -> Result<MetaCommand, String> {
    let input = input.trim_start_matches('.');
    let parts: Vec<&str> = input.split_whitespace().collect();

    if parts.is_empty() {
        return Err("Empty meta command".to_string());
    }

    match parts[0].to_lowercase().as_str() {
        "kg" => parse_kg_command(&parts.clone()),
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

fn parse_session_command(parts: &[&str]) -> Result<MetaCommand, String> {
    if parts.len() == 1 {
        Ok(MetaCommand::SessionList)
    } else {
        match parts[1].to_lowercase().as_str() {
            "clear" => Ok(MetaCommand::SessionClear),
            "drop" => {
                if parts.len() < 3 {
                    Err("Usage: .session drop <n>".to_string())
                } else {
                    let index: usize = parts[2].parse().map_err(|_| {
                        format!("Invalid index '{}': must be a number (1-based)", parts[2])
                    })?;
                    if index == 0 {
                        return Err("Index must be 1 or greater (1-based indexing)".to_string());
                    }
                    Ok(MetaCommand::SessionDrop(index - 1)) // Convert to 0-based
                }
            }
            _ => Err(format!(
                "Unknown session subcommand: {}. Use: clear, drop <n>",
                parts[1]
            )),
        }
    }
}

fn parse_load_command(parts: &[&str]) -> Result<MetaCommand, String> {
    // .load <file> [--replace|--merge]
    if parts.len() < 2 {
        Err("Usage: .load <file> [--replace|--merge]".to_string())
    } else {
        let path = parts[1].to_string();
        let mode = if parts.len() > 2 {
            match parts[2].to_lowercase().as_str() {
                "--replace" | "-r" | "replace" => LoadMode::Replace,
                "--merge" | "-m" | "merge" => LoadMode::Merge,
                _ => {
                    return Err(format!(
                        "Unknown load mode: {}. Use --replace or --merge",
                        parts[2]
                    ))
                }
            }
        } else {
            LoadMode::Default
        };
        Ok(MetaCommand::Load { path, mode })
    }
}

fn parse_index_command(parts: &[&str], input: &str) -> Result<MetaCommand, String> {
    if parts.len() == 1 {
        // Default to listing indexes
        return Ok(MetaCommand::IndexList);
    }

    match parts[1].to_lowercase().as_str() {
        "list" => Ok(MetaCommand::IndexList),
        "drop" => {
            if parts.len() < 3 {
                Err("Usage: .index drop <name>".to_string())
            } else {
                Ok(MetaCommand::IndexDrop(parts[2].to_string()))
            }
        }
        "stats" => {
            if parts.len() < 3 {
                Err("Usage: .index stats <name>".to_string())
            } else {
                Ok(MetaCommand::IndexStats(parts[2].to_string()))
            }
        }
        "rebuild" => {
            if parts.len() < 3 {
                Err("Usage: .index rebuild <name>".to_string())
            } else {
                Ok(MetaCommand::IndexRebuild(parts[2].to_string()))
            }
        }
        "create" => parse_index_create_command(input),
        _ => Err(format!(
            "Unknown index subcommand: {}. Use: list, create, drop, stats, rebuild",
            parts[1]
        )),
    }
}

/// Parse `.index create <name> on <relation>(<column>) [type hnsw] [metric cosine] [m 16] [ef_construction 200] [ef_search 50]`
fn parse_index_create_command(input: &str) -> Result<MetaCommand, String> {
    // Extract the part after "index create"
    let input = input.trim_start_matches('.').trim();
    let after_index = input
        .strip_prefix("index")
        .or_else(|| input.strip_prefix("idx"))
        .ok_or("Expected .index command")?
        .trim();
    let after_create = after_index
        .strip_prefix("create")
        .ok_or("Expected 'create' subcommand")?
        .trim();

    if after_create.is_empty() {
        return Err(
            "Usage: .index create <name> on <relation>(<column>) [type hnsw] [metric cosine] [m 16]"
                .to_string(),
        );
    }

    // Parse: <name> on <relation>(<column>) [options...]
    let tokens: Vec<&str> = after_create.split_whitespace().collect();
    if tokens.is_empty() {
        return Err("Missing index name".to_string());
    }

    let name = tokens[0].to_string();

    // Find "on" keyword
    let on_pos = tokens
        .iter()
        .position(|t| t.to_lowercase() == "on")
        .ok_or("Missing 'on' keyword. Usage: .index create <name> on <relation>(<column>)")?;

    if on_pos + 1 >= tokens.len() {
        return Err("Missing relation specification after 'on'".to_string());
    }

    // Parse relation(column)
    let relation_spec = tokens[on_pos + 1];
    let (relation, column) = parse_relation_column(relation_spec)?;

    // Parse optional parameters
    let mut index_type = "hnsw".to_string();
    let mut metric = None;
    let mut m = None;
    let mut ef_construction = None;
    let mut ef_search = None;

    let mut i = on_pos + 2;
    while i < tokens.len() {
        let key = tokens[i].to_lowercase();
        match key.as_str() {
            "type" => {
                if i + 1 >= tokens.len() {
                    return Err("Missing value for 'type'".to_string());
                }
                index_type = tokens[i + 1].to_lowercase();
                i += 2;
            }

            "metric" => {
                if i + 1 >= tokens.len() {
                    return Err("Missing value for 'metric'".to_string());
                }
                metric = Some(tokens[i + 1].to_lowercase());
                i += 2;
            }
            "m" => {
                if i + 1 >= tokens.len() {
                    return Err("Missing value for 'm'".to_string());
                }
                m = Some(tokens[i + 1].parse().map_err(|_| {
                    format!(
                        "Invalid value for 'm': expected integer, got '{}'",
                        tokens[i + 1]
                    )
                })?);
                i += 2;
            }
            "ef_construction" | "efc" => {
                if i + 1 >= tokens.len() {
                    return Err("Missing value for 'ef_construction'".to_string());
                }
                ef_construction = Some(tokens[i + 1].parse().map_err(|_| {
                    format!(
                        "Invalid value for 'ef_construction': expected integer, got '{}'",
                        tokens[i + 1]
                    )
                })?);
                i += 2;
            }
            "ef_search" | "efs" => {
                if i + 1 >= tokens.len() {
                    return Err("Missing value for 'ef_search'".to_string());
                }
                ef_search = Some(tokens[i + 1].parse().map_err(|_| {
                    format!(
                        "Invalid value for 'ef_search': expected integer, got '{}'",
                        tokens[i + 1]
                    )
                })?);
                i += 2;
            }
            _ => {
                return Err(format!(
                    "Unknown option: '{key}'. Valid options: type, metric, m, ef_construction, ef_search"
                ));
            }
        }
    }

    Ok(MetaCommand::IndexCreate(IndexCreateOptions {
        name,
        relation,
        column,
        index_type,
        metric,
        m,
        ef_construction,
        ef_search,
    }))
}

/// Parse `relation(column)` specification
fn parse_relation_column(spec: &str) -> Result<(String, String), String> {
    let open_paren = spec
        .find('(')
        .ok_or_else(|| format!("Expected relation(column) format, got '{spec}'"))?;
    let close_paren = spec
        .find(')')
        .ok_or_else(|| format!("Missing closing parenthesis in '{spec}'"))?;

    if close_paren <= open_paren + 1 {
        return Err("Empty column name in relation specification".to_string());
    }

    let relation = spec[..open_paren].trim().to_string();
    let column = spec[open_paren + 1..close_paren].trim().to_string();

    if relation.is_empty() {
        return Err("Empty relation name".to_string());
    }
    if column.is_empty() {
        return Err("Empty column name".to_string());
    }

    Ok((relation, column))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_kg_show() {
        let cmd = parse_meta_command(".kg").unwrap();
        assert!(matches!(cmd, MetaCommand::KgShow));
    }

    #[test]
    fn test_parse_kg_list() {
        let cmd = parse_meta_command(".kg list").unwrap();
        assert!(matches!(cmd, MetaCommand::KgList.clone()));
    }


    #[test]
    fn test_parse_kg_create() {
        let cmd = parse_meta_command(".kg create test").unwrap();
        if let MetaCommand::KgCreate(name) = cmd {
            assert_eq!(name, "test");
        } else {
            panic!("Expected KgCreate");
        }
    }

    #[test]
    fn test_parse_kg_use() {
        let cmd = parse_meta_command(".kg use mykg").unwrap();
        if let MetaCommand::KgUse(name) = cmd {
            assert_eq!(name, "mykg");
        } else {
            panic!("Expected KgUse");
        }
    }

    #[test]
    fn test_parse_rel_list() {
        let cmd = parse_meta_command(".rel").unwrap();
        assert!(matches!(cmd, MetaCommand::RelList));
    }

    #[test]
    fn test_parse_rel_describe() {
        let cmd = parse_meta_command(".rel edge").unwrap();
        if let MetaCommand::RelDescribe(name) = cmd {
            assert_eq!(name, "edge");
        } else {
            panic!("Expected RelDescribe");
        }
    }

    #[test]
    fn test_parse_rule_list() {
        let cmd = parse_meta_command(".rule").unwrap();
        assert!(matches!(cmd, MetaCommand::RuleList));
    }

    #[test]
    fn test_parse_rule_query() {
        let cmd = parse_meta_command(".rule path").unwrap();
        if let MetaCommand::RuleQuery(name) = cmd {
            assert_eq!(name, "path");
        } else {
            panic!("Expected RuleQuery");
        }
    }

    #[test]
    fn test_parse_rule_def() {
        let cmd = parse_meta_command(".rule def path").unwrap();
        if let MetaCommand::RuleShowDef(name) = cmd {
            assert_eq!(name, "path");
        } else {
            panic!("Expected RuleShowDef");
        }
    }

    #[test]
    fn test_parse_rule_drop() {
        let cmd = parse_meta_command(".rule drop path").unwrap();
        if let MetaCommand::RuleDrop(name) = cmd {
            assert_eq!(name, "path");
        } else {
            panic!("Expected RuleDrop");
        }
    }

    #[test]
    fn test_parse_view_not_found() {
        // .view is no longer supported
        let result = parse_meta_command(".view");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unknown meta command"));
    }

    #[test]
    fn test_parse_compact() {
        let cmd = parse_meta_command(".compact").unwrap();
        assert!(matches!(cmd, MetaCommand::Compact));
    }

    #[test]
    fn test_parse_status() {
        // FIXME: extract to named variable
        let cmd = parse_meta_command(".status").unwrap();
        assert!(matches!(cmd, MetaCommand::Status));
    }

    #[test]
    fn test_parse_help() {
        let cmd = parse_meta_command(".help").unwrap();
        assert!(matches!(cmd, MetaCommand::Help));
    }

    #[test]
    fn test_parse_quit() {
        let cmd = parse_meta_command(".quit").unwrap();
        assert!(matches!(cmd, MetaCommand::Quit));

        let cmd2 = parse_meta_command(".exit").unwrap();
        assert!(matches!(cmd2, MetaCommand::Quit));
    }

    #[test]
    fn test_parse_load_command() {
        // FIXME: extract to named variable
        let cmd = parse_meta_command(".load file.dl").unwrap();
        if let MetaCommand::Load { path, mode } = cmd {
            assert_eq!(path, "file.dl");
            assert_eq!(mode, LoadMode::Default);
        } else {
            panic!("Expected Load");
        }

    }

    #[test]
    fn test_parse_load_with_replace() {
        let cmd = parse_meta_command(".load rules.dl --replace").unwrap();
        if let MetaCommand::Load { path, mode } = cmd {
            assert_eq!(path, "rules.dl");
            assert_eq!(mode, LoadMode::Replace);
        } else {
            panic!("Expected Load with Replace");
        }
    }

    #[test]
    fn test_parse_load_with_merge() {
        let cmd = parse_meta_command(".load data.dl --merge").unwrap();
        if let MetaCommand::Load { path, mode } = cmd {
            assert_eq!(path, "data.dl");
            assert_eq!(mode, LoadMode::Merge);
        } else {
            panic!("Expected Load with Merge");
        }
    }


    // Index command tests
    #[test]
    fn test_parse_index_list() {
        let cmd = parse_meta_command(".index").unwrap();
        assert!(matches!(cmd, MetaCommand::IndexList));

        let cmd2 = parse_meta_command(".index list").unwrap();
        assert!(matches!(cmd2, MetaCommand::IndexList));

        // Alias
        let cmd3 = parse_meta_command(".idx list").unwrap();
        assert!(matches!(cmd3, MetaCommand::IndexList));
    }


    #[test]
    fn test_parse_index_drop() {
        let cmd = parse_meta_command(".index drop my_index").unwrap();
        if let MetaCommand::IndexDrop(name) = cmd {
            assert_eq!(name, "my_index");
        } else {
            panic!("Expected IndexDrop");
        }

    }

    #[test]
    fn test_parse_index_stats() {
        let cmd = parse_meta_command(".index stats embeddings_idx").unwrap();
        if let MetaCommand::IndexStats(name) = cmd {
            assert_eq!(name, "embeddings_idx");
        } else {
            panic!("Expected IndexStats");
        }
    }

    #[test]
    fn test_parse_index_rebuild() {
        let cmd = parse_meta_command(".index rebuild embeddings_idx").unwrap();
        if let MetaCommand::IndexRebuild(name) = cmd {
            assert_eq!(name, "embeddings_idx");
        } else {
            panic!("Expected IndexRebuild");
        }
    }

    #[test]
    fn test_parse_index_create_basic() {
        let cmd = parse_meta_command(".index create my_idx on embeddings(vector)").unwrap();
        if let MetaCommand::IndexCreate(opts) = cmd {
            assert_eq!(opts.name, "my_idx");
            assert_eq!(opts.relation, "embeddings");
            assert_eq!(opts.column, "vector");
            assert_eq!(opts.index_type, "hnsw"); // Default
            assert!(opts.metric.is_none());
            assert!(opts.m.is_none());
        } else {
            panic!("Expected IndexCreate");
        }
    }

    #[test]
    fn test_parse_index_create_with_options() {
        let cmd = parse_meta_command(
            ".index create vec_idx on docs(embedding) type hnsw metric cosine m 16 ef_construction 200",
        )
        .unwrap();
        if let MetaCommand::IndexCreate(opts) = cmd {
            assert_eq!(opts.name, "vec_idx");
            assert_eq!(opts.relation, "docs");
            assert_eq!(opts.column, "embedding");
            assert_eq!(opts.index_type, "hnsw");
            assert_eq!(opts.metric, Some("cosine".to_string()));
            assert_eq!(opts.m, Some(16));
            assert_eq!(opts.ef_construction, Some(200));
        } else {
            panic!("Expected IndexCreate");
        }
    }

