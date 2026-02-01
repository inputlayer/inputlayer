//! Meta command parsing for InputLayer.
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
        assert!(matches!(cmd, MetaCommand::KgList));
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
}
