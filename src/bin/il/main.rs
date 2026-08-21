//! `il` - the InputLayer product CLI.
//!
//! First slice: the ontology-registry verbs. Ontologies live in a Helm-style
//! registry (github.com/inputlayer/ontology-registry); `il` is the installer,
//! and deployment rides the engine's WebSocket API as one atomic program.
//!
//! ```text
//! il search [term]                  browse the registry index
//! il fetch <name>[@version]         download + digest-verify into the cache
//! il install <name>[@ver] --kg X    fetch, deploy over WS, pin pack_meta
//! il list [--kg X]                  what's installed where (via pack_meta)
//! il migration <verb>               generate | apply | revert | status
//! ```

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use inputlayer_ontology_client::registry::{self, Registry};
use inputlayer_ontology_client::ws;

#[derive(Parser)]
#[command(
    name = "il",
    about = "InputLayer CLI - install ontologies from the registry into a running engine",
    version
)]
struct Cli {
    /// Registry index URL
    #[arg(
        long,
        global = true,
        env = "INPUTLAYER_REGISTRY",
        default_value = registry::DEFAULT_INDEX_URL
    )]
    registry: String,

    /// Token for private registries (also read from GITHUB_TOKEN)
    #[arg(
        long,
        global = true,
        env = "INPUTLAYER_REGISTRY_TOKEN",
        hide_env_values = true
    )]
    registry_token: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// List ontologies in the registry, optionally filtered by a search term
    Search { term: Option<String> },
    /// Download an ontology into the local cache (sha256-verified)
    Fetch { spec: String },
    /// Install an ontology into a knowledge graph on a running engine
    Install {
        /// Ontology name, optionally pinned: name or name@version
        spec: String,
        /// Target knowledge graph
        #[arg(long)]
        kg: String,
        /// Create the knowledge graph if it does not exist
        #[arg(long)]
        create: bool,
        /// Engine URL
        #[arg(long, env = "INPUTLAYER_URL", default_value = "http://127.0.0.1:8080")]
        server: String,
        /// Engine API key
        #[arg(long, env = "INPUTLAYER_API_KEY", hide_env_values = true)]
        api_key: String,
    },
    /// Manage schema migrations (delegates to the Python SDK's migration tool)
    #[command(subcommand)]
    Migration(MigrationCommand),
    /// Show ontologies pinned in a knowledge graph (reads pack_meta)
    List {
        /// Knowledge graph to inspect
        #[arg(long)]
        kg: String,
        /// Engine URL
        #[arg(long, env = "INPUTLAYER_URL", default_value = "http://127.0.0.1:8080")]
        server: String,
        /// Engine API key
        #[arg(long, env = "INPUTLAYER_API_KEY", hide_env_values = true)]
        api_key: String,
    },
}

/// il owns the migration vocabulary; the delegation layer translates each
/// verb to the Python tool's subcommand, so Django-flavored names like
/// `makemigrations` never surface in the product CLI. Leaf commands pass
/// `--help` and all flags through untouched.
#[derive(Subcommand)]
enum MigrationCommand {
    /// Generate a migration from model changes
    #[command(disable_help_flag = true)]
    Generate {
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },
    /// Apply pending migrations to a knowledge graph
    #[command(disable_help_flag = true)]
    Apply {
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },
    /// Revert an applied migration
    #[command(disable_help_flag = true)]
    Revert {
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },
    /// Show applied and pending migrations
    #[command(disable_help_flag = true)]
    Status {
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },
}

impl MigrationCommand {
    /// (python subcommand, passthrough args)
    fn delegate(&self) -> (&'static str, &[String]) {
        match self {
            Self::Generate { args } => ("makemigrations", args),
            Self::Apply { args } => ("migrate", args),
            Self::Revert { args } => ("revert", args),
            Self::Status { args } => ("showmigrations", args),
        }
    }
}

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("error: {err:#}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let cli = Cli::parse();
    let token = cli
        .registry_token
        .clone()
        .or_else(|| std::env::var("GITHUB_TOKEN").ok());
    let reg = Registry::new(cli.registry.clone(), token);

    match cli.command {
        Command::Search { term } => search(&reg, term.as_deref()).await,
        Command::Fetch { spec } => {
            let dir = fetch(&reg, &spec).await?;
            println!("cached: {}", dir.display());
            Ok(())
        }
        Command::Install {
            spec,
            kg,
            create,
            server,
            api_key,
        } => install(&reg, &spec, &kg, create, &server, &api_key).await,
        Command::Migration(cmd) => migration(&cmd),
        Command::List {
            kg,
            server,
            api_key,
        } => list(&kg, &server, &api_key).await,
    }
}

/// The migrations tool lives in the Python SDK; `il` stays the single CLI by
/// delegating. Exit code and all flags pass through untouched.
fn migration(cmd: &MigrationCommand) -> Result<()> {
    let (subcommand, args) = cmd.delegate();
    match std::process::Command::new("inputlayer-migrate")
        .arg(subcommand)
        .args(args)
        .status()
    {
        Ok(status) => std::process::exit(status.code().unwrap_or(1)),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => anyhow::bail!(
            "inputlayer-migrate not found - migrations are provided by the Python SDK:\n  pip install inputlayer-client-dev"
        ),
        Err(err) => Err(err).context("failed to run inputlayer-migrate"),
    }
}

async fn search(reg: &Registry, term: Option<&str>) -> Result<()> {
    let index = reg.index().await?;
    println!("{:<24} {:<9} {:<10} TITLE", "ONTOLOGY", "LATEST", "ENGINE");
    let needle = term.map(str::to_lowercase);
    for (name, versions) in &index.entries {
        let Some(latest) = versions.first() else {
            continue;
        };
        let haystack = format!("{name} {}", latest.title).to_lowercase();
        if needle.as_deref().is_some_and(|n| !haystack.contains(n)) {
            continue;
        }
        println!(
            "{name:<24} {:<9} {:<10} {}",
            latest.version, latest.engine, latest.title
        );
    }
    Ok(())
}

async fn fetch(reg: &Registry, spec: &str) -> Result<std::path::PathBuf> {
    let (name, entry) = reg.resolve(spec).await?;
    println!("fetching {name}@{} ...", entry.version);
    let dir = reg.fetch(&name, &entry).await?;
    println!("sha256 verified: {}", entry.digest);
    Ok(dir)
}

async fn install(
    reg: &Registry,
    spec: &str,
    kg: &str,
    create: bool,
    server: &str,
    api_key: &str,
) -> Result<()> {
    let (name, entry) = reg.resolve(spec).await?;
    let entry_dir = reg.fetch(&name, &entry).await?;
    let manifest = registry::read_manifest(&entry_dir)?;
    println!(
        "{} {} - {} ({})",
        manifest.ontology.name, manifest.ontology.version, manifest.ontology.title, entry.digest
    );

    let mut program = String::new();
    for rel in &manifest.ontology.rules {
        let path = entry_dir.join(rel);
        let text = std::fs::read_to_string(&path)
            .with_context(|| format!("cannot read rules file {}", path.display()))?;
        program.push_str(&text);
        program.push('\n');
    }

    let statement_count = program
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with("//"))
        .count();
    let mut engine = ws::Engine::connect(server, api_key).await?;
    println!(
        "deploying {name}@{} -> {kg} ({statement_count} statements, 1 round trip) ...",
        entry.version
    );
    inputlayer_ontology_client::deploy::deploy_pack(
        &mut engine,
        kg,
        create,
        &manifest.ontology.name,
        &manifest.ontology.version,
        &entry.digest,
        &program,
    )
    .await?;
    println!(
        "ok {}@{} -> {kg} (pinned in pack_meta)",
        manifest.ontology.name, manifest.ontology.version
    );
    Ok(())
}

async fn list(kg: &str, server: &str, api_key: &str) -> Result<()> {
    let mut engine = ws::Engine::connect(server, api_key).await?;
    engine
        .execute(&format!(".kg use {kg}"))
        .await
        .with_context(|| format!("failed to switch to knowledge graph '{kg}'"))?;
    let result = match engine.execute("?pack_meta(Name, Version, Digest)").await {
        Ok(result) => result,
        // Only an unknown-relation error means "nothing installed"; anything
        // else (timeout, auth, disconnect) must not masquerade as an answer.
        Err(err)
            if {
                let msg = err.to_string().to_lowercase();
                msg.contains("unknown relation") || msg.contains("not found")
            } =>
        {
            println!("{kg}: no managed installs (no pack_meta relation)");
            return Ok(());
        }
        Err(err) => return Err(err.context("failed to read pack_meta")),
    };
    println!("{:<14} {:<24} {:<9} DIGEST", "KG", "ONTOLOGY", "VERSION");
    for row in &result.rows {
        let cell = |i: usize| {
            row.get(i)
                .map(|v| v.as_str().map_or_else(|| v.to_string(), str::to_string))
                .unwrap_or_default()
        };
        println!("{kg:<14} {:<24} {:<9} {}", cell(0), cell(1), cell(2));
    }
    Ok(())
}
