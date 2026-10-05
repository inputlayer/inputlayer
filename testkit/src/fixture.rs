//! Named, reproducible knowledge-graph fixtures shared by the scenario suite
//! and the performance benches.
//!
//! A fixture is plain IQL: a knowledge graph name plus the statements that
//! build it. [`Fixture::install`] creates the graph on an engine over the
//! WebSocket, exactly as a deployment would load it.

use crate::client::WsClient;
use crate::contract::{Checked, Violation};
use crate::engine::Engine;

/// Facts per insert statement, keeping every request far below the frame limit.
const FACTS_PER_STATEMENT: usize = 1_000;
/// Bytes of statements [`Fixture::install`] commits in one program.
const PROGRAM_BYTES: usize = 256 * 1024;

/// A knowledge graph and the statements that build it.
#[derive(Debug, Clone)]
pub struct Fixture {
    /// Stable fixture name recorded with every latency sample.
    pub name: String,
    pub knowledge_graph: String,
    pub statements: Vec<String>,
}

impl Fixture {
    /// Empty fixture `name` on `knowledge_graph`.
    pub fn new(name: &str, knowledge_graph: &str) -> Self {
        Self {
            name: name.to_string(),
            knowledge_graph: knowledge_graph.to_string(),
            statements: Vec::new(),
        }
    }

    /// Insert `facts` (IQL tuples such as `(1, 2)`) into `relation`.
    #[must_use]
    pub fn facts(mut self, relation: &str, facts: impl IntoIterator<Item = String>) -> Self {
        let facts: Vec<String> = facts.into_iter().collect();
        for batch in facts.chunks(FACTS_PER_STATEMENT) {
            self.statements
                .push(format!("+{relation}[{}]", batch.join(", ")));
        }
        self
    }

    /// Add a persistent rule clause, e.g. `reach(X, Y) <- edge(X, Y)`.
    #[must_use]
    pub fn rule(mut self, clause: &str) -> Self {
        self.statements.push(format!("+{clause}"));
        self
    }

    /// Add any other statement, e.g. `.index create ...`.
    #[must_use]
    pub fn statement(mut self, statement: &str) -> Self {
        self.statements.push(statement.to_string());
        self
    }

    /// Create the knowledge graph on `engine` and run every statement.
    ///
    /// Consecutive statements commit together as one program of up to
    /// [`PROGRAM_BYTES`]; a meta command (`.index create ...`) runs alone.
    pub async fn install(&self, engine: &Engine) -> Checked<()> {
        let mut admin = WsClient::connect(engine, "default").await?;
        admin
            .commit(&format!(".kg create {}", self.knowledge_graph))
            .await?;
        admin.close().await;
        let mut writer = WsClient::connect(engine, &self.knowledge_graph).await?;
        for program in self.programs() {
            writer.commit(&program).await.map_err(|e| {
                Violation::Rejected(format!("fixture '{}' statement failed: {e}", self.name))
            })?;
        }
        writer.close().await;
        Ok(())
    }

    /// The statements grouped into the programs [`Self::install`] commits.
    fn programs(&self) -> Vec<String> {
        let mut programs = Vec::new();
        let mut program = String::new();
        for statement in &self.statements {
            let meta = statement.starts_with('.');
            if !program.is_empty() && (meta || program.len() + statement.len() > PROGRAM_BYTES) {
                programs.push(std::mem::take(&mut program));
            }
            if meta {
                programs.push(statement.clone());
                continue;
            }
            if !program.is_empty() {
                program.push('\n');
            }
            program.push_str(statement);
        }
        if !program.is_empty() {
            programs.push(program);
        }
        programs
    }
}

/// `edge` is a chain `0 -> 1 -> ... -> nodes-1`; `reach` is its transitive
/// closure. A bound standing query `?reach(0, X)` sees every node.
pub fn reachability_chain(knowledge_graph: &str, nodes: i64) -> Fixture {
    Fixture::new(&format!("reachability_chain_{nodes}"), knowledge_graph)
        .facts("edge", (1..nodes).map(|n| format!("({}, {n})", n - 1)))
        .rule("reach(X, Y) <- edge(X, Y)")
        .rule("reach(X, Z) <- reach(X, Y), edge(Y, Z)")
}

/// Scale of the [`Fixture::shop_pack`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Size {
    /// Hundreds of rows: every PR.
    Small,
    /// [`Size::Small`] plus item embeddings, their HNSW index `emb_idx` and
    /// the `near` similarity rule.
    Vector,
    /// About a million `link` edges: the benchmark host only.
    Lab,
}

impl Size {
    /// Items; `link` chains them in runs of [`CHAIN`].
    fn items(self) -> usize {
        match self {
            Self::Small | Self::Vector => 100,
            Self::Lab => 1_250_000,
        }
    }

    /// Orders besides the anchor order `o-42`.
    fn orders(self) -> usize {
        match self {
            Self::Small | Self::Vector => 60,
            Self::Lab => 100_000,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Small => "small",
            Self::Vector => "vector",
            Self::Lab => "lab",
        }
    }
}

/// Items per `link` chain `i{5k} -> i{5k+1} -> ... -> i{5k+4}`.
const CHAIN: usize = 5;
/// Dimensions of a [`Size::Vector`] embedding.
const DIMENSIONS: usize = 8;

/// The shop pack's base relation schemas.
pub const SHOP_SCHEMAS: [&str; 6] = [
    "order(id: string, cust: string, total: int)",
    "order_item(order: string, item: string)",
    "stock(item: string, qty: int)",
    "blocked(item: string)",
    "link(item: string, other: string)",
    "claim(order: string, item: string, agent: string)",
];

/// The shop pack's deployed rules: join, comparison, negation, recursion,
/// negation over recursion and an aggregate.
pub const SHOP_RULES: [&str; 5] = [
    r#"eligible(Order, Item, Why) <- order_item(Order, Item), stock(Item, Q), Q > 0, !blocked(Item), Why = "in_stock""#,
    "related(Item, Other) <- link(Item, Other)",
    "related(Item, Other) <- related(Item, Mid), link(Mid, Other)",
    "offer(Order, Other) <- eligible(Order, Item, _), related(Item, Other), !blocked(Other), !claim(Order, Other, _)",
    "n_eligible(Order, count<Item>) <- eligible(Order, Item, _)",
];

/// The [`Size::Vector`] rule: an item's five nearest neighbours by embedding
/// (itself included), by item number.
pub const NEAR_RULE: &str =
    r#"near(Item, Other, D) <- embedding(Item, V), hnsw_nearest("emb_idx", V, 5, Other, D)"#;

impl Fixture {
    /// The shop pack on knowledge graph `shop`: one fixture whose rules cover
    /// the constructs the view work must maintain, so scenarios exercise the
    /// related changes together (see `TESTING.md`, Scenario Suite).
    ///
    /// Base relations: `order(id, cust, total)`, `order_item(order, item)`,
    /// `stock(item, qty)`, `blocked(item)`, `link(item, other)`,
    /// `claim(order, item, agent)` (schemas in [`SHOP_SCHEMAS`]) and, in
    /// [`Size::Vector`], `embedding(item: int, vec: vector)` with its index
    /// `emb_idx`. Items are strings `i{n}` except in `embedding` and `near`,
    /// which key them by the number `n` (an HNSW index needs integer ids).
    /// Rules: [`SHOP_RULES`] (and [`NEAR_RULE`]).
    ///
    /// Anchors the scenarios rely on, at every size:
    /// - order `o-42` (the agent's key) holds `i1` and `i5`, in stock, and
    ///   `i13`, out of stock (`stock("i13", 0)`);
    /// - `link` chains `i0 -> i1 -> i2 -> i3 -> i4` and `i5 -> ... -> i9`,
    ///   so `offer("o-42", X)` includes `i2`-`i4` and `i6`-`i9`;
    /// - blocked items are `i{n}` with `n % 17 == 10` (`i10`, `i27`, ...);
    ///   nothing in `i0`-`i9` is blocked or claimed;
    /// - every other order `o-{n}` holds two generated items.
    ///
    /// Use another knowledge graph name by setting `knowledge_graph`.
    pub fn shop_pack(size: Size) -> Self {
        let items = size.items();
        let item = |n: usize| format!("\"i{n}\"");
        // Every 13th item is out of stock, i13 among them.
        let qty = |n: usize| (n * 7) % 13;
        let other_orders = (1..=size.orders() + 1).filter(|&n| n != 42);

        let mut order = vec![r#"("o-42", "c-2", 420)"#.to_string()];
        let mut order_item: Vec<String> = ["i1", "i5", "i13"]
            .iter()
            .map(|i| format!(r#"("o-42", "{i}")"#))
            .collect();
        for n in other_orders {
            order.push(format!(r#"("o-{n}", "c-{}", {})"#, n % 10, n * 10));
            for i in [(n * 3) % items, (n * 3 + items / 2) % items] {
                order_item.push(format!(r#"("o-{n}", {})"#, item(i)));
            }
        }
        let stock = (0..items).map(|n| format!("({}, {})", item(n), qty(n)));
        let blocked = (0..items)
            .filter(|n| n % 17 == 10)
            .map(|n| format!("({})", item(n)));
        let link = (0..items)
            .filter(|n| n % CHAIN != CHAIN - 1 && n + 1 < items)
            .map(|n| format!("({}, {})", item(n), item(n + 1)));
        let claim = (0..items)
            .filter(|n| n % 23 == 20)
            .map(|n| format!(r#"("o-1", {}, "a0")"#, item(n)));

        let mut fixture = Self::new(&format!("shop_pack_{}", size.name()), "shop");
        for schema in SHOP_SCHEMAS {
            fixture = fixture.statement(&format!("+{schema}"));
        }
        fixture = fixture
            .facts("order", order)
            .facts("order_item", order_item)
            .facts("stock", stock)
            .facts("blocked", blocked)
            .facts("link", link)
            .facts("claim", claim);
        for rule in SHOP_RULES {
            fixture = fixture.rule(rule);
        }
        if size == Size::Vector {
            let embedding = (0..items).map(|n| {
                let vector: Vec<String> = (0..DIMENSIONS)
                    .map(|d| format!("0.{:02}", (n * 31 + d * 17) % 100))
                    .collect();
                format!("({n}, [{}])", vector.join(", "))
            });
            fixture = fixture
                .statement("+embedding(item: int, vec: vector)")
                .facts("embedding", embedding)
                .statement(".index create emb_idx on embedding(vec) metric cosine")
                .rule(NEAR_RULE);
        }
        fixture
    }
}
