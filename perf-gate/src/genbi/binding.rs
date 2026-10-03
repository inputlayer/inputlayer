//! How genbi-trust's SQL names map onto the relations its IQL seeds define.
//!
//! Base tables keep their SQL name and take their columns from the seed's
//! schema declaration. The suite's SQLite *views* are the reference model of
//! each business question; each binding below names the seed relation with
//! the same meaning, column for column. A check against a view whose relation
//! the seed does not define is reported as unsupported, not approximated.

/// A SQLite view and the seed relation that derives the same rows.
pub struct ViewBinding {
    pub view: &'static str,
    pub relation: &'static str,
    /// The view's columns, in the relation's argument order.
    pub columns: &'static [&'static str],
}

pub const VIEWS: &[ViewBinding] = &[
    ViewBinding {
        view: "model_visible_orders",
        relation: "visible_order",
        columns: &[
            "tenant",
            "reader",
            "order_id",
            "account",
            "billing_country",
            "shipping_country",
            "net_cents",
        ],
    },
    ViewBinding {
        view: "model_members",
        relation: "member",
        columns: &["tenant", "account"],
    },
    ViewBinding {
        view: "model_blocked",
        relation: "blocked",
        columns: &["tenant", "account"],
    },
    ViewBinding {
        view: "model_eligible_accounts",
        relation: "eligible",
        columns: &["tenant", "account"],
    },
    ViewBinding {
        view: "model_reach",
        relation: "reach",
        columns: &["tenant", "ancestor", "descendant"],
    },
    ViewBinding {
        view: "model_refund_totals",
        relation: "refund_total",
        columns: &["tenant", "order_id", "refund_cents"],
    },
    // Recursive closure of `dependency`, without the reflexive pairs.
    ViewBinding {
        view: "lineage",
        relation: "depends_on",
        columns: &["parent", "child"],
    },
    // The view is the closure *including* the review seeds, which the seed
    // calls `trusted_review`; its own `derived_review` excludes the seeds.
    ViewBinding {
        view: "derived_review",
        relation: "trusted_review",
        columns: &["account"],
    },
];

/// A seed relation that projects columns of a SQL table: the seeds keep the
/// reporting clock and period in `clock` / `period` while the SQL model keeps
/// them in `benchmark_context`. An `UPDATE` of the table rewrites the mirror.
pub struct Mirror {
    pub table: &'static str,
    pub relation: &'static str,
    /// Table columns, in the mirror relation's argument order.
    pub columns: &'static [&'static str],
}

pub const MIRRORS: &[Mirror] = &[
    Mirror {
        table: "benchmark_context",
        relation: "clock",
        columns: &["as_of"],
    },
    Mirror {
        table: "benchmark_context",
        relation: "period",
        columns: &["period_start", "period_end"],
    },
];

pub fn view(name: &str) -> Option<&'static ViewBinding> {
    VIEWS.iter().find(|v| v.view == name)
}

pub fn mirrors_of(table: &str) -> impl Iterator<Item = &'static Mirror> + '_ {
    MIRRORS.iter().filter(move |m| m.table == table)
}
