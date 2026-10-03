//! Turning genbi-trust SQL into IQL: checks become standing queries, ordered
//! mutations become write programs.
//!
//! Translation is mechanical: equality filters bind constants in a single
//! relation atom, selected columns are projected from the full rows the
//! engine returns, and every statement that does not fit is an
//! [`Unsupported`] finding for that scenario.

use std::fmt;

use serde_json::Value;

use super::binding::{self, Mirror};
use super::sql::{self, Eq, Literal, Statement};
use super::suite::Seed;

/// Why a check or mutation could not be translated.
#[derive(Debug, Clone, PartialEq)]
pub struct Unsupported(pub String);

impl fmt::Display for Unsupported {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A seed relation with the SQL column name of each argument.
#[derive(Debug, Clone, PartialEq)]
pub struct Relation {
    pub name: String,
    pub columns: Vec<String>,
}

impl Relation {
    fn position(&self, column: &str) -> Result<usize, Unsupported> {
        self.columns
            .iter()
            .position(|c| c == column)
            .ok_or_else(|| Unsupported(format!("relation {} has no column {column}", self.name)))
    }

    /// `name(arg, ...)` with `filter` bound and a fresh variable elsewhere.
    fn atom(&self, filter: &[Eq]) -> Result<String, Unsupported> {
        let mut args: Vec<String> = (0..self.columns.len()).map(|i| format!("C{i}")).collect();
        for eq in filter {
            args[self.position(&eq.column)?] = eq.value.to_string();
        }
        Ok(format!("{}({})", self.name, args.join(", ")))
    }

    fn fact(&self, values: &[String]) -> String {
        format!("{}({})", self.name, values.join(", "))
    }
}

/// A business question as a standing query over the engine.
#[derive(Debug, Clone, PartialEq)]
pub struct Standing {
    /// `?relation(...)`, subscribed by the agent and re-run as the baseline.
    pub query: String,
    /// Positions of the SQL-selected columns within each result row.
    pub projection: Vec<usize>,
}

impl Standing {
    /// The SQL-shaped row of a full engine row.
    pub fn project(&self, row: &[Value]) -> Vec<Value> {
        self.projection
            .iter()
            .map(|&i| row.get(i).cloned().unwrap_or(Value::Null))
            .collect()
    }
}

/// One translated mutation statement.
#[derive(Debug, Clone, PartialEq)]
pub enum Change {
    /// Ready IQL statements.
    Program(Vec<String>),
    /// Needs the current matching rows first (untimed); see [`Update`].
    Update(Update),
}

/// `UPDATE`: read the matching facts, then retract and re-insert each with
/// the assignments applied, rewriting any [`Mirror`] relation alongside.
#[derive(Debug, Clone, PartialEq)]
pub struct Update {
    /// `?relation(...)` selecting the facts to change.
    pub read: String,
    relation: Relation,
    assignments: Vec<(usize, String)>,
    mirrors: Vec<(Relation, Vec<usize>)>,
}

impl Update {
    /// The write program for the facts `current` returned by [`Update::read`].
    pub fn program(&self, current: &[Vec<Value>]) -> Vec<String> {
        let (mut retract, mut insert) = (Vec::new(), Vec::new());
        for row in current {
            let old: Vec<String> = row.iter().map(iql_value).collect();
            let mut new = old.clone();
            for (position, value) in &self.assignments {
                new[*position].clone_from(value);
            }
            if old == new {
                continue;
            }
            retract.push(format!("-{}", self.relation.fact(&old)));
            insert.push(format!("+{}", self.relation.fact(&new)));
            for (mirror, positions) in &self.mirrors {
                let pick = |values: &[String]| -> Vec<String> {
                    positions.iter().map(|&i| values[i].clone()).collect()
                };
                let (old, new) = (pick(&old), pick(&new));
                if old != new {
                    retract.push(format!("-{}", mirror.fact(&old)));
                    insert.push(format!("+{}", mirror.fact(&new)));
                }
            }
        }
        retract.dedup();
        insert.dedup();
        retract.extend(insert);
        retract
    }
}

/// SQL names resolved against one scenario's seed.
pub struct Catalog<'a> {
    seed: &'a Seed,
}

impl<'a> Catalog<'a> {
    pub fn new(seed: &'a Seed) -> Self {
        Self { seed }
    }

    /// The seed relation behind a SQL table or view.
    pub fn relation(&self, sql_name: &str) -> Result<Relation, Unsupported> {
        if let Some(view) = binding::view(sql_name) {
            if !self.seed.defines(view.relation) {
                return Err(Unsupported(format!(
                    "seed does not define {} (for view {sql_name})",
                    view.relation
                )));
            }
            return Ok(Relation {
                name: view.relation.to_string(),
                columns: view.columns.iter().map(ToString::to_string).collect(),
            });
        }
        match self.seed.schemas.get(sql_name) {
            Some(columns) => Ok(Relation {
                name: sql_name.to_string(),
                columns: columns.clone(),
            }),
            None => Err(Unsupported(format!(
                "seed declares no relation for {sql_name}"
            ))),
        }
    }

    pub fn standing(&self, sql: &str) -> Result<Standing, Unsupported> {
        let statement = sql::parse(sql).map_err(|e| Unsupported(format!("check: {e}")))?;
        let Statement::Select {
            columns,
            relation,
            filter,
        } = statement
        else {
            return Err(Unsupported("check is not a SELECT".into()));
        };
        let relation = self.relation(&relation)?;
        Ok(Standing {
            query: format!("?{}", relation.atom(&filter)?),
            projection: columns
                .iter()
                .map(|c| relation.position(c))
                .collect::<Result<_, _>>()?,
        })
    }

    /// Translate one mutation script, statement by statement.
    pub fn mutation(&self, sql: &str) -> Result<Vec<Change>, Unsupported> {
        let statements =
            sql::parse_script(sql).map_err(|e| Unsupported(format!("mutation: {e}")))?;
        statements.iter().map(|s| self.change(s)).collect()
    }

    fn change(&self, statement: &Statement) -> Result<Change, Unsupported> {
        match statement {
            Statement::Insert { relation, values } => {
                let target = self.writable(relation)?;
                if values.len() != target.columns.len() {
                    return Err(Unsupported(format!(
                        "INSERT into {relation}: {} values for {} columns",
                        values.len(),
                        target.columns.len()
                    )));
                }
                let values: Vec<String> = values.iter().map(Literal::to_string).collect();
                Ok(Change::Program(vec![format!("+{}", target.fact(&values))]))
            }
            Statement::Delete { relation, filter } => {
                let target = self.writable(relation)?;
                let atom = target.atom(filter)?;
                let fully_bound = filter.len() == target.columns.len()
                    && (0..target.columns.len())
                        .all(|i| filter.iter().any(|eq| eq.column == target.columns[i]));
                Ok(Change::Program(vec![if fully_bound {
                    format!("-{atom}")
                } else {
                    format!("-{atom} <- {atom}")
                }]))
            }
            Statement::Update {
                relation,
                set,
                filter,
            } => {
                let target = self.base(relation)?;
                let assignments = set
                    .iter()
                    .map(|eq| Ok((target.position(&eq.column)?, eq.value.to_string())))
                    .collect::<Result<_, Unsupported>>()?;
                let mirrors = binding::mirrors_of(relation)
                    .map(|m| self.mirror(m, &target))
                    .collect::<Result<_, _>>()?;
                Ok(Change::Update(Update {
                    read: format!("?{}", target.atom(filter)?),
                    relation: target,
                    assignments,
                    mirrors,
                }))
            }
            Statement::Select { .. } => Err(Unsupported("SELECT in a mutation".into())),
        }
    }

    /// A base relation with no mirror: one an `INSERT`/`DELETE` may change.
    fn writable(&self, table: &str) -> Result<Relation, Unsupported> {
        if binding::mirrors_of(table).next().is_some() {
            return Err(Unsupported(format!(
                "only UPDATE keeps {table}'s mirror relations in step"
            )));
        }
        self.base(table)
    }

    fn base(&self, table: &str) -> Result<Relation, Unsupported> {
        if binding::view(table).is_some() {
            return Err(Unsupported(format!("cannot write to view {table}")));
        }
        self.relation(table)
    }

    fn mirror(
        &self,
        mirror: &Mirror,
        table: &Relation,
    ) -> Result<(Relation, Vec<usize>), Unsupported> {
        let relation = self.seed.schemas.get(mirror.relation).ok_or_else(|| {
            Unsupported(format!(
                "seed declares no mirror relation {}",
                mirror.relation
            ))
        })?;
        let positions = mirror
            .columns
            .iter()
            .map(|c| table.position(c))
            .collect::<Result<_, _>>()?;
        Ok((
            Relation {
                name: mirror.relation.to_string(),
                columns: relation.clone(),
            },
            positions,
        ))
    }
}

/// IQL spelling of a value the engine returned.
pub fn iql_value(value: &Value) -> String {
    match value {
        Value::String(s) => Value::String(s.clone()).to_string(),
        Value::Number(n) if n.is_f64() => format!("{:?}", n.as_f64().unwrap_or_default()),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seed() -> Seed {
        Seed::parse(
            "+edges(tenant: string,parent: string,child: string)\n\
             +benchmark_context(singleton: int,revision: int,period_start: string,period_end: string,as_of: string)\n\
             +clock(as_of: string)\n\
             +period(start: string,end: string)\n\
             +visible_order(T,R,O,A,B,S,N) <- x(T,R,O,A,B,S,N)\n",
        )
    }

    #[test]
    fn checks_bind_filters_and_project_columns() {
        let seed = seed();
        let standing = Catalog::new(&seed)
            .standing("SELECT order_id,net_cents FROM model_visible_orders WHERE tenant='t1' AND reader='analyst' ORDER BY order_id")
            .unwrap();
        assert_eq!(
            standing.query,
            "?visible_order(\"t1\", \"analyst\", C2, C3, C4, C5, C6)"
        );
        assert_eq!(standing.projection, [2, 6]);
        let row: Vec<Value> = ["t1", "analyst", "o", "a", "b", "s"]
            .iter()
            .map(|v| Value::from(*v))
            .chain([Value::from(5)])
            .collect();
        assert_eq!(standing.project(&row), [Value::from("o"), Value::from(5)]);
    }

    #[test]
    fn unknown_relations_are_unsupported_not_guessed() {
        let seed = seed();
        let catalog = Catalog::new(&seed);
        assert!(catalog
            .standing("SELECT child FROM lineage WHERE parent='x'")
            .is_err());
        assert!(catalog.standing("SELECT a FROM job_state").is_err());
        assert!(catalog
            .mutation("INSERT INTO edges VALUES('t1','a')")
            .is_err());
        assert!(catalog
            .mutation("INSERT INTO benchmark_context VALUES(1,0,'a','b','c')")
            .is_err());
    }

    #[test]
    fn deletes_bind_what_the_filter_names() {
        let seed = seed();
        let catalog = Catalog::new(&seed);
        assert_eq!(
            catalog
                .mutation("DELETE FROM edges WHERE tenant='t1' AND parent='a' AND child='b';")
                .unwrap(),
            [Change::Program(vec!["-edges(\"t1\", \"a\", \"b\")".into()])]
        );
        assert_eq!(
            catalog
                .mutation("DELETE FROM edges WHERE tenant='t1'")
                .unwrap(),
            [Change::Program(vec![
                "-edges(\"t1\", C1, C2) <- edges(\"t1\", C1, C2)".into()
            ])]
        );
    }

    #[test]
    fn updates_rewrite_facts_and_mirrors() {
        let seed = seed();
        let changes = Catalog::new(&seed)
            .mutation("UPDATE benchmark_context SET as_of='2011-02-01'")
            .unwrap();
        let [Change::Update(update)] = changes.as_slice() else {
            panic!("{changes:?}")
        };
        assert_eq!(update.read, "?benchmark_context(C0, C1, C2, C3, C4)");
        let current = vec![vec![
            Value::from(1),
            Value::from(0),
            Value::from("2010-12-01"),
            Value::from("2012-01-01"),
            Value::from("2011-01-15"),
        ]];
        assert_eq!(
            update.program(&current),
            [
                "-benchmark_context(1, 0, \"2010-12-01\", \"2012-01-01\", \"2011-01-15\")",
                "-clock(\"2011-01-15\")",
                "+benchmark_context(1, 0, \"2010-12-01\", \"2012-01-01\", \"2011-02-01\")",
                "+clock(\"2011-02-01\")",
            ]
        );
        assert!(update.program(&[]).is_empty());
    }
}
