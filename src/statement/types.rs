//! Type system declarations for `InputLayer`.
//!
//! This module defines the type declaration syntax:
//! - `type Name: TypeExpr.`
//! - Type expressions: base types, lists, records, refined types

use serde::{Deserialize, Serialize};
use std::fmt;

/// Type declaration: type Name: `TypeExpr`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypeDecl {
    /// Type name (must be uppercase, e.g., Email, User)
    pub name: String,
    /// The type expression
    pub type_expr: TypeExpr,
}

/// Type expression (right-hand side of type declaration)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TypeExpr {
    /// Base type: int, string, bool
    Base(BaseType),
    /// Reference to another type by name
    TypeRef(String),
    /// List type: `list[T]`
    List(Box<TypeExpr>),
    /// Record type: { field: type, ... }
    Record(Vec<RecordField>),
    /// Refined type: `base_type(constraint1`, constraint2, ...)
    Refined {
        base: Box<TypeExpr>,
        refinements: Vec<Refinement>,
    },
}

impl TypeExpr {
    /// Convert to `SchemaType` for validation
    pub fn to_schema_type(&self) -> crate::schema::SchemaType {
        use crate::schema::SchemaType;
        match self {
            TypeExpr::Base(BaseType::Int) => SchemaType::Int,
            TypeExpr::Base(BaseType::Float) => SchemaType::Float,
            TypeExpr::Base(BaseType::String) => SchemaType::String,
            TypeExpr::Base(BaseType::Bool) => SchemaType::Bool,
            TypeExpr::TypeRef(name) => SchemaType::Named(name.clone()),
            TypeExpr::List(_) => SchemaType::Any, // Lists not directly supported yet
            TypeExpr::Record(_) => SchemaType::Any, // Records not directly supported yet
            TypeExpr::Refined { base, .. } => base.to_schema_type(), // Ignore refinements
        }
    }
}

/// Base types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum BaseType {
    Int,
    String,
    Bool,
    Float,
}

impl fmt::Display for BaseType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BaseType::Int => write!(f, "int"),
            BaseType::String => write!(f, "string"),
            BaseType::Bool => write!(f, "bool"),
            BaseType::Float => write!(f, "float"),
        }
    }
}

impl fmt::Display for TypeExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TypeExpr::Base(base) => write!(f, "{base}"),
            TypeExpr::TypeRef(name) => write!(f, "{name}"),
            TypeExpr::List(inner) => write!(f, "list[{inner}]"),
            TypeExpr::Record(fields) => {
                write!(f, "{{ ")?;
                for (i, field) in fields.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}: {}", field.name, field.field_type)?;
                }
                write!(f, " }}")
            }
            TypeExpr::Refined { base, refinements } => {
                write!(f, "{base}(")?;
                for (i, r) in refinements.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", r.name)?;
                    if !r.args.is_empty() {
                        write!(f, "(")?;
                        for (j, arg) in r.args.iter().enumerate() {
                            if j > 0 {
                                write!(f, ", ")?;
                            }
                            match arg {
                                RefinementArg::Int(n) => write!(f, "{n}")?,
                                RefinementArg::Float(n) => write!(f, "{n}")?,
                                RefinementArg::String(s) => write!(f, "\"{s}\"")?,
                                RefinementArg::Bool(b) => write!(f, "{b}")?,
                            }
                        }
                        write!(f, ")")?;
                    }
                }
                write!(f, ")")
            }
        }
    }
}

/// A field in a record type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordField {
    pub name: String,
    pub field_type: TypeExpr,
}

/// A refinement constraint on a type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Refinement {
    /// Refinement name (e.g., "range", "pattern", "`not_empty`")
    pub name: String,
    /// Arguments to the refinement
    pub args: Vec<RefinementArg>,
}

/// Argument to a refinement
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RefinementArg {
    Int(i64),
    Float(f64),
    String(String),
    Bool(bool),
}

// Parsing
/// Parse a type declaration: `type Name: TypeExpr.`
