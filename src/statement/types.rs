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
