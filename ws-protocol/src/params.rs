//! Parameters of an `execute`: values bound to a program's `$name`
//! references, sent beside its text instead of inside it.
//!
//! # Wire form
//!
//! `params` is a JSON object from parameter name to value. A value is either
//! a bare JSON value, typed by its JSON form, or a one-key object naming its
//! type explicitly:
//!
//! | Type | Bare form | Explicit form |
//! |------|-----------|---------------|
//! | `int` (int64) | a number without fraction or exponent: `42` | `{"int": 42}`, `{"int": "9007199254740993"}` |
//! | `float` (finite f64) | a number with a fraction or exponent: `0.5`, `1e-3` | `{"float": 1}` |
//! | `string` | `"S-77"` | `{"string": "S-77"}` |
//! | `bool` | `true` | `{"bool": true}` |
//! | `vector` (of f64) | `[0.1, 0.2]` | `{"vector": [0.1, 0.2]}` |
//!
//! Nothing is converted silently. Refused, failing the whole frame:
//! `null` (what JavaScript's `JSON.stringify` makes of `NaN` and the
//! infinities), integers outside int64, a bare number that is integral and
//! at least 2^63 in magnitude (an int that does not fit, or a float written
//! without a fraction: send `{"float": ...}`), a bare negative zero (`-0`
//! and `-0.0` read alike: send `0` for an int or `{"float": -0.0}` for a
//! float), `{"float": n}` for an integer `n` within int64 or u64 range that
//! no f64 equals, `{"int": ...}` with a fraction, objects that are not one
//! known type tag, a parameter given twice, and names that are not
//! identifiers (`[A-Za-z_][A-Za-z0-9_]*`, at most [`MAX_PARAM_NAME_LEN`]
//! bytes). `{"float": n}` for a larger integer binds the f64 nearest `n`, as
//! the same float literal in IQL would.

use std::collections::BTreeMap;
use std::fmt;

use serde::de::{self, Deserializer, MapAccess, SeqAccess, Visitor};
use serde::ser::{SerializeMap, Serializer};
use serde::{Deserialize, Serialize};

/// Longest accepted parameter name, in bytes.
pub const MAX_PARAM_NAME_LEN: usize = 64;

/// 2^63: the smallest magnitude an `i64` cannot hold (except `i64::MIN`).
const TWO_POW_63: f64 = 9_223_372_036_854_775_808.0;

/// One parameter's value. Floats and vector elements are finite.
#[derive(Debug, Clone)]
pub enum ParamValue {
    Int(i64),
    Float(f64),
    String(String),
    Bool(bool),
    Vector(Vec<f64>),
}

/// Floats compare by bit pattern, so equality is reflexive (no value is NaN
/// once validated) and distinguishes `0.0` from `-0.0`.
impl PartialEq for ParamValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Int(a), Self::Int(b)) => a == b,
            (Self::Float(a), Self::Float(b)) => a.to_bits() == b.to_bits(),
            (Self::String(a), Self::String(b)) => a == b,
            (Self::Bool(a), Self::Bool(b)) => a == b,
            (Self::Vector(a), Self::Vector(b)) => {
                a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.to_bits() == y.to_bits())
            }
            _ => false,
        }
    }
}

impl Eq for ParamValue {}

impl ParamValue {
    /// The type name used in the wire form and in errors.
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::Int(_) => "int",
            Self::Float(_) => "float",
            Self::String(_) => "string",
            Self::Bool(_) => "bool",
            Self::Vector(_) => "vector",
        }
    }

    /// Why this value cannot be bound, if it cannot: a float or vector
    /// element that is not finite. Values read from the wire never are.
    pub fn invalid(&self) -> Option<String> {
        match self {
            Self::Float(f) if !f.is_finite() => Some(format!("float {f} is not finite")),
            Self::Vector(v) => v
                .iter()
                .find(|x| !x.is_finite())
                .map(|x| format!("vector element {x} is not finite")),
            _ => None,
        }
    }

    /// Approximate size in bytes, for request size limits.
    pub fn size_bytes(&self) -> usize {
        match self {
            Self::Int(_) | Self::Float(_) | Self::Bool(_) => 8,
            Self::String(s) => s.len(),
            Self::Vector(v) => v.len() * 8,
        }
    }
}

impl From<i64> for ParamValue {
    fn from(v: i64) -> Self {
        Self::Int(v)
    }
}

impl From<f64> for ParamValue {
    fn from(v: f64) -> Self {
        Self::Float(v)
    }
}

impl From<bool> for ParamValue {
    fn from(v: bool) -> Self {
        Self::Bool(v)
    }
}

impl From<String> for ParamValue {
    fn from(v: String) -> Self {
        Self::String(v)
    }
}

impl From<&str> for ParamValue {
    fn from(v: &str) -> Self {
        Self::String(v.to_string())
    }
}

impl From<Vec<f64>> for ParamValue {
    fn from(v: Vec<f64>) -> Self {
        Self::Vector(v)
    }
}

/// Why a parameter name is not accepted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidParamName(String);

impl fmt::Display for InvalidParamName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for InvalidParamName {}

/// Whether `name` is a parameter name: an identifier of at most
/// [`MAX_PARAM_NAME_LEN`] bytes.
pub fn is_param_name(name: &str) -> bool {
    let mut chars = name.chars();
    name.len() <= MAX_PARAM_NAME_LEN
        && chars
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn check_name(name: &str) -> Result<(), InvalidParamName> {
    if is_param_name(name) {
        Ok(())
    } else {
        Err(InvalidParamName(format!(
            "parameter name {name:?} is not an identifier of at most {MAX_PARAM_NAME_LEN} bytes \
             ([A-Za-z_][A-Za-z0-9_]*)"
        )))
    }
}

/// The parameters of one `execute`, by name.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Params(BTreeMap<String, ParamValue>);

impl Params {
    pub fn new() -> Self {
        Self::default()
    }

    /// Bind `name` to `value`, replacing any earlier value.
    pub fn insert(
        &mut self,
        name: impl Into<String>,
        value: impl Into<ParamValue>,
    ) -> Result<(), InvalidParamName> {
        let name = name.into();
        check_name(&name)?;
        self.0.insert(name, value.into());
        Ok(())
    }

    /// `self` with `name` bound to `value`.
    pub fn with(
        mut self,
        name: impl Into<String>,
        value: impl Into<ParamValue>,
    ) -> Result<Self, InvalidParamName> {
        self.insert(name, value)?;
        Ok(self)
    }

    pub fn get(&self, name: &str) -> Option<&ParamValue> {
        self.0.get(name)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Names and values, in name order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &ParamValue)> {
        self.0.iter().map(|(name, value)| (name.as_str(), value))
    }

    /// Approximate size in bytes of names and values, for request size limits.
    pub fn size_bytes(&self) -> usize {
        self.0
            .iter()
            .map(|(name, value)| name.len() + value.size_bytes())
            .sum()
    }
}

impl Serialize for ParamValue {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Int(n) => serializer.serialize_i64(*n),
            // A bare integral float of 2^63 or more, or a negative zero,
            // reads back as ambiguous; the explicit form keeps every float a
            // float.
            Self::Float(f)
                if is_negative_zero(*f) || (f.fract() == 0.0 && f.abs() >= TWO_POW_63) =>
            {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("float", f)?;
                map.end()
            }
            Self::Float(f) => serializer.serialize_f64(*f),
            Self::String(s) => serializer.serialize_str(s),
            Self::Bool(b) => serializer.serialize_bool(*b),
            Self::Vector(v) => v.serialize(serializer),
        }
    }
}

impl Serialize for Params {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (name, value) in &self.0 {
            map.serialize_entry(name, value)?;
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for Params {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ParamsVisitor;

        impl<'de> Visitor<'de> for ParamsVisitor {
            type Value = Params;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("an object of parameter values")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Params, A::Error> {
                let mut params = BTreeMap::new();
                while let Some(name) = map.next_key::<String>()? {
                    check_name(&name).map_err(de::Error::custom)?;
                    if params.contains_key(&name) {
                        return Err(de::Error::custom(format!(
                            "parameter {name:?} is given twice"
                        )));
                    }
                    let value = map
                        .next_value_seed(AnyValue)
                        .map_err(|e| de::Error::custom(format!("parameter {name:?}: {e}")))?;
                    params.insert(name, value);
                }
                Ok(Params(params))
            }
        }

        deserializer.deserialize_map(ParamsVisitor)
    }
}

/// Reads one value, typed by its JSON form or its explicit tag.
struct AnyValue;

impl<'de> de::DeserializeSeed<'de> for AnyValue {
    type Value = ParamValue;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<ParamValue, D::Error> {
        deserializer.deserialize_any(Bare)
    }
}

/// The bare form of a value; an object is the explicit form.
struct Bare;

impl<'de> Visitor<'de> for Bare {
    type Value = ParamValue;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a string, number, boolean, array of numbers or {\"<type>\": value}")
    }

    fn visit_bool<E: de::Error>(self, v: bool) -> Result<ParamValue, E> {
        Ok(ParamValue::Bool(v))
    }

    fn visit_i64<E: de::Error>(self, v: i64) -> Result<ParamValue, E> {
        Ok(ParamValue::Int(v))
    }

    fn visit_u64<E: de::Error>(self, v: u64) -> Result<ParamValue, E> {
        int_from_u64(v)
    }

    fn visit_f64<E: de::Error>(self, v: f64) -> Result<ParamValue, E> {
        if v.fract() == 0.0 && v.abs() >= TWO_POW_63 {
            return Err(E::custom(format!(
                "{v} is integral and too large for an int: send {{\"float\": {v}}} for a float, \
                 or {{\"int\": \"<digits>\"}} for an int within int64"
            )));
        }
        if is_negative_zero(v) {
            return Err(E::custom(
                "a bare negative zero is ambiguous: send 0 for an int, \
                 or {\"float\": -0.0} for a float",
            ));
        }
        float(v)
    }

    fn visit_str<E: de::Error>(self, v: &str) -> Result<ParamValue, E> {
        Ok(ParamValue::String(v.to_string()))
    }

    fn visit_string<E: de::Error>(self, v: String) -> Result<ParamValue, E> {
        Ok(ParamValue::String(v))
    }

    fn visit_unit<E: de::Error>(self) -> Result<ParamValue, E> {
        Err(null())
    }

    fn visit_none<E: de::Error>(self) -> Result<ParamValue, E> {
        Err(null())
    }

    fn visit_seq<A: SeqAccess<'de>>(self, seq: A) -> Result<ParamValue, A::Error> {
        vector(seq)
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<ParamValue, A::Error> {
        let Some(tag) = map.next_key::<String>()? else {
            return Err(de::Error::custom(
                "an explicit value is {\"<type>\": value}, not {}",
            ));
        };
        let value = match tag.as_str() {
            "int" => map.next_value_seed(Typed::Int)?,
            "float" => map.next_value_seed(Typed::Float)?,
            "string" => ParamValue::String(map.next_value()?),
            "bool" => ParamValue::Bool(map.next_value()?),
            "vector" => map.next_value_seed(Typed::Vector)?,
            other => {
                return Err(de::Error::custom(format!(
                    "unknown type {other:?}: expected int, float, string, bool or vector"
                )))
            }
        };
        if map.next_key::<de::IgnoredAny>()?.is_some() {
            return Err(de::Error::custom(
                "an explicit value has exactly one key, its type",
            ));
        }
        Ok(value)
    }
}

fn null<E: de::Error>() -> E {
    E::custom(
        "null is not a value (JSON.stringify writes NaN and the infinities as null); \
         parameters are finite numbers, strings, booleans or vectors",
    )
}

fn is_negative_zero(v: f64) -> bool {
    v == 0.0 && v.is_sign_negative()
}

fn int_from_u64<E: de::Error>(v: u64) -> Result<ParamValue, E> {
    i64::try_from(v)
        .map(ParamValue::Int)
        .map_err(|_| E::custom(format!("{v} is out of range for an int (int64)")))
}

fn float<E: de::Error>(v: f64) -> Result<ParamValue, E> {
    if v.is_finite() {
        Ok(ParamValue::Float(v))
    } else {
        Err(E::custom(format!("float {v} is not finite")))
    }
}

/// The f64 equal to integer `v`, if one is.
#[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
fn exact_f64<E: de::Error>(v: i128) -> Result<f64, E> {
    let f = v as f64;
    // An f64 holding an integer below 2^127 converts back exactly.
    if f as i128 == v {
        Ok(f)
    } else {
        Err(E::custom(format!("{v} has no exact float value")))
    }
}

fn vector<'de, A: SeqAccess<'de>>(mut seq: A) -> Result<ParamValue, A::Error> {
    let mut values = Vec::with_capacity(seq.size_hint().unwrap_or(0).min(4096));
    while let Some(element) = seq.next_element_seed(Typed::Float)? {
        let ParamValue::Float(x) = element else {
            unreachable!("Typed::Float yields floats");
        };
        values.push(x);
    }
    Ok(ParamValue::Vector(values))
}

/// The value of an explicit `{"<type>": value}`.
#[derive(Clone, Copy)]
enum Typed {
    Int,
    Float,
    Vector,
}

impl<'de> de::DeserializeSeed<'de> for Typed {
    type Value = ParamValue;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<ParamValue, D::Error> {
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for Typed {
    type Value = ParamValue;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Int => "an integer, or a string of decimal digits",
            Self::Float => "a number",
            Self::Vector => "an array of numbers",
        })
    }

    fn visit_i64<E: de::Error>(self, v: i64) -> Result<ParamValue, E> {
        match self {
            Self::Int => Ok(ParamValue::Int(v)),
            Self::Float => exact_f64(i128::from(v)).map(ParamValue::Float),
            Self::Vector => Err(E::invalid_type(de::Unexpected::Signed(v), &self)),
        }
    }

    fn visit_u64<E: de::Error>(self, v: u64) -> Result<ParamValue, E> {
        match self {
            Self::Int => int_from_u64(v),
            Self::Float => exact_f64(i128::from(v)).map(ParamValue::Float),
            Self::Vector => Err(E::invalid_type(de::Unexpected::Unsigned(v), &self)),
        }
    }

    fn visit_f64<E: de::Error>(self, v: f64) -> Result<ParamValue, E> {
        match self {
            Self::Float => float(v),
            Self::Int if is_negative_zero(v) => Ok(ParamValue::Int(0)),
            Self::Int => Err(E::custom(format!(
                "{v} is not an integer: an int has no fraction or exponent"
            ))),
            Self::Vector => Err(E::invalid_type(de::Unexpected::Float(v), &self)),
        }
    }

    fn visit_str<E: de::Error>(self, v: &str) -> Result<ParamValue, E> {
        match self {
            Self::Int
                if !v.is_empty()
                    && v.strip_prefix('-')
                        .unwrap_or(v)
                        .bytes()
                        .all(|b| b.is_ascii_digit()) =>
            {
                v.parse::<i64>()
                    .map(ParamValue::Int)
                    .map_err(|_| E::custom(format!("{v} is out of range for an int (int64)")))
            }
            _ => Err(E::invalid_type(de::Unexpected::Str(v), &self)),
        }
    }

    fn visit_unit<E: de::Error>(self) -> Result<ParamValue, E> {
        Err(null())
    }

    fn visit_seq<A: SeqAccess<'de>>(self, seq: A) -> Result<ParamValue, A::Error> {
        match self {
            Self::Vector => vector(seq),
            _ => Err(de::Error::invalid_type(de::Unexpected::Seq, &self)),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::float_cmp)]
mod tests {
    use super::*;

    fn read(json: &str) -> Result<Params, String> {
        serde_json::from_str::<Params>(json).map_err(|e| e.to_string())
    }

    fn one(json: &str) -> Result<ParamValue, String> {
        read(&format!(r#"{{"p":{json}}}"#)).map(|params| params.get("p").unwrap().clone())
    }

    #[test]
    fn bare_values_are_typed_by_their_json_form() {
        assert_eq!(one("42").unwrap(), ParamValue::Int(42));
        assert_eq!(one("-7").unwrap(), ParamValue::Int(-7));
        assert_eq!(one("1.0").unwrap(), ParamValue::Float(1.0));
        assert_eq!(one("1e-3").unwrap(), ParamValue::Float(1e-3));
        assert_eq!(one("0").unwrap(), ParamValue::Int(0));
        assert_eq!(one("0.0").unwrap(), ParamValue::Float(0.0));
        assert_eq!(one(r#""S-77""#).unwrap(), ParamValue::String("S-77".into()));
        assert_eq!(one("true").unwrap(), ParamValue::Bool(true));
        assert_eq!(
            one("[0.5, 2, -1e2]").unwrap(),
            ParamValue::Vector(vec![0.5, 2.0, -100.0])
        );
        assert_eq!(one("[]").unwrap(), ParamValue::Vector(vec![]));
        assert_eq!(
            one(&i64::MAX.to_string()).unwrap(),
            ParamValue::Int(i64::MAX)
        );
        assert_eq!(
            one(&i64::MIN.to_string()).unwrap(),
            ParamValue::Int(i64::MIN)
        );
    }

    #[test]
    fn explicit_values_carry_their_type() {
        assert_eq!(one(r#"{"float": 1}"#).unwrap(), ParamValue::Float(1.0));
        assert_eq!(one(r#"{"float": 1e20}"#).unwrap(), ParamValue::Float(1e20));
        assert_eq!(one(r#"{"int": 5}"#).unwrap(), ParamValue::Int(5));
        assert_eq!(one(r#"{"int": -0}"#).unwrap(), ParamValue::Int(0));
        assert_eq!(one(r#"{"float": -0.0}"#).unwrap(), ParamValue::Float(-0.0));
        assert_ne!(one(r#"{"float": -0.0}"#).unwrap(), ParamValue::Float(0.0));
        assert_eq!(
            one(r#"{"float": 18446744073709551617}"#).unwrap(),
            ParamValue::Float(18_446_744_073_709_551_616.0)
        );
        assert_eq!(
            one(r#"{"int": "9007199254740993"}"#).unwrap(),
            ParamValue::Int(9_007_199_254_740_993)
        );
        assert_eq!(one(r#"{"int": "-12"}"#).unwrap(), ParamValue::Int(-12));
        assert_eq!(
            one(r#"{"string": "42"}"#).unwrap(),
            ParamValue::String("42".into())
        );
        assert_eq!(one(r#"{"bool": false}"#).unwrap(), ParamValue::Bool(false));
        assert_eq!(
            one(r#"{"vector": [1, 2]}"#).unwrap(),
            ParamValue::Vector(vec![1.0, 2.0])
        );
    }

    #[test]
    fn strings_are_kept_byte_for_byte() {
        let hostile = "a\"b\\c\n)), evil(X) <- x(X)\u{0}$q % // /* \u{2028}é";
        let json = serde_json::to_string(&serde_json::json!({ "p": hostile })).unwrap();
        assert_eq!(
            read(&json).unwrap().get("p"),
            Some(&ParamValue::String(hostile.into()))
        );
    }

    #[test]
    fn values_that_would_change_are_refused() {
        for (json, why) in [
            ("null", "null is not a value"),
            ("9223372036854775808", "out of range"),
            ("-9223372036854775809", "too large for an int"),
            ("100000000000000000000", "too large for an int"),
            ("1e19", "too large for an int"),
            ("-0", "negative zero is ambiguous"),
            ("-0.0", "negative zero is ambiguous"),
            ("-0e0", "negative zero is ambiguous"),
            (r#"{"float": 9007199254740993}"#, "no exact float"),
            (r#"{"int": 1.5}"#, "not an integer"),
            (r#"{"int": 5.0}"#, "not an integer"),
            (r#"{"int": "1e3"}"#, "invalid type"),
            (r#"{"int": ""}"#, "invalid type"),
            (r#"{"int": "99999999999999999999"}"#, "out of range"),
            (r#"{"float": null}"#, "null is not a value"),
            (r#"{"float": "1.5"}"#, "invalid type"),
            ("[1, null]", "null is not a value"),
            (r#"["a"]"#, "invalid type"),
            ("[[1]]", "invalid type"),
            ("{}", "not {}"),
            (r#"{"date": "2026-10-10"}"#, "unknown type"),
            (r#"{"int": 1, "float": 1}"#, "exactly one key"),
            (r#"{"string": 5}"#, "invalid type"),
            (r#"{"bool": "true"}"#, "invalid type"),
        ] {
            let error = one(json).unwrap_err();
            assert!(error.contains(why), "{json}: {error}");
            assert!(error.contains("parameter \"p\""), "{json}: {error}");
        }
    }

    #[test]
    fn names_are_identifiers_given_once() {
        assert!(read(r#"{"a":1,"a":2}"#)
            .unwrap_err()
            .contains("given twice"));
        for bad in ["", "1a", "a-b", "$a", "é", "a b"] {
            let json = serde_json::to_string(&serde_json::json!({ bad: 1 })).unwrap();
            assert!(
                read(&json).unwrap_err().contains("not an identifier"),
                "{bad}"
            );
        }
        let long = "a".repeat(MAX_PARAM_NAME_LEN + 1);
        assert!(read(&format!(r#"{{"{long}":1}}"#)).is_err());
        let longest = "a".repeat(MAX_PARAM_NAME_LEN);
        assert!(read(&format!(r#"{{"{longest}":1}}"#)).is_ok());
        assert!(read(r#"{"_A9":1}"#).is_ok());
        assert!(Params::new().insert("a-b", 1).is_err());
        assert!(read("[1]").is_err());
    }

    #[test]
    fn serialization_reads_back_identically() {
        let params = Params::new()
            .with("i", i64::MIN)
            .unwrap()
            .with("f", 0.1)
            .unwrap()
            .with("whole", 2.0)
            .unwrap()
            .with("huge", 1e300)
            .unwrap()
            .with("neg_huge", -TWO_POW_63)
            .unwrap()
            .with("neg_zero", -0.0)
            .unwrap()
            .with("s", "x\"y")
            .unwrap()
            .with("b", true)
            .unwrap()
            .with("v", vec![1.0, 1e-7])
            .unwrap();
        let json = serde_json::to_string(&params).unwrap();
        assert_eq!(read(&json).unwrap(), params, "{json}");
    }

    #[test]
    fn floats_parse_exactly_as_rust_parses_their_text() {
        // Found by the Python SDK's round-trip fuzz: the default serde_json
        // parser reads this one ULP off.
        let text = "6.076327178933334e-236";
        let ParamValue::Float(f) = one(text).unwrap() else {
            panic!("not a float");
        };
        assert_eq!(f.to_bits(), text.parse::<f64>().unwrap().to_bits());

        // Every finite bit pattern, written the shortest way (as JS and
        // Python write floats), reads back to the same bits.
        let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
        for _ in 0..20_000 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let value = f64::from_bits(state);
            if !value.is_finite() || (value.fract() == 0.0 && value.abs() >= TWO_POW_63) {
                continue;
            }
            for text in [format!("{value:?}"), format!("{value:e}")] {
                let parsed = one(&text).unwrap();
                assert_eq!(parsed, ParamValue::Float(value), "{text}");
            }
        }
    }

    #[test]
    fn invalid_reports_non_finite_values() {
        assert!(ParamValue::Float(f64::NAN).invalid().is_some());
        assert!(ParamValue::Vector(vec![1.0, f64::INFINITY])
            .invalid()
            .is_some());
        assert!(ParamValue::Float(1.0).invalid().is_none());
    }
}
