//! Lossless binary encoding of tuples for v2 batch files.
//!
//! Layout (little-endian): `u32` arity, then per value a tag byte and payload.
//! Variable-length payloads (strings, vectors) carry a `u32` element count.

use crate::value::{Tuple, Value};
use std::sync::Arc;

const INT32: u8 = 0;
const INT64: u8 = 1;
const FLOAT64: u8 = 2;
const STRING: u8 = 3;
const BOOL: u8 = 4;
const NULL: u8 = 5;
const VECTOR: u8 = 6;
const VECTOR_INT8: u8 = 7;
const TIMESTAMP: u8 = 8;

/// Append the encoding of `tuple` to `out`.
pub fn encode_tuple(tuple: &Tuple, out: &mut Vec<u8>) {
    put_len(out, tuple.arity());
    for value in tuple.values() {
        encode_value(value, out);
    }
}

fn put_len(out: &mut Vec<u8>, len: usize) {
    let len = u32::try_from(len).expect("persisted lengths fit in u32");
    out.extend_from_slice(&len.to_le_bytes());
}

fn encode_value(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::Int32(v) => {
            out.push(INT32);
            out.extend_from_slice(&v.to_le_bytes());
        }
        Value::Int64(v) => {
            out.push(INT64);
            out.extend_from_slice(&v.to_le_bytes());
        }
        Value::Float64(v) => {
            out.push(FLOAT64);
            out.extend_from_slice(&v.to_bits().to_le_bytes());
        }
        Value::String(s) => {
            out.push(STRING);
            put_len(out, s.len());
            out.extend_from_slice(s.as_bytes());
        }
        Value::Bool(b) => {
            out.push(BOOL);
            out.push(u8::from(*b));
        }
        Value::Null => out.push(NULL),
        Value::Vector(v) => {
            out.push(VECTOR);
            put_len(out, v.len());
            for x in v.iter() {
                out.extend_from_slice(&x.to_bits().to_le_bytes());
            }
        }
        Value::VectorInt8(v) => {
            out.push(VECTOR_INT8);
            put_len(out, v.len());
            out.extend(v.iter().map(|x| x.to_le_bytes()[0]));
        }
        Value::Timestamp(t) => {
            out.push(TIMESTAMP);
            out.extend_from_slice(&t.to_le_bytes());
        }
    }
}

/// Decode a tuple produced by [`encode_tuple`]. Rejects truncated, trailing or unknown data.
pub fn decode_tuple(bytes: &[u8]) -> Result<Tuple, String> {
    let mut r = Reader { bytes, pos: 0 };
    let arity = r.len()?;
    let values = (0..arity)
        .map(|_| r.value())
        .collect::<Result<Vec<_>, _>>()?;
    if r.pos != bytes.len() {
        return Err(format!("{} trailing bytes", bytes.len() - r.pos));
    }
    Ok(Tuple::new(values))
}

struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], String> {
        let end = self
            .pos
            .checked_add(n)
            .filter(|&end| end <= self.bytes.len())
            .ok_or_else(|| format!("truncated at byte {}", self.pos))?;
        let slice = &self.bytes[self.pos..end];
        self.pos = end;
        Ok(slice)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], String> {
        let mut buf = [0u8; N];
        buf.copy_from_slice(self.take(N)?);
        Ok(buf)
    }

    fn len(&mut self) -> Result<usize, String> {
        Ok(u32::from_le_bytes(self.array()?) as usize)
    }

    fn value(&mut self) -> Result<Value, String> {
        let [tag] = self.array()?;
        Ok(match tag {
            INT32 => Value::Int32(i32::from_le_bytes(self.array()?)),
            INT64 => Value::Int64(i64::from_le_bytes(self.array()?)),
            FLOAT64 => Value::Float64(f64::from_bits(u64::from_le_bytes(self.array()?))),
            STRING => {
                let n = self.len()?;
                let s = std::str::from_utf8(self.take(n)?).map_err(|e| e.to_string())?;
                Value::String(Arc::from(s))
            }
            BOOL => match self.array::<1>()? {
                [0] => Value::Bool(false),
                [1] => Value::Bool(true),
                [b] => return Err(format!("invalid bool byte {b}")),
            },
            NULL => Value::Null,
            VECTOR => {
                let n = self.len()?;
                let raw = self.take(n.checked_mul(4).ok_or("vector too long")?)?;
                let (chunks, _) = raw.as_chunks::<4>();
                let v = chunks
                    .iter()
                    .map(|c| f32::from_bits(u32::from_le_bytes(*c)))
                    .collect();
                Value::Vector(Arc::new(v))
            }
            VECTOR_INT8 => {
                let n = self.len()?;
                let v = self
                    .take(n)?
                    .iter()
                    .map(|&b| i8::from_le_bytes([b]))
                    .collect();
                Value::VectorInt8(Arc::new(v))
            }
            TIMESTAMP => Value::Timestamp(i64::from_le_bytes(self.array()?)),
            other => return Err(format!("unknown value tag {other}")),
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn roundtrip(values: Vec<Value>) {
        let tuple = Tuple::new(values);
        let mut buf = Vec::new();
        encode_tuple(&tuple, &mut buf);
        assert_eq!(decode_tuple(&buf).unwrap(), tuple);
    }

    #[test]
    fn every_variant_roundtrips() {
        roundtrip(vec![
            Value::Int32(i32::MIN),
            Value::Int64(i64::MAX),
            Value::Float64(-0.0),
            Value::Float64(f64::NAN),
            Value::string("héllo"),
            Value::string(""),
            Value::Bool(false),
            Value::Bool(true),
            Value::Null,
            Value::vector(vec![1.5, f32::INFINITY]),
            Value::vector(vec![]),
            Value::vector_int8(vec![-128, 127]),
            Value::Timestamp(-1),
        ]);
        roundtrip(vec![]);
    }

    #[test]
    fn nan_bits_preserved() {
        let nan = f64::from_bits(0x7ff8_0000_0000_1234);
        let mut buf = Vec::new();
        encode_tuple(&Tuple::new(vec![Value::Float64(nan)]), &mut buf);
        let back = decode_tuple(&buf).unwrap();
        match back.get(0) {
            Some(Value::Float64(f)) => assert_eq!(f.to_bits(), nan.to_bits()),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn rejects_corrupt_input() {
        let mut buf = Vec::new();
        encode_tuple(&Tuple::new(vec![Value::string("abc")]), &mut buf);
        assert!(decode_tuple(&buf[..buf.len() - 1]).is_err());
        let mut trailing = buf.clone();
        trailing.push(0);
        assert!(decode_tuple(&trailing).is_err());
        assert!(decode_tuple(&[1, 0, 0, 0, 99]).is_err());
        assert!(decode_tuple(&[1, 0, 0, 0, BOOL, 2]).is_err());
        assert!(decode_tuple(&[1, 0, 0, 0, VECTOR, 255, 255, 255, 255]).is_err());
    }
}
