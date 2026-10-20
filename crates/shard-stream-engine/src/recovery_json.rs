//! Shared bounded native JSON shape walk, before typed manifest allocation.

use serde::de::{DeserializeSeed, MapAccess, SeqAccess, Visitor};
use std::fmt;

pub(crate) trait Budget {
    fn node(&mut self) -> bool;
    fn maximum_depth(&self) -> usize;
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum ShapeError {
    Decode,
    Trailing,
}

struct Shape<'a, B> {
    budget: &'a mut B,
    nodes: &'a mut usize,
    depth: usize,
}

impl<'de, B: Budget> DeserializeSeed<'de> for Shape<'_, B> {
    type Value = ();
    fn deserialize<D: serde::Deserializer<'de>>(self, decoder: D) -> Result<(), D::Error> {
        if !self.budget.node() {
            return Err(serde::de::Error::custom(
                "JSON cumulative node limit or expiry",
            ));
        }
        *self.nodes = self
            .nodes
            .checked_add(1)
            .ok_or_else(|| serde::de::Error::custom("JSON node count overflow"))?;
        if self.depth > self.budget.maximum_depth() {
            return Err(serde::de::Error::custom("JSON depth limit"));
        }
        decoder.deserialize_any(self)
    }
}

impl<'de, B: Budget> Visitor<'de> for Shape<'_, B> {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("bounded native JSON")
    }
    fn visit_bool<E: serde::de::Error>(self, _: bool) -> Result<(), E> {
        Ok(())
    }
    fn visit_i64<E: serde::de::Error>(self, _: i64) -> Result<(), E> {
        Ok(())
    }
    fn visit_u64<E: serde::de::Error>(self, _: u64) -> Result<(), E> {
        Ok(())
    }
    fn visit_f64<E: serde::de::Error>(self, _: f64) -> Result<(), E> {
        Ok(())
    }
    fn visit_str<E: serde::de::Error>(self, _: &str) -> Result<(), E> {
        Ok(())
    }
    fn visit_unit<E: serde::de::Error>(self) -> Result<(), E> {
        Ok(())
    }
    fn visit_seq<S: SeqAccess<'de>>(self, mut seq: S) -> Result<(), S::Error> {
        while seq
            .next_element_seed(Shape {
                budget: &mut *self.budget,
                nodes: &mut *self.nodes,
                depth: self.depth + 1,
            })?
            .is_some()
        {}
        Ok(())
    }
    fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<(), M::Error> {
        while map
            .next_key_seed(Shape {
                budget: &mut *self.budget,
                nodes: &mut *self.nodes,
                depth: self.depth + 1,
            })?
            .is_some()
        {
            map.next_value_seed(Shape {
                budget: &mut *self.budget,
                nodes: &mut *self.nodes,
                depth: self.depth + 1,
            })?;
        }
        Ok(())
    }
}

pub(crate) fn inspect<B: Budget>(bytes: &[u8], budget: &mut B) -> Result<usize, ShapeError> {
    let mut decoder = serde_json::Deserializer::from_slice(bytes);
    let mut nodes = 0;
    Shape {
        budget,
        nodes: &mut nodes,
        depth: 0,
    }
    .deserialize(&mut decoder)
    .map_err(|_| ShapeError::Decode)?;
    decoder.end().map_err(|_| ShapeError::Trailing)?;
    Ok(nodes)
}
