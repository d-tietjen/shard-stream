use crate::{StorageError, StorageResult};

pub(crate) struct Decoder<'a> {
    bytes: &'a [u8],
    cursor: usize,
}

impl<'a> Decoder<'a> {
    pub(crate) const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, cursor: 0 }
    }

    pub(crate) fn u8(&mut self) -> StorageResult<u8> {
        Ok(self.take(1)?[0])
    }

    pub(crate) fn u32(&mut self) -> StorageResult<u32> {
        let bytes = self.array::<4>()?;
        Ok(u32::from_le_bytes(bytes))
    }

    pub(crate) fn u64(&mut self) -> StorageResult<u64> {
        let bytes = self.array::<8>()?;
        Ok(u64::from_le_bytes(bytes))
    }

    pub(crate) fn u128(&mut self) -> StorageResult<u128> {
        let bytes = self.array::<16>()?;
        Ok(u128::from_le_bytes(bytes))
    }

    pub(crate) fn take(&mut self, length: usize) -> StorageResult<&'a [u8]> {
        let end = self
            .cursor
            .checked_add(length)
            .ok_or_else(|| StorageError::InvalidInput("decoder cursor overflow".into()))?;
        let value = self
            .bytes
            .get(self.cursor..end)
            .ok_or_else(|| StorageError::InvalidInput("truncated encoded value".into()))?;
        self.cursor = end;
        Ok(value)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.cursor == self.bytes.len()
    }

    fn array<const N: usize>(&mut self) -> StorageResult<[u8; N]> {
        self.take(N)?
            .try_into()
            .map_err(|_| StorageError::InvalidInput("invalid fixed-width value".into()))
    }
}

pub(crate) fn push_u8(bytes: &mut Vec<u8>, value: u8) {
    bytes.push(value);
}

pub(crate) fn push_u32(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

pub(crate) fn push_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

pub(crate) fn push_u128(bytes: &mut Vec<u8>, value: u128) {
    bytes.extend_from_slice(&value.to_le_bytes());
}
