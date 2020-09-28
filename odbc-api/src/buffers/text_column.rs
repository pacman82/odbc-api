use super::{BindColParameters, ColumnBuffer};
use odbc_sys::{CDataType, Len, Pointer, NULL_DATA};
use std::{cmp::min, convert::TryInto};

/// A buffer intended to be bound to a column of a cursor. Elements of the buffer will contain a
/// variable amount of characters up to a maximum string length. Since most SQL types have a string
/// representation this buffer can be bound to a column of almost any type, ODBC driver and driver
/// manager should take care of the conversion. Since elements of this type have variable length an
/// indicator buffer needs to be bound, wether the column is nullable or not, and therefore does not
/// matter for this buffer.
pub struct TextColumn {
    max_str_len: usize,
    values: Vec<u8>,
    indicators: Vec<Len>,
}

unsafe impl ColumnBuffer for TextColumn {
    fn bind_arguments(&mut self) -> BindColParameters {
        BindColParameters {
            target_type: CDataType::Char,
            target_value: self.values.as_mut_ptr() as Pointer,
            target_length: self.max_str_len.try_into().unwrap(),
            indicator: self.indicators.as_mut_ptr(),
        }
    }
}

impl TextColumn {
    /// This will allocate a value and indicator buffer for `batch_size` elments. Each value may
    /// have a maximum length of `max_str_len`. This implies that max_str_len is increased by one in
    /// order to make space for the null terminating zero at the end of strings.
    pub fn new(batch_size: usize, mut max_str_len: usize) -> Self {
        max_str_len += 1;
        TextColumn {
            max_str_len,
            values: vec![0; (max_str_len + 1) * batch_size],
            indicators: vec![0; batch_size],
        }
    }

    /// Return the value for the given row index.
    ///
    /// # Safety
    ///
    /// The column buffer does not know how many elements were in the last row group, and therefore
    /// can not guarantee the accessed element to be valid and in a defined state. It also can not
    /// panic on accessing an undefined element. It will panic however if `row_index` is larger or
    /// equal to the maximum number of elements in the buffer.
    pub unsafe fn value_at(&self, row_index: usize) -> Option<&[u8]> {
        let str_len = self.indicators[row_index];
        if str_len == NULL_DATA {
            None
        } else {
            let offset = row_index * self.max_str_len;
            let length = min(self.max_str_len, str_len as usize);
            Some(&self.values[offset..offset + length])
        }
    }
}
