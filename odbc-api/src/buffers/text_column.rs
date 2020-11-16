use crate::{
    handles::{CData, Input},
    DataType,
};

use super::{BindColArgs, ColumnBuffer};
use log::debug;
use odbc_sys::{CDataType, Len, Pointer, NULL_DATA};
use std::{cmp::min, convert::TryInto, ffi::c_void};

/// A buffer intended to be bound to a column of a cursor. Elements of the buffer will contain a
/// variable amount of characters up to a maximum string length. Since most SQL types have a string
/// representation this buffer can be bound to a column of almost any type, ODBC driver and driver
/// manager should take care of the conversion. Since elements of this type have variable length an
/// indicator buffer needs to be bound, whether the column is nullable or not, and therefore does
/// not matter for this buffer.
pub struct TextColumn {
    /// Maximum text length without terminating zero.
    max_str_len: usize,
    values: Vec<u8>,
    /// Elements in this buffer are either `NULL_DATA` or hold the length of the element in value
    /// with the same index. Please note that this value may be larger than `max_str_len` if the
    /// text has been truncated.
    indicators: Vec<Len>,
}

unsafe impl ColumnBuffer for TextColumn {
    fn bind_arguments(&mut self) -> BindColArgs {
        BindColArgs {
            target_type: CDataType::Char,
            target_value: self.values.as_mut_ptr() as Pointer,
            target_length: (self.max_str_len + 1).try_into().unwrap(),
            indicator: self.indicators.as_mut_ptr(),
        }
    }
}

impl TextColumn {
    /// This will allocate a value and indicator buffer for `batch_size` elements. Each value may
    /// have a maximum length of `max_str_len`. This implies that max_str_len is increased by one in
    /// order to make space for the null terminating zero at the end of strings.
    pub fn new(batch_size: usize, max_str_len: usize) -> Self {
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
            let offset = row_index * (self.max_str_len + 1);
            let length = min(self.max_str_len, str_len as usize);
            Some(&self.values[offset..offset + length])
        }
    }

    /// Changes the max_str_len of the buffer. This operation is useful if you find an unexpected
    /// large input string during insertion.
    ///
    /// This is however costly, as not only does the new buffer have to be allocated, but all values
    /// have to copied from the old to the new buffer.
    ///
    /// This method could also be used to reduce the maximum string length, which would truncate
    /// strings in the process.
    ///
    /// # Parameters
    ///
    /// * `new_max_str_len`: New maximum string length without terminating zero.
    /// * `num_rows`: Number of valid rows currently stored in this buffer.
    pub fn rebind(&mut self, new_max_str_len: usize, num_rows: usize) {
        debug!(
            "Rebinding text column buffer with {} elements. Maximum string length {} => {}",
            num_rows, self.max_str_len, new_max_str_len
        );

        let batch_size = self.indicators.len();
        // Allocate a new buffer large enough to hold a batch of strings with maximum length.
        let mut new_values = vec![0u8; (new_max_str_len + 1) * batch_size];
        // Copy values from old to new buffer.
        let max_copy_length = min(self.max_str_len, new_max_str_len);
        for ((&indicator, old_value), new_value) in self
            .indicators
            .iter()
            .zip(self.values.chunks_exact_mut(self.max_str_len + 1))
            .zip(new_values.chunks_exact_mut(new_max_str_len + 1))
            .take(num_rows)
        {
            if indicator != NULL_DATA {
                let num_bytes_to_copy = min(indicator as usize, max_copy_length);
                new_value[..num_bytes_to_copy].copy_from_slice(&old_value[..num_bytes_to_copy]);
            }
        }
        self.values = new_values;
        self.max_str_len = new_max_str_len;
    }

    /// Appends a new element to the column buffer. Rebinds the buffer to increase maximum string
    /// length should text be to large.
    ///
    /// # Parameters
    ///
    /// * `index`: Zero based index of the new row position. Must be equal to the number of rows
    ///   currently in the buffer.
    /// * `text`: Text to store without terminating zero.
    pub fn append(&mut self, index: usize, text: Option<&[u8]>) {
        if let Some(text) = text {
            if text.len() > self.max_str_len {
                let new_max_str_len = (text.len() as f64 * 1.2) as usize;
                self.rebind(new_max_str_len, index)
            }

            let offset = index * (self.max_str_len + 1);
            self.values[offset..offset + text.len()].copy_from_slice(text);
            // Add terminating zero to string.
            self.values[offset + text.len()] = 0;
            // And of courseset the indicator correctly.
            self.indicators[index] = text.len().try_into().unwrap();
        } else {
            self.indicators[index] = NULL_DATA;
        }
    }
}

unsafe impl CData for TextColumn {
    fn cdata_type(&self) -> CDataType {
        CDataType::Char
    }

    fn indicator_ptr(&self) -> *const i64 {
        self.indicators.as_ptr()
    }

    fn value_ptr(&self) -> *const c_void {
        self.values.as_ptr() as *const c_void
    }

    fn buffer_length(&self) -> Len {
        (self.max_str_len + 1).try_into().unwrap()
    }
}

unsafe impl Input for TextColumn {
    fn data_type(&self) -> DataType {
        DataType::Varchar {
            length: self.max_str_len.try_into().unwrap(),
        }
    }
}
