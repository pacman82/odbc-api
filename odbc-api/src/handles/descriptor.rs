use std::marker::PhantomData;

use odbc_sys::{
    CDataType, Desc, HDesc, HStmt, Handle, HandleType, Pointer, IS_POINTER, IS_SMALLINT,
};

use super::{sql_result::ExtSqlReturn, AsHandle, SqlResult};

#[cfg(not(any(feature = "wide", all(not(feature = "narrow"), target_os = "windows"))))]
use odbc_sys::SQLSetDescField as sql_set_desc_field;

#[cfg(any(feature = "wide", all(not(feature = "narrow"), target_os = "windows")))]
use odbc_sys::SQLSetDescFieldW as sql_set_desc_field;

/// A descriptor associated with a statement. This wrapper does not wrap explicitly allocated
/// descriptors which have the connection as parent, but usually implicitly allocated ones
/// associated with the statement. It could also represent an explicitly allocated one, but ony in
/// the context there it is associated with a statement and currently borrowed from it.
///
/// * IPD Implementation parameter descriptor
/// * APD Application parameter descriptor
/// * IRD Implemenation row descriptor
/// * ARD Application row descriptor
pub struct Descriptor<'stmt> {
    handle: HDesc,
    parent: PhantomData<&'stmt HStmt>,
}

impl Descriptor<'_> {
    /// # Safety
    ///
    /// Call this method only with a valid (successfully allocated) ODBC descriptor handle.
    pub unsafe fn new(handle: HDesc) -> Self {
        Self {
            handle,
            parent: PhantomData,
        }
    }

    /// Directly acces the underlying ODBC handle.
    pub fn as_sys(&self) -> HDesc {
        self.handle
    }

    /// Number of digits for an exact numeric type, the number of bits in the mantissa (binary
    /// precision) for an approximate numeric type, or the numbers of digits in the fractional
    /// seconds component for the SQL_TYPE_TIME, SQL_TYPE_TIMESTAMP, or SQL_INTERVAL_SECOND data
    /// type. This field is undefined for all other data types.
    /// See: <https://learn.microsoft.com/sql/odbc/reference/syntax/sqlsetdescfield-function>
    pub fn set_precision(&mut self, rec_number: i16, precision: i16) -> SqlResult<()> {
        unsafe {
            sql_set_desc_field(
                self.as_sys(),
                rec_number,
                Desc::Precision,
                precision as Pointer,
                IS_SMALLINT,
            )
            .into_sql_result("SQLSetDescField")
        }
    }

    /// The defined scale for decimal and numeric data types. The field is undefined for all other
    /// data types.
    pub fn set_scale(&mut self, rec_number: i16, scale: i16) -> SqlResult<()> {
        unsafe {
            sql_set_desc_field(
                self.as_sys(),
                rec_number,
                Desc::Scale,
                scale as Pointer,
                IS_SMALLINT,
            )
            .into_sql_result("SQLSetDescField")
        }
    }

    /// C-Type bound to the data pointer.
    ///
    /// # Safety
    ///
    /// The buffer bound to the data pointer in ARD must match, otherwise calls to fetch might e.g.
    /// write beyond the bounds of these types, if e.g. a larger type is bound
    pub unsafe fn set_type(&mut self, rec_number: i16, c_type: CDataType) -> SqlResult<()> {
        sql_set_desc_field(
            self.as_sys(),
            rec_number,
            Desc::Type,
            c_type as i16 as Pointer,
            IS_SMALLINT,
        )
        .into_sql_result("SQLSetDescField")
    }

    /// Data pointer filled with values from the source when fetching data.
    ///
    /// # Safety
    ///
    /// Pointer must be valid and match the description set using set_type.
    pub unsafe fn set_data_ptr(&mut self, rec_number: i16, data_ptr: Pointer) -> SqlResult<()> {
        sql_set_desc_field(
            self.as_sys(),
            rec_number,
            Desc::DataPtr,
            data_ptr,
            IS_POINTER,
        )
        .into_sql_result("SQLSetDescField")
    }
}

unsafe impl AsHandle for Descriptor<'_> {
    fn as_handle(&self) -> Handle {
        self.handle as Handle
    }

    fn handle_type(&self) -> HandleType {
        HandleType::Desc
    }
}
