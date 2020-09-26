use odbc_sys::{
    Desc, HDesc, HStmt, Handle, HandleType, Integer, Pointer, SQLSetDescFieldW, SmallInt,
    SqlDataType,
};
use std::marker::PhantomData;

use crate::Error;

use super::{error::ToResult, AsHandle};

/// Wraps a valid description handle.
pub struct Description<'d> {
    parent: PhantomData<&'d HStmt>,
    handle: HDesc,
}

unsafe impl<'c> AsHandle for Description<'c> {
    fn as_handle(&self) -> Handle {
        self.handle as Handle
    }

    fn handle_type(&self) -> HandleType {
        HandleType::Desc
    }
}

impl<'d> Description<'d> {
    /// # Safety
    ///
    /// `handle` must be a valid description handle.
    pub unsafe fn new(handle: HDesc) -> Self {
        Self {
            handle,
            parent: PhantomData,
        }
    }

    pub fn set_field_precision(
        &mut self,
        column_number: SmallInt,
        precision: Integer,
    ) -> Result<(), Error> {
        unsafe {
            SQLSetDescFieldW(
                self.handle,
                column_number,
                Desc::Precision,
                precision as Pointer,
                0,
            )
            .to_result(self)
        }
    }

    pub fn set_field_scale(
        &mut self,
        column_number: SmallInt,
        scale: Integer,
    ) -> Result<(), Error> {
        unsafe {
            SQLSetDescFieldW(self.handle, column_number, Desc::Scale, scale as Pointer, 0)
                .to_result(self)
        }
    }

    pub unsafe fn set_field_type(
        &mut self,
        column_number: SmallInt,
        data_type: SqlDataType,
    ) -> Result<(), Error> {
        SQLSetDescFieldW(
            self.handle,
            column_number,
            Desc::Type,
            data_type.0 as Pointer,
            0,
        )
        .to_result(self)
    }

    pub unsafe fn set_field_concise_type(
        &mut self,
        column_number: SmallInt,
        data_type: SqlDataType,
    ) -> Result<(), Error> {
        SQLSetDescFieldW(
            self.handle,
            column_number,
            Desc::ConciseType,
            data_type.0 as Pointer,
            0,
        )
        .to_result(self)
    }
}
