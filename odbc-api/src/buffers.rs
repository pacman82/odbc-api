/*!
# Using buffers to fetch results

The most efficient way to query results is not query an ODBC data source row by row, but to
ask for a whole bulk of rows at once. The ODBC driver and driver manager will then fill these
row sets into buffers which have been previously bound. This is also the most efficient way to
query a single row many times for many queries, if the application can reuse the bound buffer.
This crate allows you to provide your own buffers by implementing the [`crate::RowSetBuffer`]
trait. That however requires `unsafe` code.

This crate also provides three implementations of the [`crate::RowSetBuffer`] trait, ready to be
used in safe code:

* [`crate::buffers::ColumnarBuffer`]: Binds to the result set column wise. This is usually helpful
  in dataengineering or data sciense tasks. This buffer type can be used in situations there the
  schema of the queried data is known at compile time, as well as for generic applications which do
  work with wide range of different data. Checkt the struct documentation for examples.
* [`crate::buffers::TextRowSet`]: Queries all data as text bound in columns. Since the columns are
  homogeneous, you can also use this, to iterate row wise over the buffer. Excellent if you want
  to print the contents of a table, or are for any reason only interessted in the text
  representation of the values.
* [`crate::buffers::RowVec`]: A good choice if you know the schema at compile time and your
  application logic is build in a row by row fashion, rather than column by column.

*/

mod any_buffer;
mod bin_column;
mod column_with_indicator;
mod columnar;
mod description;
mod indicator;
mod item;
mod row_vec;
mod text_column;

pub use self::{
    any_buffer::{AnyBuffer, AnySlice, AnySliceMut, ColumnarAnyBuffer},
    bin_column::{BinColumn, BinColumnIt, BinColumnSliceMut, BinColumnView},
    column_with_indicator::{NullableSlice, NullableSliceMut},
    columnar::{ColumnBuffer, ColumnarBuffer, TextRowSet},
    description::BufferDesc,
    indicator::Indicator,
    item::Item,
    row_vec::{FetchRow, FetchRowMember, RowVec},
    text_column::{
        CharColumn, TextColumn, TextColumnIt, TextColumnSliceMut, TextColumnView, WCharColumn,
    },
};
