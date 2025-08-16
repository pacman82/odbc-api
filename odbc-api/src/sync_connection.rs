use std::sync::{Arc, Mutex};

use crate::{Connection, handles::ConnectionOwner};

/// A convinient type alias in case you want to use a connection from multiple threads which share
/// ownership of it.
pub type SharedConnection<'env> = Arc<Mutex<Connection<'env>>>;

/// # Safety:
///
/// Connection is guaranteed to be alive and in connected state for the lifetime of
/// [`SharedConnection`].
unsafe impl ConnectionOwner for SharedConnection<'_> {}
