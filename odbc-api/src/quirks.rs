/// A (non exhaustive) description of the non ODBC API conformant behavior of ODBC drivers.
/// Workarounds which are intended to help application developers seperate between the descision of
/// how to deal with non conformity from the knowledge which driver behaves weird in exactly which
/// way.
///
/// For example it wants to avoid an if statement specifying "if the database is DB2 please use
/// terminating zeroes instead of indiactors to determine string lengths" and seperate this into
/// "IBM DB2 returns memory garbage for indicators" and another part of the application decides,
/// this is how I deal with memory garbage indicicators.
#[non_exhaustive]
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Quirks {
    /// IBM DB2 has been observered that the length indicators returned from memory are garbage for
    /// strings. It seems to be preferable to rely on the terminating zero exclusively to determine
    /// string length. This behavior seems to so far only manifest with variadic string fields.
    /// See: <https://github.com/pacman82/arrow-odbc-py/issues/68> and also
    /// <https://github.com/pacman82/odbc-api/issues/398>
    pub indicators_returned_from_bulk_fetch_are_memory_garbage: bool,
}

impl Quirks {
    /// A new instance describing an ODBC driver without quirks
    pub fn new() -> Self {
        Quirks {
            indicators_returned_from_bulk_fetch_are_memory_garbage: false,
        }
    }

    /// Fill quircks based on database name
    /// 
    /// ```no_run
    /// use odbc_api::{Environment, ConnectionOptions, Quirks};
    ///
    /// let env = Environment::new()?;
    ///
    /// let mut conn = env.connect(
    ///     "YourDatabase", "SA", "My@Test@Password1",
    ///     ConnectionOptions::default()
    /// )?;
    /// let dbms_name = conn.database_management_system_name()?;
    /// let quirks = Quirks::from_dbms_name(&dbms_name);
    /// 
    /// /// Application logic can now implement workarounds based on known quirks.
    /// /// ...
    /// # Ok::<(), odbc_api::Error>(())
    /// ```
    pub fn from_dbms_name(name: &str) -> Self {
        let mut quirks = Quirks::new();

        if name.starts_with("DB2/LINUX") {
            quirks.indicators_returned_from_bulk_fetch_are_memory_garbage = true
        }

        quirks
    }
}

impl Default for Quirks {
    fn default() -> Self {
        Self::new()
    }
}
