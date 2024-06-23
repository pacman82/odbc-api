pub const MSSQL_CONNECTION: &str =
    "Driver={ODBC Driver 18 for SQL Server};Server=localhost;UID=SA;PWD=My@Test@Password1;\
    TrustServerCertificate=yes;";

#[cfg(target_os = "windows")]
pub const SQLITE_3_CONNECTION: &str =
    "Driver={SQLite3 ODBC Driver};Database=sqlite-test.db;{Journal Mode}=WAL;";
#[cfg(not(target_os = "windows"))]
pub const SQLITE_3_CONNECTION: &str =
    "Driver={SQLite3};Database=sqlite-test.db;{Journal Mode}=WAL;";

#[cfg(target_os = "windows")]
pub const MARIADB_CONNECTION: &str = "Driver={MariaDB ODBC 3.1 Driver};\
    Server=localhost;DB=test_db;\
    UID=root;PWD=my-secret-pw;\
    Port=3306";

// Use 127.0.0.1 instead of localhost so the system uses the TCP/IP connector instead of the socket
// connector. Prevents error message: 'Can't connect to local MySQL server through socket'.
#[cfg(not(target_os = "windows"))]
pub const MARIADB_CONNECTION: &str = "Driver={MariaDB 3.1 Driver};\
    Server=127.0.0.1;DB=test_db;\
    UID=root;PWD=my-secret-pw;\
    Port=3306";

pub const POSTGRES_CONNECTION: &str = "Driver={PostgreSQL UNICODE};\
    Server=localhost;\
    Port=5432;\
    Database=test;\
    Uid=test;\
    Pwd=test;";
