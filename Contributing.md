# Contributions

Whether they be in code, interesting feature suggestions, design critique or bug reports, all contributions are welcome. Please start an issue, before investing a lot of work. This helps avoid situations there I would feel the need to reject a large body of work, and a lot of your time has been wasted. `odbc-api` is a pet project and a work of love, which implies that I maintain it in my spare time. Please understand that I may not always react immediately. If you contribute code to fix a Bug, please also contribute the test to fix it. Happy contributing.

## Local build and test setup

Running local tests currently requires:

* Docker and Docker compose.

### Visual Studio Code

Should you use Visual Studio Code with the Remote Development extension, it will pick up the `.devcontainer` configuration and everything should be setup for you.

### Not Visual Studio Code

With docker and the SQL Driver installed run:

```shell
docker-compose up
```

This starts containers called `odbc-api_dev`, `odbc-api_mssql` and `odbc-api_mariadb`. You can use the `dev` container to build your code and execute tests in case you do not want to install the required ODBC drivers and/or Rust toolchain on your local machine.

Otherwise you can install these requirements from here:

* Install Rust compiler and Cargo. Follow the instructions on [this site](https://www.rust-lang.org/en-US/install.html).
* [Microsoft ODBC Driver 18 for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver16).
* Maria DB ODBC Connector
* PostgreSQL ODBC driver

The `mssql` container runs a Microsoft SQL Server used for answering the test queries. We can execute the tests in Rust typical fashion using:

```shell
cargo test
```

to run all tests in the workspace, which should now succeed.
