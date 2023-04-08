# ODBC-API

[![Docs](https://docs.rs/odbc-api/badge.svg)](https://docs.rs/odbc-api/)
[![Licence](https://img.shields.io/crates/l/odbc-api)](https://github.com/pacman82/odbc-api/blob/main/License)
[![Crates.io](https://img.shields.io/crates/v/odbc-api)](https://crates.io/crates/odbc-api)
[![Coverage Status](https://coveralls.io/repos/github/pacman82/odbc-api/badge.svg?branch=main)](https://coveralls.io/github/pacman82/odbc-api?branch=main)

Rust ODBC bindings. ODBC (Open Database Connectivity) is an open standard to connect to a variaty of data sources. Most data sources offer ODBC drivers. This crate is currently tested against:

* Microsoft SQL Server
* PostgreSQL
* MariaDB
* SQLite

Current ODBC Version is `3.80`.

This crate is build on top of the `odbc-sys` ffi bindings, which provide definitions of the ODBC C Interface, but do not build any kind of abstraction on top of it.

## Usage

Check the [guide](https://docs.rs/odbc-api/latest/odbc_api/guide/index.html) for code examples and a tour of the features.

## Installation

To build this library you need to link against the `odbc` library of your systems ODBC driver manager. It should be automatically detected by the build. On Windows systems it is preinstalled. On Linux and OS-X [unix-odbc](http://www.unixodbc.org/) must be installed. To create a Connections to a data source, its ODBC driver must also be installed.

### Windows

Nothing to do. ODBC driver manager is preinstalled.

### Ubuntu

```shell
sudo apt-get install unixodbc-dev
```

### OS-X (intel)

You can use homebrew to install UnixODBC

```shell
brew install unixodbc
```

### OS-X (ARM / MAC M1)

`cargo build` is not going to pick up `libodbc.so` installed via homebrew due to the fact that homebrew on ARM Mac installs into `/opt/homebrew/Cellar` as opposed to `/usr/local/opt/`.

You find documentation on what directories are searched during build here: <https://doc.rust-lang.org/cargo/reference/environment-variables.html#dynamic-library-paths>.

You can also install unixODBC from source:

1. copy the unixODBC-2.3.9.tar.gz file somewhere you can create files and directories
2. gunzip unixODBC*.tar.gz
3. tar xvf unixODBC*.tar
4. `./configure`
5. `make`
6. `make install`

### Non-system wide installation with Nix Flakes

On Linux and OS-X one can install the Nix package manager:

- The [unofficial installer](https://zero-to-nix.com/start/install)
- Or the [official installer](https://nixos.org/download.html)

The unofficial installer has better capability to roll back the installation and enables nix flakes by default. The official installer requires [extra steps](https://nixos.wiki/wiki/Flakes) to enable flakes.

Here is an example of a `flake.nix` that sets up a dev environment for `odbc-api` for use with the ODBC Driver 17 for SQL Server:

```nix
{
  description = "Setup a devShell for odbc-api";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
          config.allowUnfree = true;
        };
      in
      {
        # For odbc-api to work it needs openssl 1.1 and unixODBC bying in the
        # LD_LIBRARY_PATH and odbcinst.ini contents and location need to be set.
        devShell = pkgs.mkShell {
          shellHook = with pkgs; ''
            export LD_LIBRARY_PATH="${unixODBC}/lib:${openssl_1_1.out}/lib";

            # Make rust build directory if it doesn't already exist
            mkdir ./target

            # see https://www.systutorials.com/docs/linux/man/7-unixODBC/
            # Overloads path to unixODBC configuration files. By default equals to '/etc'.
            export ODBCSYSINI=$(realpath ./target)

            echo "[ODBC Driver 17 for SQL Server]" > ./target/odbcinst.ini
            echo "Description = ODBC Driver 17 for SQL Server" >> ./target/odbcinst.ini
            echo "Driver = ${unixODBCDrivers.msodbcsql17}/lib/libmsodbcsql-17.7.so.1.1" >> ./target/odbcinst.ini
          '';
        };
      });
}
```

Add this as `flake.nix` to the root of your rust project, stage it in `git`, and run `nix develop` to enter the devShell.

For example:

```shell
$ cargo new odbc-tester
$ cd odbc-tester
$ cargo add odbc-api
$ touch flake.nix # And paste the above flake.nix into it
$ git init
$ git add -A
$ nix develop
```

Nix flakes depend on `git` and are not aware of files not staged or committed, this is why `git add -A` is needed to stage the `flake.nix` file. Otherwise, `nix develop` will fail.

The `flake.nix` above will require some modifications to work with other ODBC drivers. For example, replacing `ODBC Driver 17 for SQL Server` with one of `PostgreSQL`, `MariaDB`, or `SQLite`. And `unixODBCDrivers.msodbcsql17` with one of the [other drivers](https://search.nixos.org/options?channel=unstable&show=environment.unixODBCDrivers&from=0&size=50&sort=relevance&type=packages&query=unixODBCDrivers) such as `mariadb`, `psql` or `sqlite`.

The hard coded driver path will need to be changed as well for other drivers. One can figure out this path by a combination of looking at the [nixpkgs source](https://github.com/NixOS/nixpkgs/blob/456d8190ad756a30d69064381b5149bceabc14a6/pkgs/development/libraries/unixODBCDrivers/default.nix#L62) and / or evaluating the package:

```shell
$ nix-build '<nixpkgs>' -A unixODBCDrivers.mariadb
$ cd ./result
$ ls
```

The `openssl` version may need to change as well or may not be needed at all for other drivers. I have not tested the above `flake.nix` with other drivers or on OS-X.

## Features

* [x] Connect using Data Source names (DSN)
* [x] Connect using ODBC connection strings
* [x] Connect using prompts (windows)
* [x] Log ODBC diagnostics and warnings (via `log` crate).
* [x] Columnar bulk inserts.
* [x] Columnar bulk queries.
* [ ] Rowise bulk inserts.
* [ ] Rowise bulk queries.
* [x] Output parameters of stored procedures.
* [x] prepared and 'one shot' queries.
* [x] Transactions
* [x] Pass parameters to queries
* [x] Asynchronous execution of one shot queries
* [ ] Asynchronous execution of prepared queries
* [ ] Asynchronous connecting to database
* [ ] Asynchronous fetching of Metainformation
* [x] Multithreading
* [x] Inserting large binary / text data in stream
* [x] Fetching arbitrary large text / binary data in stream
* [x] Connection pooling
* [x] List tables of data sources
* [x] Retrieve multiple result sets
