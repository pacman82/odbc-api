# See here for image contents: https://github.com/microsoft/vscode-dev-containers/tree/v0.163.1/containers/debian/.devcontainer/base.Dockerfile

# [Choice] Debian version: buster, stretch
ARG VARIANT="buster"
FROM mcr.microsoft.com/vscode/devcontainers/base:0-${VARIANT}

# Install Microsoft ODBC SQL Drivers (msodbcsql17 package) for Debian 10
# https://docs.microsoft.com/de-de/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-ver15
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN echo msodbcsql17 msodbcsql/ACCEPT_EULA boolean true | debconf-set-selections

# Add buster backports to source list (required for Maria DB ODBC driver)
# 
# Turns out this package ships with to old a driver. Leave this in for reference still.
# 
# RUN echo "deb http://deb.debian.org/debian buster-backports main" >> /etc/apt/sources.list

# This section to installs additional packages.
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
	&& apt-get -y install --no-install-recommends \
	build-essential \
	unixodbc-dev \
	msodbcsql17 \
	# odbc-mariadb/buster-backports \
	sqlite3 \
	libsqliteodbc

# Fix SQLite driver paths
RUN sed --in-place 's/libsqlite3odbc.so/\/usr\/lib\/x86_64-linux-gnu\/odbc\/libsqlite3odbc.so/' /etc/odbcinst.ini
RUN sed --in-place 's/libsqliteodbc.so/\/usr\/lib\/x86_64-linux-gnu\/odbc\/libsqliteodbc.so/' /etc/odbcinst.ini

# Install MariaDB driver from tar bundle
COPY docker/mariadb-connector-odbc-3.1.15-debian-buster-amd64.tar.gz .
COPY docker/mariadb_odbc_template.ini .
RUN tar -xf mariadb-connector-odbc-3.1.15-debian-buster-amd64.tar.gz
RUN cp mariadb-connector-odbc-3.1.15-debian-buster-amd64/lib/mariadb/libmaodbc.so /usr/lib/x86_64-linux-gnu/odbc/libmaodbc.so
RUN cp mariadb-connector-odbc-3.1.15-debian-buster-amd64/lib/mariadb/libmariadb.so.3 /usr/lib/x86_64-linux-gnu/
RUN odbcinst -i -d -f mariadb_odbc_template.ini

# There is also a rust devcontainer, yet this way we get a toolchain
# which is updatable with rustup.
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --default-toolchain stable -y

# Setup workspace
WORKDIR /workspace
COPY . .
RUN cp odbcsv/tests/list-drivers-linux.txt odbcsv/tests/list-drivers.txt

CMD ~/.cargo/bin/cargo test --release & ~/.cargo/bin/cargo test --release --features narrow & ~/.cargo/bin/cargo test --release --features odbc_version_3_5
