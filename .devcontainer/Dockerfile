# Docker image intended for local development using vscode/zed
FROM mcr.microsoft.com/devcontainers/rust:2-1-trixie

# Download the package to configure the Microsoft repo
RUN curl -sSL -O https://packages.microsoft.com/config/debian/13/packages-microsoft-prod.deb
# Install the package
RUN dpkg -i packages-microsoft-prod.deb
RUN rm packages-microsoft-prod.deb

# Install packages
RUN echo msodbcsql18 msodbcsql/ACCEPT_EULA boolean true | debconf-set-selections
RUN apt update && export DEBIAN_FRONTEND=noninteractive \
&& apt-get -y install --no-install-recommends \
    build-essential \
    unixodbc-dev \
    msodbcsql18 \
    odbc-mariadb \
    libsqliteodbc \
    odbc-postgresql \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

# # Fix SQLite driver paths
# RUN sed --in-place 's/libsqlite3odbc.so/\/usr\/lib\/x86_64-linux-gnu\/odbc\/libsqlite3odbc.so/' /etc/odbcinst.ini
# RUN sed --in-place 's/libsqliteodbc.so/\/usr\/lib\/x86_64-linux-gnu\/odbc\/libsqliteodbc.so/' /etc/odbcinst.ini

# # Fix PostgreSQL driver paths
# RUN sed --in-place 's/psqlodbca.so/\/usr\/lib\/x86_64-linux-gnu\/odbc\/psqlodbca.so/' /etc/odbcinst.ini
# RUN sed --in-place 's/psqlodbcw.so/\/usr\/lib\/x86_64-linux-gnu\/odbc\/psqlodbcw.so/' /etc/odbcinst.ini

# Install DuckDB ODBC driver
RUN curl -L -o duckdb_odbc.zip https://github.com/duckdb/duckdb-odbc/releases/download/v1.4.4.0/duckdb_odbc-linux-amd64.zip \
	&& unzip duckdb_odbc.zip -d /opt/duckdb_odbc \
	&& rm duckdb_odbc.zip \
	&& printf "[DuckDB Driver]\nDriver = /opt/duckdb_odbc/libduckdb_odbc.so\n" | odbcinst -i -d -r

# Setup cargo and install cargo packages required for tests
USER vscode
