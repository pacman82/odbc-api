use anyhow::{anyhow, bail, Error};
use clap::{ArgAction, Args, Parser};
use log::info;
use odbc_api::{
    buffers::{Indicator, TextRowSet},
    escape_attribute_value,
    handles::OutputStringBuffer,
    Connection, ConnectionOptions, Cursor, DriverCompleteOption, Environment, IntoParameter,
};
use std::{
    fs::{read_to_string, File},
    io::{stdin, stdout, Read, Write},
    path::PathBuf,
};

/// Query an ODBC data source and output the result as CSV.
#[derive(Parser)]
#[clap(version)]
struct Cli {
    /// Verbose mode (-v, -vv, -vvv, etc)
    #[arg(short = 'v', long, action = ArgAction::Count)]
    verbose: u8,
    #[command(subcommand)]
    command: Command,
}

#[derive(Parser)]
enum Command {
    /// Query a data source and write the result as csv. This is the deprecated version of `fetch`.
    Query {
        #[clap(flatten)]
        query_opt: QueryOpt,
    },
    /// Query a data source and write the result as csv.
    Fetch {
        #[clap(flatten)]
        fetch_opt: FetchOpt,
    },
    /// Read the content of a csv and insert it into a table.
    Insert {
        #[clap(flatten)]
        insert_opt: InsertOpt,
    },
    /// List tables, schemas, views and catalogs provided by the datasource.
    ListTables {
        #[clap(flatten)]
        table_opt: ListTablesOpt,
    },
    /// List columns
    ListColumns {
        #[clap(flatten)]
        columns_opt: ListColumnsOpt,
    },
    /// List available drivers. Useful to find out which exact driver name to specify in the
    /// connections string.
    ListDrivers,
    /// List preconfigured data sources. Useful to find data source name to connect to database.
    ListDataSources,
}

// Attention: This has overwritten some help messages for the enduser if turned into a docstring:
// Command line arguments used to establish a connection with the ODBC data source
#[derive(Args)]
struct ConnectOpts {
    #[arg(long, conflicts_with = "dsn")]
    /// Prompts the user for missing information from the connection string. Only supported on
    /// windows platform.
    prompt: bool,
    /// The connection string used to connect to the ODBC data source. Alternatively you may specify
    /// the ODBC dsn.
    #[arg(long, short = 'c')]
    connection_string: Option<String>,
    /// ODBC Data Source Name. Either this or the connection string must be specified to identify
    /// the datasource. Data source name (dsn) and connection string, may not be specified both.
    #[arg(long, conflicts_with = "connection_string")]
    dsn: Option<String>,
    /// User used to access the datasource specified in dsn. Should you specify a connection string
    /// instead of a Data Source Name the user name is going to be appended at the end of it as the
    /// `UID` attribute.
    #[arg(long, short = 'u', env = "ODBC_USER")]
    user: Option<String>,
    /// Password used to log into the datasource. Only used if dsn is specified, instead of a
    /// connection string. Should you specify a Connection string instead of a Data Source Name the
    /// password is going to be appended at the end of it as the `PWD` attribute.
    #[arg(long, short = 'p', env = "ODBC_PASSWORD", hide_env_values = true)]
    password: Option<String>,
}

#[derive(Args)]
struct QueryOpt {
    #[clap(flatten)]
    connect_opts: ConnectOpts,
    /// Number of rows queried from the database on block. Larger numbers may reduce io overhead,
    /// but require more memory during execution.
    #[arg(long, default_value = "5000")]
    batch_size: usize,
    /// Maximum string length in bytes. If omitted no limit is applied and the ODBC driver is taken
    /// for its word regarding the maximum length of the columns.
    #[arg(long, short = 'm')]
    max_str_len: Option<usize>,
    /// Setting a maximum string length does help with avoiding allocating really large buffers, and
    /// save memory, but it may lead to values being truncated, if any fields are larger than the
    /// number of bytes specified with `max-str-len`. By default a truncation will raise an error
    /// and cause the tool to exit with a non-zero status code. Set this flag to continue execution
    /// and just write the truncated values into the csv.
    #[arg(long)]
    ignore_truncation: bool,
    /// Path to the output csv file the returned values are going to be written to. If omitted the
    /// csv is going to be printed to standard out.
    #[arg(long, short = 'o')]
    output: Option<PathBuf>,
    /// Query executed against the ODBC data source. Question marks (`?`) can be used as
    /// placeholders for positional parameters.
    query: String,
    /// For each placeholder question mark (`?`) in the query text one parameter must be passed at
    /// the end of the command line.
    parameters: Vec<String>,
}

#[derive(Args)]
struct FetchOpt {
    #[clap(flatten)]
    connect_opts: ConnectOpts,
    /// Number of rows queried from the database on block. Larger numbers may reduce io overhead,
    /// but require more memory during execution.
    #[arg(long, default_value = "5000")]
    batch_size: usize,
    /// Maximum string length in bytes. If omitted no limit is applied and the ODBC driver is taken
    /// for its word regarding the maximum length of the columns.
    #[arg(long, short = 'm')]
    max_str_len: Option<usize>,
    /// Path to the output csv file the returned values are going to be written to. If omitted the
    /// csv is going to be printed to standard out.
    #[arg(long, short = 'o')]
    output: Option<PathBuf>,
    /// Query executed against the ODBC data source. Within the SQL text Question marks (`?`) can be
    /// used as placeholders for positional parameters.
    #[arg(long, short = 'q', conflicts_with = "sql_file")]
    query: Option<String>,
    /// Read the SQL query from a file, rather than a literal passed at the command line. Argument
    /// specifies path to that file. Within the SQL text question marks (`?`) can be used as
    /// placeholders for positional parameters.
    #[arg(long, short = 'f', conflicts_with = "query")]
    sql_file: Option<PathBuf>,
    /// For each placeholder question mark (`?`) in the query text one parameter must be passed at
    /// the end of the command line.
    parameters: Vec<String>,
}
#[derive(Args)]
struct InsertOpt {
    #[clap(flatten)]
    connect_opts: ConnectOpts,
    /// Number of rows inserted into the database on block. Larger numbers may reduce io overhead,
    /// but require more memory during execution.
    #[arg(long, default_value = "5000")]
    batch_size: usize,
    /// Path to the input csv file which is used to fill the database table with values. If
    /// omitted standard input is used.
    #[arg(long, short = 'i')]
    input: Option<PathBuf>,
    /// Name of the table to insert the values into. No precautions against SQL injection are
    /// taken.
    table: String,
}

#[derive(Args)]
struct ListTablesOpt {
    #[clap(flatten)]
    connect_opts: ConnectOpts,
    /// Filter result by catalog name. Accept search patterns. Use `%` to match any number of
    /// characters. Use `_` to match exactly on character. Use `\` to escape characeters.
    #[arg(long)]
    catalog: Option<String>,
    /// Filter result by schema. Accepts patterns in the same way as `catalog`.
    #[arg(long)]
    schema: Option<String>,
    /// Filter result by table name. Accepts patterns in the same way as `catalog`.
    #[arg(long)]
    name: Option<String>,
    /// Filters results by table type. E.g: 'TABLE', 'VIEW'. This argument accepts a comma separeted
    /// list of table types. Ommit it to not filter the result by table type at all.
    #[arg(long = "type")]
    type_: Option<String>,
}

#[derive(Args)]
struct ListColumnsOpt {
    #[clap(flatten)]
    connect_opts: ConnectOpts,
    /// Filter result by catalog name. Accept search patterns. Use `%` to match any number of
    /// characters. Use `_` to match exactly on character. Use `\` to escape characeters.
    #[arg(long)]
    catalog: Option<String>,
    /// Filter result by schema. Accepts patterns in the same way as `catalog`.
    #[arg(long)]
    schema: Option<String>,
    /// Filter result by table name. Accepts patterns in the same way as `catalog`.
    #[arg(long)]
    table: Option<String>,
    /// Filter result by column name. Accepts patterns in the same way as `catalog`.
    #[arg(long)]
    column: Option<String>,
}

fn main() -> Result<(), Error> {
    // Parse arguments from command line interface. Exit on Error
    let opt = Cli::parse();

    // Initialize logging.
    stderrlog::new()
        .module(module_path!())
        .module("odbc_api")
        .quiet(false)
        .verbosity(opt.verbose as usize)
        .timestamp(stderrlog::Timestamp::Second)
        .init()?;

    // It is recommended to have only one Environment per Application.
    let environment = Environment::new()?;

    match opt.command {
        Command::Query { query_opt } => {
            query(&environment, &query_opt)?;
        }
        Command::Fetch { fetch_opt } => {
            fetch(&environment, fetch_opt)?;
        }
        Command::Insert { insert_opt } => {
            if insert_opt.batch_size == 0 {
                bail!("batch size, must be at least 1");
            }
            insert(&environment, &insert_opt)?;
        }
        Command::ListTables { table_opt } => {
            tables(&environment, &table_opt)?;
        }
        Command::ListColumns { columns_opt } => {
            columns(&environment, &columns_opt)?;
        }
        Command::ListDrivers => {
            let mut first = true;
            for driver_info in environment.drivers()? {
                // After first item, always place an additional newline in between.
                if first {
                    first = false;
                } else {
                    println!()
                }
                println!("{}", driver_info.description);
                for (key, value) in &driver_info.attributes {
                    println!("\t{key}={value}");
                }
            }
        }
        Command::ListDataSources => {
            let mut first = true;
            for data_source_info in environment.data_sources()? {
                // After first item, always place an additional newline in between.
                if first {
                    first = false;
                } else {
                    println!()
                }
                println!("Server name: {}", data_source_info.server_name);
                println!("Driver: {}", data_source_info.driver);
            }
        }
    }

    Ok(())
}

/// Open a database connection using the options provided on the command line.
fn open_connection<'e>(
    environment: &'e Environment,
    opt: &ConnectOpts,
) -> Result<Connection<'e>, Error> {
    if let Some(dsn) = opt.dsn.as_deref() {
        return environment
            .connect(
                dsn,
                opt.user.as_deref().unwrap_or(""),
                opt.password.as_deref().unwrap_or(""),
                ConnectionOptions::default(),
            )
            .map_err(|e| e.into());
    }

    // Append user and or password to connection string
    let mut cs = opt.connection_string.clone().unwrap_or_default();
    if let Some(uid) = opt.user.as_deref() {
        cs = format!("{}UID={};", cs, &escape_attribute_value(uid));
    }
    if let Some(pwd) = opt.password.as_deref() {
        cs = format!("{}PWD={};", cs, &escape_attribute_value(pwd));
    }

    #[cfg(target_os = "windows")]
    let driver_completion = if opt.prompt {
        DriverCompleteOption::Complete
    } else {
        DriverCompleteOption::NoPrompt
    };

    #[cfg(not(target_os = "windows"))]
    let driver_completion = if opt.prompt {
        // Would rather use conditional compilation on the flag itself. While this works fine, it
        // does mess with rust analyzer, so I keep it and panic here to keep development experience
        // smooth.
        bail!("--prompt is only supported on windows.");
    } else {
        DriverCompleteOption::NoPrompt
    };

    if !opt.prompt && opt.connection_string.is_none() && opt.dsn.is_none() {
        bail!("Either DSN, connection string or prompt must be specified.")
    }

    // We are not interessted in the actual string used, lets create an empty buffer.
    let mut completed_connection_string = OutputStringBuffer::empty();

    environment
        .driver_connect(&cs, &mut completed_connection_string, driver_completion)
        .map_err(|e| e.into())
}

/// Execute a query and writes the result to csv.
fn fetch(environment: &Environment, opt: FetchOpt) -> Result<(), Error> {
    let FetchOpt {
        connect_opts,
        output,
        parameters,
        query: query_literal,
        batch_size,
        max_str_len,
        sql_file,
    } = opt;

    let query_str = match (query_literal, sql_file) {
        (Some(literal), _) => literal,
        (None, Some(path)) => read_to_string(path)?,
        _ => bail!("Either `--query` or `--sql-file` must be specified."),
    };

    let query_opt = QueryOpt {
        connect_opts,
        batch_size,
        max_str_len,
        ignore_truncation: false,
        output,
        query: query_str,
        parameters,
    };

    query(environment, &query_opt)
}

/// Execute a query and writes the result to csv.
fn query(environment: &Environment, opt: &QueryOpt) -> Result<(), Error> {
    let QueryOpt {
        connect_opts,
        output,
        parameters,
        query,
        batch_size,
        max_str_len,
        ignore_truncation,
    } = opt;

    // If an output file has been specified write to it, otherwise use stdout instead.
    let hold_stdout; // Prolongs scope of `stdout()` so we can lock() it.
    let out: Box<dyn Write> = if let Some(path) = output {
        Box::new(File::create(path)?)
    } else {
        hold_stdout = stdout();
        Box::new(hold_stdout.lock())
    };
    let mut writer = csv::Writer::from_writer(out);

    let connection = open_connection(environment, connect_opts)?;

    // Convert the input strings into parameters suitable to for use with ODBC.
    let params: Vec<_> = parameters
        .iter()
        .map(|param| param.as_str().into_parameter())
        .collect();

    // Execute the query as a one off, and pass the parameters.
    match connection.execute(query, params.as_slice())? {
        Some(cursor) => {
            // Write column names.
            cursor_to_csv(
                cursor,
                &mut writer,
                *batch_size,
                *max_str_len,
                *ignore_truncation,
            )?;
        }
        None => {
            eprintln!("Query came back empty (not even a schema has been returned). No output has been created.");
        }
    };
    Ok(())
}

/// Read the content of a csv and insert it into a table.
fn insert(environment: &Environment, insert_opt: &InsertOpt) -> Result<(), Error> {
    let InsertOpt {
        input,
        connect_opts,
        table,
        batch_size,
    } = insert_opt;

    // If an input file has been specified, read from it. Use stdin otherwise.
    let hold_stdin; // Prolongs scope of `stdin()` so we can lock() it.
    let input: Box<dyn Read> = if let Some(path) = input {
        Box::new(File::open(path)?)
    } else {
        hold_stdin = stdin();
        Box::new(hold_stdin.lock())
    };
    let mut reader = csv::Reader::from_reader(input);
    let connection = open_connection(environment, connect_opts)?;

    // Generate statement text from table name and headline
    let headline = reader.byte_headers()?;
    let column_names: Vec<&str> = headline
        .iter()
        .map(std::str::from_utf8)
        .collect::<Result<_, _>>()?;
    let columns = column_names.join(", ");
    let values = column_names
        .iter()
        .map(|_| "?")
        .collect::<Vec<_>>()
        .join(", ");
    let statement_text = format!("INSERT INTO {table} ({columns}) VALUES ({values});");
    info!("Insert statement Text: {}", statement_text);

    let mut statement = connection.prepare(&statement_text)?;

    // Log column types.
    // Could get required buffer sizes from parameter description.
    let _parameter_descriptions: Vec<_> = (1..=headline.len())
        .map(|parameter_number| {
            statement
                .describe_param(parameter_number as u16)
                .map(|desc| {
                    info!("Column {} identified as: {:?}", parameter_number, desc);
                    desc
                })
        })
        .collect::<Result<_, _>>()?;

    // Allocate buffer

    // Let's start with a buffer size of 50 per column. This is not much memory on a modern pc, yet
    // may prevent reallocating and rebinding buffers a lot than compared to starting with `0`.
    let mut statement =
        statement.into_text_inserter(*batch_size, (0..headline.len()).map(|_| 50))?;

    // Used to log batch number
    let mut num_batch = 0;

    for try_record in reader.into_byte_records() {
        if statement.num_rows() == *batch_size {
            num_batch += 1;
            // Batch is full. We need to send it to the data base and clear it, before we insert
            // more rows into it.
            statement.execute()?;
            info!(
                "Insert batch {} with {} rows into DB.",
                num_batch, batch_size
            );
            statement.clear();
        }

        let record = try_record?;
        statement.append(
            record
                .iter()
                .map(|field| if field.is_empty() { None } else { Some(field) }),
        )?;
    }

    // Insert the remainder of the buffer to the database. If buffer is empty nothing will be
    // executed.
    statement.execute()?;
    info!("Insert last batch with {} rows into DB.", batch_size);

    Ok(())
}

fn tables(environment: &Environment, table_opt: &ListTablesOpt) -> Result<(), Error> {
    let ListTablesOpt {
        connect_opts,
        catalog,
        schema,
        name,
        type_,
    } = table_opt;
    let conn = open_connection(environment, connect_opts)?;

    let cursor = conn.tables(
        catalog.as_deref().unwrap_or_default(),
        schema.as_deref().unwrap_or_default(),
        name.as_deref().unwrap_or_default(),
        type_.as_deref().unwrap_or_default(),
    )?;

    let hold_stdout = stdout();
    let out = hold_stdout.lock();
    let mut writer = csv::Writer::from_writer(out);

    // Limit the amount of memory allocated for each column element in case some driver decides to
    // report crazy max column lengths (e.g. MariaDB on Windows)
    let max_str_len = Some(4096);
    cursor_to_csv(cursor, &mut writer, 100, max_str_len, true)?;
    Ok(())
}

fn columns(environment: &Environment, columns_opt: &ListColumnsOpt) -> Result<(), Error> {
    let ListColumnsOpt {
        connect_opts,
        catalog,
        schema,
        table,
        column,
    } = columns_opt;

    let conn = open_connection(environment, connect_opts)?;
    let cursor = conn.columns(
        catalog.as_deref().unwrap_or_default(),
        schema.as_deref().unwrap_or_default(),
        table.as_deref().unwrap_or_default(),
        column.as_deref().unwrap_or_default(),
    )?;

    let hold_stdout = stdout();
    let out = hold_stdout.lock();
    let mut writer = csv::Writer::from_writer(out);

    // Limit the amount of memory allocated for each column element in case some driver decides to
    // report crazy max column lengths (e.g. MariaDB on Windows)
    let max_str_len = Some(4096);
    cursor_to_csv(cursor, &mut writer, 100, max_str_len, true)?;
    Ok(())
}

fn cursor_to_csv(
    mut cursor: impl Cursor,
    writer: &mut csv::Writer<impl Write>,
    batch_size: usize,
    max_str_len: Option<usize>,
    ignore_truncation: bool,
) -> Result<(), Error> {
    let headline: Vec<String> = cursor.column_names()?.collect::<Result<_, _>>()?;
    writer.write_record(headline)?;
    let mut buffers = TextRowSet::for_cursor(batch_size, &mut cursor, max_str_len)?;
    let mut row_set_cursor = cursor.bind_buffer(&mut buffers)?;
    let mut num_batch = 0;
    while let Some(buffer) = row_set_cursor
        .fetch_with_truncation_check(!ignore_truncation)
        .map_err(provide_context_for_truncation_error)?
    {
        num_batch += 1;
        info!(
            "Fetched batch {} with {} rows.",
            num_batch,
            buffer.num_rows()
        );
        for row_index in 0..buffer.num_rows() {
            let record = (0..buffer.num_cols())
                .map(|col_index| buffer.at(col_index, row_index).unwrap_or(&[]));
            writer.write_record(record)?;
        }
    }
    Ok(())
}

fn provide_context_for_truncation_error(error: odbc_api::Error) -> Error {
    match error {
        odbc_api::Error::TooLargeValueForBuffer {
            indicator: Some(required),
            buffer_index,
        } => {
            anyhow!(
                "Truncation of text or binary data detected. Try using of \
                `--max-str-len` larger than {required}. Or do not specify it at all in order to \
                allow for larger values. You can also use the `--ignore-truncation` flag in order \
                to consider truncations warnings only. This will cause the truncated value to be \
                written into the csv, and execution to be continued normally."
            )
        }
        odbc_api::Error::TooLargeValueForBuffer {
            indicator: None,
            buffer_index,
        } => {
            anyhow!(
                "Truncation of text or binary data detected. Try using larger values of \
                `--max-str-len` (or do not specify it at all) in order to allow for larger values. \
                You can also use the `--ignore-truncation` flag in order to consider truncations \
                warnings only. This will cause the truncated value to be written into the csv, and \
                execution to be continued normally. The ODBC driver has been unable to tell how \
                large the value that caused the truncation is."
            )
        }
        other => other.into(),
    }
}
