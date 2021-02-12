use anyhow::{bail, Error};
use log::info;
use odbc_api::{buffers::TextRowSet, Connection, Cursor, Environment, IntoParameter};
use std::{
    fs::File,
    io::{stdin, stdout, Read, Write},
    path::PathBuf,
};
use structopt::{clap::ArgGroup, StructOpt};

/// Query an ODBC data source and output the result as CSV.
#[derive(StructOpt)]
struct Cli {
    /// Verbose mode (-v, -vv, -vvv, etc)
    #[structopt(short = "v", long, parse(from_occurrences))]
    verbose: usize,
    #[structopt(subcommand)]
    command: Command,
}

#[derive(StructOpt)]
enum Command {
    /// Query a data source and write the result as csv.
    Query {
        #[structopt(flatten)]
        query_opt: QueryOpt,
    },
    /// Read the content of a csv and insert it into a table.
    Insert {
        #[structopt(flatten)]
        insert_opt: InsertOpt,
    },
    /// List available drivers. Useful to find out which exact driver name to specify in the
    /// connections string.
    ListDrivers,
    /// List preconfigured datasources. Useful to find data source name to connect to database.
    ListDataSources,
}

/// Command line arguments used to establish a connection with the ODBC data source
#[derive(StructOpt)]
struct ConnectOpts {
    /// The connection string used to connect to the ODBC data source. Alternatively you may
    /// specify the ODBC dsn.
    #[structopt(long, short = "c")]
    connection_string: Option<String>,
    /// ODBC Data Source Name. Either this or the connection string must be specified to identify
    /// the datasource. Data source name (dsn) and connection string, may not be specified both.
    #[structopt(long, conflicts_with = "connection-string")]
    dsn: Option<String>,
    /// User used to access the datasource specified in dsn.
    #[structopt(long, short = "u", env = "ODBC_USER")]
    user: Option<String>,
    /// Password used to log into the datasource. Only used if dsn is specified, instead of a
    /// connection string.
    #[structopt(long, short = "p", env = "ODBC_PASSWORD", hide_env_values = true)]
    password: Option<String>,
}

#[derive(StructOpt)]
struct QueryOpt {
    #[structopt(flatten)]
    connect_opts: ConnectOpts,
    /// Number of rows queried from the database on block. Larger numbers may reduce io overhead,
    /// but require more memory during execution.
    #[structopt(long, default_value = "5000")]
    batch_size: u32,
    /// Path to the output csv file the returned values are going to be written to. If omitted the
    /// csv is going to be printed to standard out.
    #[structopt(long, short = "o")]
    output: Option<PathBuf>,
    /// Query executed against the ODBC data source. Question marks (`?`) can be used as
    /// placeholders for positional parameters.
    query: String,
    /// For each placeholder question mark (`?`) in the query text one parameter must be passed at
    /// the end of the command line.
    parameters: Vec<String>,
}

#[derive(StructOpt)]
struct InsertOpt {
    #[structopt(flatten)]
    connect_opts: ConnectOpts,
    /// Number of rows inserted into the database on block. Larger numbers may reduce io overhead,
    /// but require more memory during execution.
    #[structopt(long, default_value = "5000")]
    batch_size: u32,
    /// Path to the input csv file which is used to fill the database table with values. If
    /// omitted stdin is used.
    #[structopt(long, short = "i")]
    input: Option<PathBuf>,
    /// Name of the table to insert the values into. No precautions against SQL injection are
    /// taken.
    table: String,
}

fn main() -> Result<(), Error> {
    // Verify that either `dsn` or `connection-string` is specified.
    Cli::clap().group(
        ArgGroup::with_name("source")
            .required(true)
            .args(&["dsn", "connection-string"]),
    );
    // Parse arguments from command line interface
    let opt = Cli::from_args();

    // Initialize logging.
    stderrlog::new()
        .module(module_path!())
        .module("odbc_api")
        .quiet(false)
        .verbosity(opt.verbose)
        .timestamp(stderrlog::Timestamp::Second)
        .init()?;

    // We know this is going to be the only ODBC environment in the entire process, so this is safe.
    let mut environment = unsafe { Environment::new() }?;

    match opt.command {
        Command::Query { query_opt } => {
            query(&environment, &query_opt)?;
        }
        Command::Insert { insert_opt } => {
            if insert_opt.batch_size == 0 {
                bail!("batch size, must be at least 1");
            }
            insert(&environment, &insert_opt)?;
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
                    println!("\t{}={}", key, value);
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
) -> Result<Connection<'e>, odbc_api::Error> {
    if let Some(dsn) = opt.dsn.as_deref() {
        environment.connect(
            dsn,
            opt.user.as_deref().unwrap_or(""),
            opt.password.as_deref().unwrap_or(""),
        )
    } else {
        environment.connect_with_connection_string(
            opt.connection_string
                .as_deref()
                .expect("Connection string must be specified, if dsn is not."),
        )
    }
}

/// Execute a query and writes the result to csv.
fn query(environment: &Environment, opt: &QueryOpt) -> Result<(), Error> {
    let QueryOpt {
        connect_opts,
        output,
        parameters,
        query,
        batch_size,
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

    let connection = open_connection(&environment, connect_opts)?;

    // Convert the input strings into parameters suitable to for use with ODBC.
    let params: Vec<_> = parameters
        .iter()
        .map(|param| param.into_parameter())
        .collect();

    // Execute the query as a one off, and pass the parameters.
    match connection.execute(&query, params.as_slice())? {
        Some(cursor) => {
            // Write column names.
            let headline: Vec<String> = cursor.column_names()?.collect::<Result<_, _>>()?;
            writer.write_record(headline)?;

            let mut buffers = TextRowSet::for_cursor(*batch_size, &cursor)?;
            let mut row_set_cursor = cursor.bind_buffer(&mut buffers)?;

            // Use this number to count the batches. Only used for logging.
            let mut num_batch = 0;
            while let Some(buffer) = row_set_cursor.fetch()? {
                num_batch += 1;
                info!(
                    "Fetched batch {} with {} rows.",
                    num_batch,
                    buffer.num_rows()
                );
                for row_index in 0..buffer.num_rows() {
                    let record = (0..buffer.num_cols()).map(|col_index| {
                        buffer
                            .at(col_index, row_index)
                            .unwrap_or(&[])
                    });
                    writer.write_record(record)?;
                }
            }
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
    let connection = open_connection(&environment, connect_opts)?;

    // Generate statement text from table name and headline
    let headline = reader.byte_headers()?;
    let column_names: Vec<&str> = headline
        .iter()
        .map(|bytes| std::str::from_utf8(bytes))
        .collect::<Result<_, _>>()?;
    let columns = column_names.join(", ");
    let values = column_names
        .iter()
        .map(|_| "?")
        .collect::<Vec<_>>()
        .join(", ");
    let statement_text = format!("INSERT INTO {} ({}) VALUES ({});", table, columns, values);
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
    let mut buffer = TextRowSet::new(*batch_size, (0..headline.len()).map(|_| 0));

    for try_record in reader.into_byte_records() {
        if buffer.num_rows() == *batch_size as usize {
            // Batch is full. We need to send it to the data base and clear it, before we insert
            // more rows into it.
            statement.execute(&buffer)?;
            buffer.clear();
        }

        let record = try_record?;
        buffer.append(
            record
                .iter()
                .map(|field| if field.is_empty() { None } else { Some(field) }),
        );
    }

    // Insert the remainder of the buffer to the database. If buffer is empty nothing will be
    // executed.
    statement.execute(&buffer)?;

    Ok(())
}
