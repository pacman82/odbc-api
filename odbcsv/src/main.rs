use anyhow::Error;
use log::info;
use odbc_api::{buffers::TextRowSet, Cursor, Environment, IntoParameter};
use std::{
    fs::File,
    io::{stdout, Write},
    path::PathBuf,
};
use structopt::{clap::ArgGroup, StructOpt};

/// Query an ODBC data source and output the result as CSV.
#[derive(StructOpt, Debug)]
#[structopt()]
struct Cli {
    /// Verbose mode (-v, -vv, -vvv, etc)
    #[structopt(short = "v", long, parse(from_occurrences))]
    verbose: usize,
    /// Number of rows queried from the database on block. Larger numbers may reduce io overhead,
    /// but require more memory during execution.
    #[structopt(long, default_value = "5000")]
    batch_size: u32,
    /// Path to the output csv file the returned values are going to be written to. If omitted the
    /// csv is going to be printed to standard out.
    #[structopt(long, short = "o")]
    output: Option<PathBuf>,
    /// The connection string used to connect to the ODBC data source. Alternatively you may specify
    /// the ODBC dsn.
    #[structopt(long, short = "c")]
    connection_string: Option<String>,
    /// ODBC Data Source Name. Either this or the connection string must be specified to identify
    /// the datasource. Data source name (dsn) and connection string, may not be specified both.
    #[structopt(long, conflicts_with = "connection-string")]
    dsn: Option<String>,
    /// User used to access the datasource specified in dsn.
    #[structopt(long, short = "u")]
    user: Option<String>,
    /// Password used to log into the datasource. Only used if dsn is specified, instead of a
    /// connection string.
    #[structopt(long, short = "p")]
    password: Option<String>,
    /// Query executed against the ODBC data source. Question marks (`?`) can be used as
    /// placeholders for positional parameters.
    query: String,
    /// For each placeholder question mark (`?`) in the query text one parameter must be passed at
    /// the end of the command line.
    parameters: Vec<String>,
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
    // If an output file has been specified write to it, otherwise use stdout instead.
    let out = stdout();
    let out: Box<dyn Write> = if let Some(path) = opt.output {
        Box::new(File::create(path)?)
    } else {
        Box::new(out.lock())
    };
    let mut writer = csv::Writer::from_writer(out);

    // Initialize logging.
    stderrlog::new()
        .module(module_path!())
        .module("odbc_api")
        .quiet(false)
        .verbosity(opt.verbose)
        .timestamp(stderrlog::Timestamp::Second)
        .init()?;

    // We know this is going to be the only ODBC environment in the entire process, so this is safe.
    let environment = unsafe { Environment::new() }?;

    let mut connection = if let Some(dsn) = opt.dsn {
        environment.connect(
            &dsn,
            opt.user.as_deref().unwrap_or(""),
            opt.password.as_deref().unwrap_or(""),
        )?
    } else {
        environment.connect_with_connection_string(
            &opt.connection_string
                .expect("Connection string must be specified, if dsn is not."),
        )?
    };

    // Convert the input strings into parameters suitable to for use with ODBC.
    let params: Vec<_> = opt
        .parameters
        .iter()
        .map(|param| param.into_parameter())
        .collect();

    // Execute the query as a one off, and pass the parameters.
    match connection.execute(&opt.query, params.as_slice())? {
        Some(cursor) => {
            // Write column names.
            let headline: Vec<String> = cursor.column_names()?.collect::<Result<_, _>>()?;
            writer.write_record(headline)?;

            let mut buffers = TextRowSet::new(opt.batch_size, &cursor)?;
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
                    let record = (0..buffer.num_cols())
                        .map(|col_index| buffer.at(col_index, row_index).unwrap_or(&[]));
                    writer.write_record(record)?;
                }
            }
        }
        None => {
            eprintln!(
                "Query came back empty (not even a schema has been returned). No output has been created."
            );
        }
    }

    Ok(())
}
