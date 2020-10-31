use anyhow::Error;
use log::info;
use odbc_api::{buffers::TextRowSet, Cursor, Environment};
use std::{
    char::decode_utf16,
    fs::File,
    io::{stdout, Write},
    path::PathBuf,
};
use structopt::StructOpt;

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
    #[structopt(long, short = "-o")]
    output: Option<PathBuf>,
    /// The connection string used to connect to the ODBC datasource.
    #[structopt()]
    connection_string: String,
    /// Query executed against the ODBC data source.
    #[structopt()]
    query: String,
    /// You can pass replace placeholder question marks ('?') in the query text with parameters.
    parameters: Vec<String>,
}

fn main() -> Result<(), Error> {
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

    let mut connection = environment.connect_with_connection_string(&opt.connection_string)?;

    match connection.exec_direct(&opt.query, ())? {
        Some(cursor) => {
            let num_cols = cursor.num_result_cols()?;
            // Some ODBC drivers do not report the required size to hold the column name. Starting
            // with a reasonable sized buffers, allows us to fetch reasonable sized column alias
            // even from those.
            let mut buf_wchar = Vec::with_capacity(128);
            let mut headline = Vec::new();
            let mut buffers = TextRowSet::new(opt.batch_size, &cursor)?;

            // Loop over the number of reported columns in the result set and fetch the Column name
            // for each one, if available.
            for index in 1..(num_cols as u16 + 1) {
                cursor.col_name(index, &mut buf_wchar)?;
                let name =
                    decode_utf16(buf_wchar.iter().copied()).collect::<Result<String, _>>()?;
                headline.push(name);
            }

            let mut row_set_cursor = cursor.bind_row_set_buffer(&mut buffers)?;

            writer.write_record(headline)?;

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
