use serde_json::Value;

use gcp_bigquery_client::error::BQError;
use gcp_bigquery_client::model::table_data_insert_all_request::TableDataInsertAllRequest;

use clap::Clap;

use tokio::io::{stdin, AsyncBufReadExt, BufReader};
use tokio::time::{sleep, Duration, Instant};
use tokio_stream::{wrappers::LinesStream, StreamExt};
use tracing::info;
use tracing_subscriber;

/// Read json lines from stdin, write to bigquery
#[derive(Debug, Clone, Clap)]
#[clap(version = "1.0", author = "Collins Huff")]
struct Opts {
    /// interface name
    #[clap(short, long)]
    project: String,
    #[clap(short, long)]
    dataset: String,
    #[clap(short, long)]
    table: String,
    #[clap(short, long)]
    service_account: String,
    #[clap(short, long)]
    batch_size: usize,
    #[clap(short, long)]
    timeout: u64,
}

#[tokio::main]
async fn main() -> Result<(), BQError> {
    let opts: Opts = Opts::parse();
    tracing_subscriber::fmt::init();

    let project_id = &opts.project;
    let dataset_id = &opts.dataset;
    let table_id = &opts.table;
    let gcp_sa_key = &opts.service_account;

    let client = gcp_bigquery_client::Client::from_service_account_key_file(gcp_sa_key).await;
    let input = BufReader::new(stdin());
    let mut stream = LinesStream::new(input.lines());

    let mut start = Instant::now();
    let mut insert_request = TableDataInsertAllRequest::new();
    let mut cur_batch_size = 0;

    let timeout = Duration::from_secs(opts.timeout);
    loop {
        tokio::select! {
            line = stream.next() => {
                let line = match line {
                    Some(line) => line,
                    None => break,
                };
                let line = line?;
                let row: Value = serde_json::from_str(&line)?;
                insert_request.add_row(None, row)?;
                cur_batch_size += 1;

                if start.elapsed().as_secs() > opts.timeout || cur_batch_size >= opts.batch_size {
                    info!(cur_batch_size, "calling insert with batch size");
                    client
                        .tabledata()
                        .insert_all(project_id, dataset_id, table_id, insert_request)
                        .await?;
                    info!(cur_batch_size, "finished calling insert with batch size");

                    start = Instant::now();
                    insert_request = TableDataInsertAllRequest::new();
                    cur_batch_size = 0;
                }
            },
            _ = sleep(timeout) => {
                    info!(cur_batch_size, "calling insert with batch size after timeout");
                    client
                        .tabledata()
                        .insert_all(project_id, dataset_id, table_id, insert_request)
                        .await?;
                    info!(cur_batch_size, "finished calling insert with batch size");

                    start = Instant::now();
                    insert_request = TableDataInsertAllRequest::new();
                    cur_batch_size = 0;
            },
        }
    }

    client
        .tabledata()
        .insert_all(project_id, dataset_id, table_id, insert_request)
        .await?;

    Ok(())
}
