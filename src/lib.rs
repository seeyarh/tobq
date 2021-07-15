use std::marker::Unpin;

use serde_json::Value;

use gcp_bigquery_client::error::BQError;
use gcp_bigquery_client::model::table_data_insert_all_request::TableDataInsertAllRequest;
use gcp_bigquery_client::Client;

use clap::Clap;

use tokio::io::{stdin, AsyncBufRead, AsyncBufReadExt, BufReader};
use tokio::time::{sleep, Duration};
use tokio_stream::{wrappers::LinesStream, StreamExt};
use tracing::info;

/// Read json lines from stdin, write to bigquery
#[derive(Debug, Clone, Clap)]
#[clap(version = "1.0", author = "Collins Huff")]
pub struct Opts {
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

#[derive(Debug)]
struct BqLocation {
    project: String,
    dataset: String,
    table: String,
}

pub async fn run(opts: Opts) -> Result<(), BQError> {
    let destination = BqLocation {
        project: opts.project,
        dataset: opts.dataset,
        table: opts.table,
    };
    let gcp_sa_key = &opts.service_account;

    let client = gcp_bigquery_client::Client::from_service_account_key_file(gcp_sa_key).await;
    let input = BufReader::new(stdin());
    let stream = LinesStream::new(input.lines());
    let timeout = Duration::from_secs(opts.timeout);

    read_stream(&client, &destination, opts.batch_size, timeout, stream).await?;
    Ok(())
}

async fn read_stream<R: AsyncBufRead + Unpin>(
    client: &Client,
    destination: &BqLocation,
    batch_size: usize,
    timeout: Duration,
    mut stream: LinesStream<R>,
) -> Result<(), BQError> {
    let mut insert_request = TableDataInsertAllRequest::new();
    let mut cur_batch_size: usize = 0;

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

                if cur_batch_size >= batch_size {
                    info!(cur_batch_size, "calling insert with batch size");
                    client
                        .tabledata()
                        .insert_all(&destination.project, &destination.dataset, &destination.table, insert_request)
                        .await?;
                    info!(cur_batch_size, "finished calling insert with batch size");

                    insert_request = TableDataInsertAllRequest::new();
                    cur_batch_size = 0;
                }
            },
            _ = sleep(timeout) => {
                if cur_batch_size > 0 {
                    info!(cur_batch_size, "calling insert with batch size after timeout");
                    client
                        .tabledata()
                        .insert_all(&destination.project, &destination.dataset, &destination.table, insert_request)
                        .await?;
                    info!(cur_batch_size, "finished calling insert with batch size after timeout");

                    insert_request = TableDataInsertAllRequest::new();
                    cur_batch_size = 0;
                }
            },
        }
    }

    if cur_batch_size > 0 {
        info!(cur_batch_size, "calling insert after end of input stream");
        client
            .tabledata()
            .insert_all(
                &destination.project,
                &destination.dataset,
                &destination.table,
                insert_request,
            )
            .await?;
        info!(
            cur_batch_size,
            "finished calling insert after end of input stream"
        );
    }

    Ok(())
}

#[cfg(test)]
mod integration_tests {
    use super::*;
    use chrono::{DateTime, Utc};
    use gcp_bigquery_client::env_vars;
    use gcp_bigquery_client::model::query_request::QueryRequest;
    use gcp_bigquery_client::model::table::Table;
    use gcp_bigquery_client::model::table_field_schema::TableFieldSchema;
    use gcp_bigquery_client::model::table_schema::TableSchema;
    use serde::Serialize;
    use std::io::Cursor;
    use std::time::Duration;
    use tracing_subscriber;

    use rand::distributions::Alphanumeric;
    use rand::{thread_rng, Rng};

    #[derive(Serialize)]
    struct MyRow {
        ts: DateTime<Utc>,
        x: i64,
    }

    #[tokio::test]
    async fn tobq_happy() {
        tracing_subscriber::fmt::init();
        let (ref project_id, ref dataset_id, ref table_id, ref sa_key) = env_vars();

        let mut table_id = String::from(table_id);

        let rand_string: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(30)
            .map(char::from)
            .collect();

        table_id.push_str(&rand_string);

        let destination = BqLocation {
            project: project_id.into(),
            dataset: dataset_id.into(),
            table: table_id.into(),
        };
        eprintln!("{:?}", destination);
        eprintln!("{}", sa_key);

        let client = gcp_bigquery_client::Client::from_service_account_key_file(sa_key).await;
        let dataset = client
            .dataset()
            .get(&destination.project, &destination.dataset)
            .await
            .expect("failed to get dataset");

        let _table = dataset
            .create_table(
                &client,
                Table::from_dataset(
                    &dataset,
                    &destination.table,
                    TableSchema::new(vec![
                        TableFieldSchema::timestamp("ts"),
                        TableFieldSchema::integer("x"),
                    ]),
                )
                .friendly_name("tobq-test table"),
            )
            .await
            .expect("failed to create table");

        let json = r#"{"ts": "2021-01-11 00:00:01 UTC", "x": 1}
{"ts": "2021-01-11 00:00:02 UTC", "x": 2}
{"ts": "2021-01-11 00:00:03 UTC", "x": 3}"#;
        let cursor = Cursor::new(json.as_bytes());

        let input = BufReader::new(cursor);
        let stream = LinesStream::new(input.lines());
        let timeout = Duration::from_secs(10);
        let batch_size = 3;

        read_stream(&client, &destination, batch_size, timeout, stream)
            .await
            .expect("failed to read stream");

        let mut rs = client
            .job()
            .query(
                project_id,
                QueryRequest::new(format!(
                    "SELECT COUNT(*) AS c FROM `{}.{}.{}`",
                    project_id, dataset_id, &destination.table
                )),
            )
            .await
            .expect("failed to execute query");

        while rs.next_row() {
            let n_rows = rs
                .get_i64_by_name("c")
                .expect("failed to get row from query")
                .unwrap();
            assert_eq!(n_rows, 3);

            println!("Number of rows inserted: {}", n_rows)
        }

        client
            .table()
            .delete(project_id, dataset_id, &destination.table)
            .await
            .expect("failed to delete table");
    }
}
