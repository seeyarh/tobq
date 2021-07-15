use clap::Clap;
use gcp_bigquery_client::error::BQError;
use tobq::{run, Opts};
use tracing_subscriber;

#[tokio::main]
async fn main() -> Result<(), BQError> {
    let opts: Opts = Opts::parse();
    tracing_subscriber::fmt::init();
    run(opts).await?;
    Ok(())
}
