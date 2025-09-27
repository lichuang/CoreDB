use tokio::signal;

mod base;
mod config;
mod protocol;
mod server;
mod storage;

use base::Result;

use tracing::{Level, info};
use tracing_subscriber::fmt;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
  let subscriber = fmt::Subscriber::builder()
    .with_max_level(Level::DEBUG)
    .with_ansi(true)
    //.with_timer(fmt::time::UtcTime::rfc_3339())
    .with_span_events(FmtSpan::CLOSE)
    .finish();
  tracing::subscriber::set_global_default(subscriber).unwrap();

  server::run(signal::ctrl_c()).await
}
