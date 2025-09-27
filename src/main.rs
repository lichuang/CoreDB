use tokio::signal;

mod base;
mod config;
mod protocol;
mod server;

use base::Result;

#[tokio::main]
async fn main() -> Result<()> {
  server::run(signal::ctrl_c()).await
}
