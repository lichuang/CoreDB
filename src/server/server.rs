use tokio::{
  net::{TcpListener, TcpStream},
  sync::{broadcast, mpsc},
};
use tracing::{debug, error, info, instrument};

use super::shutdown::Shutdown;

use crate::{Result, server::connection::Connection};

const DEFAULT_PORT: u16 = 6379;

pub struct Server {
  listener: TcpListener,

  notify_shutdown: broadcast::Sender<()>,

  shutdown_complete_tx: mpsc::Sender<()>,
}

impl Server {
  async fn run(&mut self) -> Result<()> {
    info!("accepting connections");

    loop {
      let (socket, client_addr) = self.listener.accept().await?;
      info!("accept connection from {:?}", client_addr);

      let mut connection = Connection::new(
        socket,
        Shutdown::new(self.notify_shutdown.subscribe()),
        self.shutdown_complete_tx.clone(),
      );

      tokio::spawn(async move {
        if let Err(err) = connection.run().await {
          error!(cause = ?err, "connection error");
        }
      });
    }
    Ok(())
  }
}

pub async fn run(shutdown: impl Future) -> Result<()> {
  let listener = TcpListener::bind(&format!("127.0.0.1:{}", DEFAULT_PORT)).await?;

  let (notify_shutdown, _) = broadcast::channel(1);
  let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel(1);

  let mut server = Server {
    listener,
    notify_shutdown,
    shutdown_complete_tx,
  };

  tokio::select! {
      res = server.run() => {
          // If an error is received here, accepting connections from the TCP
          // listener failed multiple times and the server is giving up and
          // shutting down.
          //
          // Errors encountered when handling individual connections do not
          // bubble up to this point.
          if let Err(err) = res {
              error!(cause = %err, "failed to accept");
          }
      }
      _ = shutdown => {
          // The shutdown signal has been received.
          info!("coredb server shutting down");
      }
  }

  let Server {
    shutdown_complete_tx,
    notify_shutdown,
    ..
  } = server;

  drop(notify_shutdown);
  drop(shutdown_complete_tx);

  let _ = shutdown_complete_rx.recv().await;

  Ok(())
}
