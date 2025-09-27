use std::io::Cursor;

use crate::Result;
use anyhow::bail;
use bytes::Buf;
use bytes::BytesMut;
use tokio::{
  io::{AsyncReadExt, BufWriter},
  net::TcpStream,
  sync::mpsc,
};

use super::shutdown::Shutdown;
use crate::protocol::Frame;

struct RedisStream {
  stream: BufWriter<TcpStream>,

  buffer: BytesMut,
}

pub struct Connection {
  stream: RedisStream,

  shutdown: Shutdown,

  _shutdown_complete: mpsc::Sender<()>,
}

impl Connection {
  pub fn new(
    socket: TcpStream,
    shutdown: Shutdown,
    shutdown_complete: mpsc::Sender<()>,
  ) -> Connection {
    Connection {
      stream: RedisStream::new(socket),
      shutdown,
      _shutdown_complete: shutdown_complete,
    }
  }

  pub async fn run(&mut self) -> Result<()> {
    while !self.shutdown.is_shutdown() {
      let maybe_frame = tokio::select! {
          res = self.stream. read_frame() => res?,
          _ = self.shutdown.recv() => {
              // If a shutdown signal is received, return from `run`.
              // This will result in the task terminating.
              return Ok(());
          }
      };

      let frame = match maybe_frame {
        Some(frame) => frame,
        None => return Ok(()),
      };
    }
    Ok(())
  }
}

impl RedisStream {
  pub fn new(socket: TcpStream) -> RedisStream {
    Self {
      stream: BufWriter::new(socket),
      buffer: BytesMut::with_capacity(4 * 1024),
    }
  }

  async fn read_frame(&mut self) -> Result<Option<Frame>> {
    loop {
      if let Some(frame) = self.parse_frame()? {
        return Ok(Some(frame));
      }

      if 0 == self.stream.read_buf(&mut self.buffer).await? {
        if self.buffer.is_empty() {
          return Ok(None);
        } else {
          bail!("connection reset by peer");
        }
      }
    }
  }

  fn parse_frame(&mut self) -> crate::Result<Option<Frame>> {
    use crate::protocol::Error::Incomplete;

    let mut buf = Cursor::new(&self.buffer[..]);

    match Frame::check(&mut buf) {
      Ok(_) => {
        let len = buf.position() as usize;

        buf.set_position(0);

        let frame = Frame::parse(&mut buf)?;

        self.buffer.advance(len);

        Ok(Some(frame))
      }
      Err(Incomplete) => Ok(None),
      Err(e) => Err(e.into()),
    }
  }
}
