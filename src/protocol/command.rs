use crate::{
  server::{Connection, Shutdown},
  storage::Db,
};

use super::{
  Frame, Parser,
  cmd::{Set, Unknown},
};

#[derive(Debug)]
pub enum Command {
  Set(Set),

  Unknown(Unknown),
}

impl Command {
  pub fn from_frame(frame: Frame) -> crate::Result<Command> {
    let mut parser = Parser::new(frame)?;

    let command_name = parser.next_string()?.to_lowercase();

    // Match the command name, delegating the rest of the parsing to the
    // specific command.
    let command = match &command_name[..] {
      "set" => Command::Set(Set::parse_frames(&mut parser)?),
      _ => {
        // The command is not recognized and an Unknown command is
        // returned.
        return Ok(Command::Unknown(Unknown::new(command_name)));
      }
    };

    parser.finish()?;

    Ok(command)
  }

  pub(crate) async fn apply(
    self,
    db: &Db,
    dst: &mut Connection,
    _shutdown: &mut Shutdown,
  ) -> crate::Result<()> {
    use Command::*;

    match self {
      Set(cmd) => cmd.apply(db, dst).await,
      Unknown(cmd) => cmd.apply(dst).await,
    }
  }

  pub(crate) fn get_name(&self) -> &str {
    match self {
      Command::Set(_) => "set",
      Command::Unknown(cmd) => cmd.get_name(),
    }
  }
}
