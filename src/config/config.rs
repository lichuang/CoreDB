use std::collections::HashMap;

use configparser::ini::Ini;
use configparser::ini::IniDefault;
use tracing::Level;

use crate::Result;

#[derive(Debug)]
pub struct Config {
  pub server_host: String,
  pub server_port: u32,

  pub log_level: Level,

  pub data_dir: String,
}

impl Default for Config {
  fn default() -> Self {
    Self {
      server_host: "127.0.0.1".to_string(),
      server_port: 6622,
      log_level: Level::INFO,
      data_dir: "./.coredb_data/".to_string(),
    }
  }
}

impl Config {
  pub fn new(config_file: &Option<String>) -> Result<Self> {
    let mut config = Self::default();

    if let Some(config_file) = config_file {
      let mut default = IniDefault::default();
      // default.comment_symbols = vec![';'];
      default.delimiters = vec![' '];

      if let Some(map) = Ini::new_from_defaults(default)
        .load(config_file)
        .unwrap()
        .get("default")
      {
        let config_map: HashMap<_, _> = map
          .iter()
          .into_iter()
          .filter_map(|(k, v)| v.clone().map(|val| (k, val)))
          .collect();

        for (key, value) in config_map.into_iter() {
          if *key == "server_host" {
            config.server_host = value;
          } else if *key == "server_port" {
            config.server_port = value.parse()?;
          } else if *key == "log_level" {
            config.log_level = value.parse()?;
          } else if *key == "data_dir" {
            config.data_dir = value.parse()?;
          }
        }
      }
    }

    Ok(config)
  }
}
