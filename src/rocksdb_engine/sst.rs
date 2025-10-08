use std::sync::Arc;

use rocksdb::{ColumnFamilyOptions, Env, SstFileReader};

pub struct RocksSstReader {
  inner: SstFileReader,
}

impl RocksSstReader {
  pub fn open_with_env(path: &str, env: Option<Arc<Env>>) -> Result<Self> {
    let mut cf_options = ColumnFamilyOptions::new();
    if let Some(env) = env {
      cf_options.set_env(env);
    }
    let mut reader = SstFileReader::new(cf_options);
    reader.open(path).map_err(r2e)?;
    Ok(RocksSstReader { inner: reader })
  }

  pub fn compression_name(&self) -> String {
    let mut result = String::new();
    self.inner.read_table_properties(|p| {
      result = p.compression_name().to_owned();
    });
    result
  }
}
