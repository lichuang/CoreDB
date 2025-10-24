pub type Result<T> = anyhow::Result<T>;
pub type Error = anyhow::Error;

pub fn to_error(s: impl ToString) -> Error {
  anyhow::anyhow!(s.to_string())
}
