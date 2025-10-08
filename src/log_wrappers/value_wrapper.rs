use std::fmt;

#[derive(Clone, Copy)]
pub struct Value<'a>(pub &'a [u8]);

impl<'a> Value<'a> {
  pub fn key(key: &'a [u8]) -> Self {
    Value(key)
  }

  pub fn value(v: &'a [u8]) -> Self {
    Value(v)
  }
}

impl fmt::Display for Value<'_> {
  #[inline]
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "{}", hex::encode_upper(self.0))
  }
}

impl fmt::Debug for Value<'_> {
  #[inline]
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    fmt::Display::fmt(self, f)
  }
}
