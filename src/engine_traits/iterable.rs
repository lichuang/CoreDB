use super::errors::Result;
use super::options::IterOptions;
use crate::util::KeyBuilder;

pub trait Iterator: Send {
  /// Move the iterator to a specific key.
  ///
  /// When an exact match is not found, `seek` sets the iterator to the next
  /// key greater than that specified as `key`, if such a key exists;
  /// `seek_for_prev` sets the iterator to the previous key less than
  /// that specified as `key`, if such a key exists.
  ///
  /// # Returns
  ///
  /// `true` if seeking succeeded and the iterator is valid,
  /// `false` if seeking failed and the iterator is invalid.
  fn seek(&mut self, key: &[u8]) -> Result<bool>;

  /// Move the iterator to a specific key.
  ///
  /// For the difference between this method and `seek`,
  /// see the documentation for `seek`.
  ///
  /// # Returns
  ///
  /// `true` if seeking succeeded and the iterator is valid,
  /// `false` if seeking failed and the iterator is invalid.
  fn seek_for_prev(&mut self, key: &[u8]) -> Result<bool>;

  /// Seek to the first key in the engine.
  fn seek_to_first(&mut self) -> Result<bool>;

  /// Seek to the last key in the database.
  fn seek_to_last(&mut self) -> Result<bool>;

  /// Move a valid iterator to the previous key.
  ///
  /// # Panics
  ///
  /// If the iterator is invalid, iterator may panic or aborted.
  fn prev(&mut self) -> Result<bool>;

  /// Move a valid iterator to the next key.
  ///
  /// # Panics
  ///
  /// If the iterator is invalid, iterator may panic or aborted.
  fn next(&mut self) -> Result<bool>;

  /// Retrieve the current key.
  ///
  /// # Panics
  ///
  /// If the iterator is invalid, iterator may panic or aborted.
  fn key(&self) -> &[u8];

  /// Retrieve the current value.
  ///
  /// # Panics
  ///
  /// If the iterator is invalid, iterator may panic or aborted.
  fn value(&self) -> &[u8];

  /// Returns `true` if the iterator points to a `key`/`value` pair.
  fn valid(&self) -> Result<bool>;
}

pub trait RefIterable {
  type Iterator<'a>: Iterator
  where Self: 'a;

  fn iter(&self, opts: IterOptions) -> Result<Self::Iterator<'_>>;
}

pub trait Iterable {
  type Iterator: Iterator;

  fn iterator_opt(&self, cf: &str, opts: IterOptions) -> Result<Self::Iterator>;

  fn iterator(&self, cf: &str) -> Result<Self::Iterator> {
    self.iterator_opt(cf, IterOptions::default())
  }

  /// scan the key between start_key(inclusive) and end_key(exclusive),
  /// the upper bound is omitted if end_key is empty
  fn scan<F>(
    &self,
    cf: &str,
    start_key: &[u8],
    end_key: &[u8],
    fill_cache: bool,
    f: F,
  ) -> Result<()>
  where
    F: FnMut(&[u8], &[u8]) -> Result<bool>,
  {
    let iter_opt = iter_option(start_key, end_key, fill_cache);
    scan_impl(self.iterator_opt(cf, iter_opt)?, start_key, f)
  }

  // Seek the first key >= given key, if not found, return None.
  fn seek(&self, cf: &str, key: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
    let mut iter = self.iterator(cf)?;
    if iter.seek(key)? {
      return Ok(Some((iter.key().to_vec(), iter.value().to_vec())));
    }
    Ok(None)
  }
}

fn scan_impl<Iter, F>(mut it: Iter, start_key: &[u8], mut f: F) -> Result<()>
where
  Iter: Iterator,
  F: FnMut(&[u8], &[u8]) -> Result<bool>,
{
  let mut remained = it.seek(start_key)?;
  while remained {
    remained = f(it.key(), it.value())? && it.next()?;
  }
  Ok(())
}

pub fn iter_option(lower_bound: &[u8], upper_bound: &[u8], fill_cache: bool) -> IterOptions {
  let lower_bound = Some(KeyBuilder::from_slice(lower_bound, 0, 0));
  let upper_bound = if upper_bound.is_empty() {
    None
  } else {
    Some(KeyBuilder::from_slice(upper_bound, 0, 0))
  };
  IterOptions::new(lower_bound, upper_bound, fill_cache)
}
