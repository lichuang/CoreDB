use std::time::Duration;

#[derive(Debug, Clone)]
pub(crate) struct Backoff {
  /// delay increase ratio of meta
  ///
  /// should be not little than 1.0
  back_off_ratio: f32,
  /// min delay duration of back off
  back_off_min_delay: Duration,
  /// max delay duration of back off
  back_off_max_delay: Duration,
  /// chances of back off
  back_off_chances: u64,
}

impl Backoff {
  /// Set exponential back off policy for meta service
  ///
  /// - `ratio`: delay increase ratio of meta
  ///
  ///   should be not smaller than 1.0
  /// - `min_delay`: minimum back off duration, where the backoff duration vary starts from
  /// - `max_delay`: maximum back off duration, if the backoff duration is larger than this, no backoff will be raised
  /// - `chances`: maximum back off times, chances off backoff
  #[allow(dead_code)]
  pub fn with_back_off_policy(
    mut self,
    ratio: f32,
    min_delay: Duration,
    max_delay: Duration,
    chances: u64,
  ) -> Self {
    self.back_off_ratio = ratio;
    self.back_off_min_delay = min_delay;
    self.back_off_max_delay = max_delay;
    self.back_off_chances = chances;
    self
  }
}

impl Default for Backoff {
  fn default() -> Self {
    Self {
      back_off_ratio: 1.5,
      back_off_min_delay: Duration::from_millis(50),
      back_off_max_delay: Duration::from_millis(1_000),
      back_off_chances: 10,
    }
  }
}
