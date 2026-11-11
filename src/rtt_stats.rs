use std::time::Duration;

#[derive(Default, Clone)]
pub struct RttStats {
  samples: Vec<f64>,
  pub timeouts: u64,
  pub send_errors: u64,
}

pub struct Snapshot {
  pub count: usize,
  pub last: Option<f64>,
  pub mean: Option<f64>,
  pub min: Option<f64>,
  pub max: Option<f64>,
  pub p50: Option<f64>,
  pub p95: Option<f64>,
  pub p99: Option<f64>,
  pub stddev: Option<f64>,
}

impl RttStats {
  pub fn record(&mut self, duration: Duration) {
    //Retain up to 100,000 samples; percential metrics cover the retained window.
    if self.samples.len() == 100_000 { self.samples.remove(0); }
    self.samples.push(duration.as_secs_f64() * 1000.0);
    
  }

  pub fn success_rate(&self) -> f64 {
    let total = self.samples.len() as u64 + self.timeouts;
    if total == 0 { 
      0.0
    }
    else {
      100.0 * self.samples.len() as f64 / total as f64
    }
  }

  pub fn snapshot(&self) -> Snapshot {
    let n = self.samples.len();
    if n == 0 {
      return Snapshot {count: 0, last: None, mean: None, min: None, max: None,
                p50: None, p95: None, p99: None, stddev: None };
    }
    let mut sorted = self.samples.clone();
    sorted.sort_by(f64::total_cmp);
    let mean = self.samples.iter().sum::<f64>() / n as f64;
    let variance = self.samples.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n as f64;
    let percentile = |p: f64| -> f64 {
      let position = (n - 1) as f64 * p;
      let low = position.floor() as usize;
      let high = position.ceil() as usize;
      sorted[low] + (sorted[high] - sorted[low]) * position.fract()
    };
    
    Snapshot {
        count: n, last: self.samples.last().copied(), 
        mean: Some(mean),
        min: sorted.first().copied(), 
        max: sorted.last().copied(),
        p50: Some(percentile(0.50)), 
        p95: Some(percentile(0.95)),
        p99: Some(percentile(0.99)), 
        stddev: Some(variance.sqrt()),
    }
  }
}