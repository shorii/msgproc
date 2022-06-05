use std::time::Duration;

pub trait IJobPolicy {
    fn interval(&self) -> Duration;
    fn check(&self) -> bool;
    fn reset(&mut self);
    fn update(&mut self);
}

pub struct DefaultJobPolicy {
    interval: Duration,
    limit: usize,
    count: usize,
}

impl DefaultJobPolicy {
    pub fn new(interval: Duration, limit: usize) -> Self {
        Self {
            interval,
            limit,
            count: 0,
        }
    }
}

impl IJobPolicy for DefaultJobPolicy {
    fn interval(&self) -> Duration {
        self.interval
    }

    fn check(&self) -> bool {
        self.count >= self.limit
    }

    fn reset(&mut self) {
        self.count = 0;
    }

    fn update(&mut self) {
        self.count += 1;
    }
}
