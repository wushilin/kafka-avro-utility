use chrono::Local;
use kafka_avro_utility::fieldspec::{Fluctuate, FluctUnit, TemplateToken};
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::Duration;

fn main() {
    let fl = Fluctuate {
        min: 1,
        max: 20,
        delta_min: 1,
        delta_max: 3,
        unit: FluctUnit::Second,
        key: "demo".to_string(),
        state: Arc::new(Mutex::new(None)),
    };

    println!("Fluctuate demo (range 1..=20, step>=2, bucket=second). Press Ctrl+C to stop.");
    let mut i: u64 = 0;
    loop {
        let val = fl.to_token();
        let bucket = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
        println!("[{:06}] bucket: {} value: {}", i, bucket, val);
        i += 1;
        sleep(Duration::from_millis(100));
    }
}
