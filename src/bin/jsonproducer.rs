use clap::Parser;
use precise_rate_limiter::FastQuotaSync;
use rdkafka::producer::{BaseRecord, Producer, ThreadedProducer};
use rdkafka::util::Timeout;
use kafka_avro_utility::fastjsonfile::load_json_file;
use kafka_avro_utility::kafka_config::KafkaConfig;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(name = "jsonproducer")]
#[command(about = "Produce JSON records from a file to Kafka")]
struct Args {
    /// Path to Kafka client configuration file
    #[arg(long, default_value = "client.properties")]
    client_config: String,

    /// Input JSON file path
    #[arg(long, required = true)]
    in_file: PathBuf,

    /// Destination Kafka topic
    #[arg(long, required = true)]
    topic: String,

    /// Number of messages to produce. When not specified, produces unlimited messages.
    #[arg(long)]
    num_messages: Option<u64>,

    /// Production speed limit in MiB/s. When not specified, no limit is applied.
    #[arg(long)]
    speed: Option<f64>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Load Kafka configuration and create producer with custom context
    let kafka_config = KafkaConfig::from_file(&args.client_config)?;

    // Create a custom producer context to handle delivery reports
    struct ProducerContext {
        error_count: std::sync::Arc<std::sync::atomic::AtomicU64>,
    }

    impl rdkafka::ClientContext for ProducerContext {}

    impl rdkafka::producer::ProducerContext for ProducerContext {
        type DeliveryOpaque = ();

        fn delivery(
            &self,
            delivery_result: &rdkafka::message::DeliveryResult<'_>,
            _delivery_opaque: Self::DeliveryOpaque,
        ) {
            match delivery_result {
                Ok(_) => {
                    // Message delivered successfully
                }
                Err((err, _msg)) => {
                    eprintln!("Delivery error: {:?}", err);
                    self.error_count
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }
    }

    let error_count = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let context = ProducerContext {
        error_count: error_count.clone(),
    };

    let producer: ThreadedProducer<ProducerContext> = kafka_config.create_producer(context)?;

    // Load JSON file
    let mut iterator = load_json_file(&args.in_file)?;

    let mut count = 0u64;
    let mut total_bytes = 0u64;
    let start_time = std::time::Instant::now();

    // Determine if we have a message limit
    let message_limit = args.num_messages;

    // Create rate limiter if speed limit is specified
    let rate_limiter = args.speed.map(|speed_mib| {
        let bytes_per_sec = (speed_mib * 1024.0 * 1024.0) as usize;
        println!(
            "Rate limiting enabled: {:.2} MiB/s ({} bytes/s)",
            speed_mib, bytes_per_sec
        );
        let refill_interval = Duration::from_millis(100);
        let refill_amount = bytes_per_sec / 10; // 10 refills per second
        FastQuotaSync::new(bytes_per_sec, refill_amount, refill_interval)
    });

    // Reuse payload buffer to avoid allocations
    loop {
        // Check if we've reached the message limit
        if let Some(limit) = message_limit {
            if count >= limit {
                break;
            }
        }

        // Get next record from iterator
        let record_bytes = match iterator.next() {
            Some(record) => record,
            None => {
                // End of file reached
                if message_limit.is_some() {
                    // We have a limit, so reopen the file and continue
                    println!("End of file reached, reopening from beginning...");
                    let new_iterator = load_json_file(&args.in_file)?;
                    iterator = new_iterator;
                    match iterator.next() {
                        Some(record) => record,
                        None => {
                            eprintln!("Error: File is empty, cannot continue");
                            break;
                        }
                    }
                } else {
                    // No limit specified, we're done
                    break;
                }
            }
        };

        match record_bytes {
            Ok(bytes_entry) => {
                let bytes_raw = bytes_entry.get_data();
                let payload_size = bytes_raw.len() as u64;

                // Apply rate limiting if enabled
                if let Some(ref limiter) = rate_limiter {
                    limiter.acquire(bytes_raw.len());
                }

                let retry_start = std::time::Instant::now();
                let max_retry_duration = Duration::from_secs(5);
                let mut retry_count = 0;
                const MAX_RETRIES: u32 = 100;

                loop {
                    let record_copy: BaseRecord<(), [u8], ()> = BaseRecord::to(&args.topic).payload(bytes_raw);
                    match producer.send(record_copy) {
                        Ok(_) => {
                            count += 1;
                            total_bytes += payload_size;

                            if count % 10000 == 0 {
                                let elapsed = start_time.elapsed().as_secs_f64();
                                let msg_rate = count as f64 / elapsed;
                                let byte_rate = total_bytes as f64 / elapsed;

                                println!(
                                    "Produced {} messages, {} total ({:.2} msg/s, {}/s)",
                                    count,
                                    humanize_bytes(total_bytes),
                                    msg_rate,
                                    humanize_bytes(byte_rate as u64)
                                );
                            }
                            // Poll more frequently to prevent queue buildup
                            if count % 1000 == 0 {
                                producer.poll(Timeout::After(Duration::from_millis(0)));
                            }
                            break;
                        }
                        Err((
                            rdkafka::error::KafkaError::MessageProduction(
                                rdkafka::types::RDKafkaErrorCode::QueueFull,
                            ),
                            _returned_record,
                        )) => {
                            retry_count += 1;

                            // Check if we've exceeded retry limits
                            if retry_count > MAX_RETRIES
                                || retry_start.elapsed() > max_retry_duration
                            {
                                eprintln!(
                                    "Error: Queue full after {} retries and {:.2}s. Topic may not exist or broker is unavailable.",
                                    retry_count,
                                    retry_start.elapsed().as_secs_f64()
                                );
                                return Err("Producer queue full - check if topic exists and broker is reachable".into());
                            }

                            // Queue is full, poll and retry
                            producer.poll(Timeout::After(Duration::from_millis(10)));
                            continue;
                        }
                        Err((e, _)) => {
                            eprintln!("Error producing message: {:?}", e);
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("Error reading record from file: {}", e);
                break;
            }
        }
    }

    // Flush remaining messages
    println!("Flushing remaining messages...");
    producer.flush(Timeout::After(Duration::from_secs(30)))?;

    let elapsed = start_time.elapsed().as_secs_f64();
    let msg_rate = count as f64 / elapsed;
    let byte_rate = total_bytes as f64 / elapsed;
    let errors = error_count.load(std::sync::atomic::Ordering::Relaxed);

    println!(
        "Done. Produced {} messages, {} total in {:.2}s ({:.2} msg/s, {}/s)",
        count,
        humanize_bytes(total_bytes),
        elapsed,
        msg_rate,
        humanize_bytes(byte_rate as u64)
    );

    if errors > 0 {
        eprintln!("Warning: {} messages failed delivery", errors);
        return Err(format!("{} messages failed delivery", errors).into());
    }

    Ok(())
}

/// Humanize bytes into human-readable format (KB, MB, GB, etc.)
fn humanize_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];

    if bytes == 0 {
        return "0 B".to_string();
    }

    let mut size = bytes as f64;
    let mut unit_idx = 0;

    while size >= 1024.0 && unit_idx < UNITS.len() - 1 {
        size /= 1024.0;
        unit_idx += 1;
    }

    if unit_idx == 0 {
        format!("{} {}", bytes, UNITS[0])
    } else {
        format!("{:.2} {}", size, UNITS[unit_idx])
    }
}

