use clap::Parser;
use precise_rate_limiter::FastQuotaSync;
use rdkafka::producer::{BaseRecord, Producer, ThreadedProducer};
use rdkafka::util::Timeout;
use kafka_avro_utility::fastprotofile::load_bytes_file;
use kafka_avro_utility::kafka_config::KafkaConfig;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(name = "protoproducer")]
#[command(about = "Produce Protobuf records from a file to Kafka")]
struct Args {
    /// Path to Kafka client configuration file
    #[arg(long, default_value = "client.properties")]
    client_config: String,

    /// Path to Schema Registry configuration file
    #[arg(long, default_value = "sr.properties")]
    sr_config: String,


    /// Input Protobuf file path
    #[arg(long, required = true)]
    in_file: PathBuf,

    /// Name of the message type (fully qualified). Optional if the input file is self-describing.
    #[arg(long)]
    message: Option<String>,

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

    // Load Kafka configuration
    let kafka_config = KafkaConfig::from_file(&args.client_config)?;

    // Create a custom producer context
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
                Ok(_) => {}
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

    // Load Schema Registry configuration
    let sr_config = KafkaConfig::from_file(&args.sr_config)?;
    let sr_url = sr_config
        .get_schema_registry_url()
        .ok_or("Schema Registry URL not found")?
        .clone();

    let auth_info = sr_config
        .get("schema.registry.basic.auth.user.info")
        .cloned();

    // Load Proto file (raw bytes)
    let (schema_text, file_message_name, mut iterator) = load_bytes_file(&args.in_file)?;

    let effective_message_name = if !file_message_name.is_empty() {
        if let Some(expected) = &args.message {
            if expected != &file_message_name {
                eprintln!(
                    "Error: input file contains message '{}', but --message was '{}'.",
                    file_message_name, expected
                );
                std::process::exit(1);
            }
        }
        file_message_name.clone()
    } else if let Some(expected) = &args.message {
        expected.clone()
    } else {
        eprintln!("Error: input file does not contain message metadata. Please specify --message.");
        std::process::exit(1);
    };

    println!("File contains message '{}'.", effective_message_name);

    // We need to register the schema if possible, or use a dummy ID.
    // Since we have the message name from CLI args, we could try to fetch the latest schema for the subject
    // and use its ID if it matches.
    // For this implementation, we will assume the schema is already registered or use a dummy ID.
    
    let subject = format!("{}-value", args.topic);
    println!("Target subject: {}", subject);

    // Register schema (PROTOBUF) to obtain schema ID
    let schema_id = register_proto_schema(&sr_url, auth_info.as_deref(), &subject, &schema_text)?;
    println!("Registered schema with ID: {} for subject: {}", schema_id, subject);

    let mut count = 0u64;
    let mut total_bytes = 0u64;
    let start_time = std::time::Instant::now();

    let message_limit = args.num_messages;

    let rate_limiter = args.speed.map(|speed_mib| {
        let bytes_per_sec = (speed_mib * 1024.0 * 1024.0) as usize;
        println!(
            "Rate limiting enabled: {:.2} MiB/s ({} bytes/s)",
            speed_mib, bytes_per_sec
        );
        let refill_interval = Duration::from_millis(100);
        let refill_amount = bytes_per_sec / 10;
        FastQuotaSync::new(bytes_per_sec, refill_amount, refill_interval)
    });

    loop {
        if let Some(limit) = message_limit {
            if count >= limit {
                break;
            }
        }

        let record_bytes = match iterator.next() {
            Some(record) => record,
            None => {
                if message_limit.is_some() {
                    println!("End of file reached, reopening from beginning...");
                    let (_, _, new_iterator) = load_bytes_file(&args.in_file)?;
                    iterator = new_iterator;
                    match iterator.next() {
                        Some(record) => record,
                        None => {
                            eprintln!("Error: File is empty, cannot continue");
                            break;
                        }
                    }
                } else {
                    break;
                }
            }
        };

        match record_bytes {
            Ok(mut raw_bytes) => {
                // Set Schema ID (Confluent Wire Format: Magic Byte 0 + 4 byte ID + Data)
                raw_bytes.set_schema_id(schema_id);

                let bytes_raw = raw_bytes.take();
                let payload_size = bytes_raw.len() as u64;

                if let Some(ref limiter) = rate_limiter {
                    limiter.acquire(bytes_raw.len());
                }

                let retry_start = std::time::Instant::now();
                let max_retry_duration = Duration::from_secs(5);
                let mut retry_count = 0;
                const MAX_RETRIES: u32 = 100;

                loop {
                    let record_copy: BaseRecord<(), [u8], ()> = BaseRecord::to(&args.topic).payload(&bytes_raw);
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
                            if retry_count > MAX_RETRIES || retry_start.elapsed() > max_retry_duration {
                                eprintln!("Error: Queue full after retries.");
                                return Err("Producer queue full".into());
                            }
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

/// Register Protobuf schema and return schema ID
fn register_proto_schema(
    base_url: &str,
    auth_info: Option<&str>,
    subject: &str,
    schema: &str,
) -> Result<u32, Box<dyn std::error::Error>> {
    use reqwest::blocking::Client;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize)]
    struct RegisterRequest {
        schema: String,
        #[serde(rename = "schemaType")]
        schema_type: String,
    }

    #[derive(Deserialize)]
    struct RegisterResponse {
        id: u32,
    }

    let url = format!("{}/subjects/{}/versions", base_url, subject);

    // Create HTTP client with SSL verification disabled
    let client = Client::builder()
        .danger_accept_invalid_certs(true)
        .build()?;

    let mut request = client.post(&url).json(&RegisterRequest {
        schema: schema.to_string(),
        schema_type: "PROTOBUF".to_string(),
    });

    // Add basic auth if present
    if let Some(auth) = auth_info {
        let parts: Vec<&str> = auth.splitn(2, ':').collect();
        if parts.len() == 2 {
            request = request.basic_auth(parts[0], Some(parts[1]));
        }
    }

    let response = request.send()?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text()?;
        return Err(format!(
            "Schema registration failed: {} - {}",
            status, body
        )
        .into());
    }

    let register_response: RegisterResponse = response.json()?;
    Ok(register_response.id)
}

fn humanize_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    if bytes == 0 { return "0 B".to_string(); }
    let mut size = bytes as f64;
    let mut unit_idx = 0;
    while size >= 1024.0 && unit_idx < UNITS.len() - 1 {
        size /= 1024.0;
        unit_idx += 1;
    }
    format!("{:.2} {}", size, UNITS[unit_idx])
}

