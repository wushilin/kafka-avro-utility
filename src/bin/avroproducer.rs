use clap::Parser;
use precise_rate_limiter::FastQuotaSync;
use rdkafka::producer::{BaseRecord, Producer, ThreadedProducer};
use rdkafka::util::Timeout;
use kafka_avro_utility::avrogen::generate_random_record;
use kafka_avro_utility::fastavrofile::load_bytes_file;
use kafka_avro_utility::kafka_config::KafkaConfig;
use kafka_avro_utility::fieldspec::FieldSpec;
use apache_avro as avro;
use std::sync::Arc;
use std::fs;
use std::path::PathBuf;
use std::time::{Duration, Instant};

#[derive(Parser, Debug)]
#[command(name = "kafkaproducer")]
#[command(about = "Produce Avro records from a file to Kafka")]
struct Args {
    /// Path to Kafka client configuration file
    #[arg(long, default_value = "client.properties")]
    client_config: String,

    /// Path to Schema Registry configuration file
    #[arg(long, default_value = "sr.properties")]
    sr_config: String,

    /// Input Avro file path (mutually exclusive with --schema)
    #[arg(long)]
    in_file: Option<PathBuf>,

    /// Avro schema file path for on-the-fly generation (mutually exclusive with --in-file)
    #[arg(long)]
    schema: Option<PathBuf>,

    /// Field specification file (used with --schema)
    #[arg(long, default_value = "field_spec.txt")]
    field_spec: PathBuf,

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
                // Message delivery failed
                eprintln!("Delivery error: {:?}", err);
                self.error_count
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Enforce mutually exclusive input options
    match (&args.in_file, &args.schema) {
        (Some(_), None) | (None, Some(_)) => {}
        (None, None) => {
            eprintln!("Error: either --in-file or --schema must be provided");
            std::process::exit(1);
        }
        (Some(_), Some(_)) => {
            eprintln!("Error: --in-file and --schema are mutually exclusive");
            std::process::exit(1);
        }
    }

    // Load Kafka configuration and create producer with custom context
    let kafka_config = KafkaConfig::from_file(&args.client_config)?;

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

    let subject = format!("{}-value", args.topic);

    let mut count = 0u64;
    let mut total_bytes = 0u64;
    let start_time = std::time::Instant::now();

    // Create rate limiter if speed limit is specified
    let rate_limiter: Option<Arc<FastQuotaSync>> = args.speed.map(|speed_mib| {
        let bytes_per_sec = (speed_mib * 1024.0 * 1024.0) as usize;
        println!(
            "Rate limiting enabled: {:.2} MiB/s ({} bytes/s)",
            speed_mib, bytes_per_sec
        );
        // Use 100ms refill interval for smooth rate limiting
        let refill_interval = Duration::from_millis(100);
        let refill_amount = bytes_per_sec / 10; // 10 refills per second
        FastQuotaSync::new(bytes_per_sec, refill_amount, refill_interval)
    });

    if let Some(in_file) = &args.in_file {
        run_file_mode(
            in_file,
            &args,
            &producer,
            &subject,
            &sr_url,
            auth_info.as_deref(),
            rate_limiter.as_ref(),
            &mut count,
            &mut total_bytes,
            start_time,
        )?;
    } else {
        run_generate_mode(
            &args,
            &producer,
            &subject,
            &sr_url,
            auth_info.as_deref(),
            rate_limiter.as_ref(),
            &mut count,
            &mut total_bytes,
            start_time,
        )?;
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

/// Register schema with Schema Registry and return the schema ID
fn register_schema(
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

    let request_body = RegisterRequest {
        schema: schema.to_string(),
        schema_type: "AVRO".to_string(),
    };

    // Create HTTP client with SSL verification disabled
    let client = Client::builder()
        .danger_accept_invalid_certs(true)
        .build()?;

    let mut request = client.post(&url).json(&request_body);

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
        return Err(format!("Schema registration failed: {} - {}", status, body).into());
    }

    let register_response: RegisterResponse = response.json()?;
    Ok(register_response.id)
}

fn run_file_mode(
    in_file: &PathBuf,
    args: &Args,
    producer: &ThreadedProducer<ProducerContext>,
    subject: &str,
    sr_url: &str,
    auth_info: Option<&str>,
    rate_limiter: Option<&Arc<FastQuotaSync>>,
    count: &mut u64,
    total_bytes: &mut u64,
    start_time: std::time::Instant,
) -> Result<(), Box<dyn std::error::Error>> {
    let (schema_str, mut iterator) = load_bytes_file(in_file)?;

    // Register schema with Schema Registry and get schema ID
    let schema_id = register_schema(sr_url, auth_info, subject, &schema_str)?;

    println!(
        "Registered schema with ID: {} for subject: {}",
        schema_id, subject
    );

    // Determine if we have a message limit
    let message_limit = args.num_messages;

    loop {
        if let Some(limit) = message_limit {
            if *count >= limit {
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
                    let (_, new_iterator) = load_bytes_file(in_file)?;
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
            Ok(mut raw_bytes) => {
                // Clear and rebuild payload with Confluent wire format
                raw_bytes.set_schema_id(schema_id);

                let bytes_raw = raw_bytes.take();
                let payload_size = bytes_raw.len() as u64;

                // Apply rate limiting if enabled
                if let Some(limiter) = rate_limiter {
                    limiter.acquire(bytes_raw.len());
                }

                produce_payload(
                    producer,
                    &args.topic,
                    bytes_raw,
                    payload_size,
                    count,
                    total_bytes,
                    start_time,
                )?;
            }
            Err(e) => {
                eprintln!("Error reading record from file: {}", e);
                break;
            }
        }
    }

    Ok(())
}

fn run_generate_mode(
    args: &Args,
    producer: &ThreadedProducer<ProducerContext>,
    subject: &str,
    sr_url: &str,
    auth_info: Option<&str>,
    rate_limiter: Option<&Arc<FastQuotaSync>>,
    count: &mut u64,
    total_bytes: &mut u64,
    start_time: std::time::Instant,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema_path = args
        .schema
        .as_ref()
        .expect("schema path required in generate mode");
    let schema_str = std::fs::read_to_string(schema_path)?;
    let schema = avro::Schema::parse_str(&schema_str)?;
    let record_schema = if let avro::Schema::Record(ref inner) = schema {
        inner
    } else {
        return Err("Schema must be a Record".into());
    };

    let mut field_spec = FieldSpec::load(&args.field_spec)?;
    let mut last_mtime = fs::metadata(&args.field_spec)?.modified()?;
    let mut next_check = Instant::now() + Duration::from_secs(1);
    let mut sample_after_reload = false;

    let schema_id = register_schema(sr_url, auth_info, subject, &schema_str)?;
    println!(
        "Registered schema with ID: {} for subject: {} (generate mode)",
        schema_id, subject
    );

    let to_generate = args.num_messages.unwrap_or(100);

    for _ in 0..to_generate {
        // Periodically check for field_spec updates (at most once per second)
        if Instant::now() >= next_check {
            if let Ok(md) = fs::metadata(&args.field_spec) {
                if let Ok(mt) = md.modified() {
                    if mt > last_mtime {
                        match FieldSpec::load(&args.field_spec) {
                            Ok(new_spec) => {
                                field_spec = new_spec;
                                last_mtime = mt;
                                println!(
                                    "Reloaded field_spec from {:?}",
                                    &args.field_spec
                                );
                                sample_after_reload = true;
                            }
                            Err(e) => {
                                eprintln!(
                                    "Warning: failed to reload field_spec: {}",
                                    e
                                );
                            }
                        }
                    }
                }
            }
            next_check = Instant::now() + Duration::from_secs(1);
        }

        let record = generate_random_record(record_schema, &field_spec)?;
        if sample_after_reload {
            println!("Sample record after reload: {:?}", record);
            sample_after_reload = false;
        }
        let avro_bytes = avro::to_avro_datum(&schema, record)?;

        let mut payload = Vec::with_capacity(5 + avro_bytes.len());
        payload.push(0);
        payload.extend_from_slice(&schema_id.to_be_bytes());
        payload.extend_from_slice(&avro_bytes);

        let payload_size = payload.len() as u64;

        if let Some(limiter) = rate_limiter {
            limiter.acquire(payload.len());
        }

        produce_payload(
            producer,
            &args.topic,
            payload,
            payload_size,
            count,
            total_bytes,
            start_time,
        )?;
    }

    Ok(())
}

fn produce_payload(
    producer: &ThreadedProducer<ProducerContext>,
    topic: &str,
    bytes_raw: Vec<u8>,
    payload_size: u64,
    count: &mut u64,
    total_bytes: &mut u64,
    start_time: std::time::Instant,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut record: BaseRecord<(), Vec<u8>, ()> = BaseRecord::to(topic).payload(&bytes_raw);

    // Retry loop for handling QueueFull errors with timeout
    let retry_start = std::time::Instant::now();
    let max_retry_duration = Duration::from_secs(5);
    let mut retry_count = 0;
    const MAX_RETRIES: u32 = 100;

    loop {
        match producer.send(record) {
            Ok(_) => {
                *count += 1;
                *total_bytes += payload_size;

                if *count % 10000 == 0 {
                    let elapsed = start_time.elapsed().as_secs_f64();
                    let msg_rate = *count as f64 / elapsed;
                    let byte_rate = *total_bytes as f64 / elapsed;

                    println!(
                        "Produced {} messages, {} total ({:.2} msg/s, {}/s)",
                        *count,
                        humanize_bytes(*total_bytes),
                        msg_rate,
                        humanize_bytes(byte_rate as u64)
                    );
                }
                // Poll more frequently to prevent queue buildup
                if *count % 1000 == 0 {
                    producer.poll(Timeout::After(Duration::from_millis(0)));
                }
                break;
            }
            Err((
                rdkafka::error::KafkaError::MessageProduction(
                    rdkafka::types::RDKafkaErrorCode::QueueFull,
                ),
                returned_record,
            )) => {
                retry_count += 1;

                // Check if we've exceeded retry limits
                if retry_count > MAX_RETRIES || retry_start.elapsed() > max_retry_duration {
                    eprintln!(
                        "Error: Queue full after {} retries and {:.2}s. Topic may not exist or broker is unavailable.",
                        retry_count,
                        retry_start.elapsed().as_secs_f64()
                    );
                    return Err(
                        "Producer queue full - check if topic exists and broker is reachable"
                            .into(),
                    );
                }

                // Queue is full, poll and retry
                producer.poll(Timeout::After(Duration::from_millis(10)));
                record = returned_record;
                continue;
            }
            Err((e, _)) => {
                eprintln!("Error producing message: {:?}", e);
                break;
            }
        }
    }

    Ok(())
}
