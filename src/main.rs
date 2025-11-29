use kafka_avro_utility::avrogen::generate_random_record;
use kafka_avro_utility::fieldspec::FieldSpec;
use kafka_avro_utility::kafka_config::KafkaConfig;

use clap::Parser;

use governor::{Quota, RateLimiter};
use rdkafka::producer::{DeliveryResult, Producer, ProducerContext, ThreadedProducer};
use rdkafka::util::Timeout;
use rdkafka::ClientContext;
use schema_registry_converter::blocking::avro::AvroEncoder;
use schema_registry_converter::blocking::schema_registry::SrSettings;
use schema_registry_converter::schema_registry_common::{
    SchemaType, SubjectNameStrategy, SuppliedSchema,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Parser, Debug)]
#[command(name = "rust-avro-producer")]
#[command(about = "High-throughput Kafka producer for Avro Person records")]
struct Args {
    /// Path to Kafka client configuration file
    #[arg(long, default_value = "client.properties")]
    command_config: String,

    /// Number of producer threads
    #[arg(long, default_value_t = 1)]
    threads: usize,

    /// Destination Kafka topic
    #[arg(long, default_value = "test-avro")]
    topic: String,

    /// Number of messages to send (-1 for unlimited)
    #[arg(long = "num-messages", default_value_t = -1)]
    num_messages: i64,

    /// Print the Avro schema and exit
    #[arg(long)]
    print_schema: bool,

    /// Schema Registry URL (optional, overrides config file)
    #[arg(long)]
    schema_registry_url: Option<String>,

    /// Path to Schema Registry configuration file
    #[arg(long, default_value = "sr.properties")]
    schema_registry_config: String,

    /// Path to Avro schema file (.avsc)
    #[arg(long, default_value = "schemas/person.avsc")]
    schema_file: String,

    /// Rate limit in messages per second
    #[arg(long, default_value_t = 500)]
    rate: u32,

    /// Path to field specification file
    #[arg(long, default_value = "field_spec.txt")]
    field_spec: String,
}

struct Stats {
    messages: AtomicU64,
    bytes: AtomicU64,
    start_time: Instant,
}

// Producer context for callback-based producer
struct StatsProducerContext {
    stats: Arc<Stats>,
}

impl ClientContext for StatsProducerContext {}

impl ProducerContext for StatsProducerContext {
    type DeliveryOpaque = usize; // Store buffer size for stats

    fn delivery(&self, result: &DeliveryResult<'_>, buffer_size: Self::DeliveryOpaque) {
        match result {
            Ok(_) => {
                // Message delivered successfully - stats already updated on send
                // The callback is just for error reporting
            }
            Err(e) => {
                eprintln!("Error delivering message: {}", e.0);
                // Decrement stats on delivery failure (we incremented on send)
                self.stats.messages.fetch_sub(1, Ordering::Relaxed);
                self.stats
                    .bytes
                    .fetch_sub(buffer_size as u64, Ordering::Relaxed);
            }
        }
    }
}

impl Stats {
    fn new() -> Self {
        Self {
            messages: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            start_time: Instant::now(),
        }
    }

    fn get_stats(&self) -> (u64, u64, f64, f64) {
        let messages = self.messages.load(Ordering::Relaxed);
        let bytes = self.bytes.load(Ordering::Relaxed);
        let elapsed = self.start_time.elapsed().as_secs_f64();
        let msg_per_sec = if elapsed > 0.0 {
            messages as f64 / elapsed
        } else {
            0.0
        };
        let bytes_per_sec = if elapsed > 0.0 {
            bytes as f64 / elapsed
        } else {
            0.0
        };
        (messages, bytes, msg_per_sec, bytes_per_sec)
    }
}

fn producer_worker(
    rate_limiter: Arc<
        RateLimiter<
            governor::state::direct::NotKeyed,
            governor::state::InMemoryState,
            governor::clock::QuantaClock,
        >,
    >,
    producer: ThreadedProducer<StatsProducerContext>,
    topic: String,
    stats: Arc<Stats>,
    schema: Arc<apache_avro::Schema>,
    supplied_schema: Arc<SuppliedSchema>,
    sr_settings: Arc<SrSettings>,
    field_spec: Arc<FieldSpec>,
    num_messages: u64,
    worker_id: usize,
) {
    let _ = rate_limiter;
    println!("Starting producer worker {}", worker_id);
    let mut message_count = 0u64;

    let record_schema = if let apache_avro::Schema::Record(ref inner) = *schema {
        inner
    } else {
        eprintln!("Error: Top-level schema must be a Record type");
        return;
    };

    // Create a Tokio runtime for this thread to handle async encoding
    let encoder = AvroEncoder::new(sr_settings.as_ref().clone());
    let subject_name_strategy = SubjectNameStrategy::TopicNameStrategyWithSchema(
        topic.clone(),
        false, // false = value, true = key
        supplied_schema.as_ref().clone(),
    );

    // Poll periodically instead of after every send for better performance
    // Poll every 100 messages or every 10ms, whichever comes first
    const POLL_INTERVAL_MESSAGES: u64 = 10000000;
    const POLL_INTERVAL_MS: u64 = 100;
    let mut last_poll_time = Instant::now();

    loop {
        // Wait for rate limiter to allow the next operation
        // This blocks until the rate limit permits proceeding
        futures::executor::block_on(rate_limiter.until_ready());
        // Always increment message counter for stats
        // Then check if we've exceeded the limit (if limit is set)
        let current = stats.messages.fetch_add(1, Ordering::Relaxed);

        // Check if we've exceeded the message limit
        if num_messages > 0 && current >= num_messages {
            // We exceeded the limit, decrement back and exit
            stats.messages.fetch_sub(1, Ordering::Relaxed);
            break;
        }

        // Generate a random record based on the schema
        let avro_value = match generate_random_record(record_schema, &field_spec) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("Error creating Avro record: {}", e);
                // Release the message slot we claimed
                stats.messages.fetch_sub(1, Ordering::Relaxed);
                break;
            }
        };

        // Use Confluent Schema Registry encoder to encode with wire format
        // The encoder will automatically register the schema if needed
        // Create encoder per message (it's cheap, just holds settings)
        // Note: We'll create it per message since we need async runtime handle

        // Use TopicNameStrategyWithSchema to provide the schema string so it can be registered
        // Use the pre-parsed SuppliedSchema

        // Create encoder for this message and encode synchronously (blocking)

        // Convert Value::Record to Vec<(&str, Value)> for the blocking encoder
        // Note: This conversion is required because the blocking AvroEncoder::encode() method
        // only accepts Vec<(&str, Value)>, not Value::Record directly. Unfortunately, this
        // means we need to extract and clone the fields, even though encode() will reconstruct
        // the Record internally. This is a limitation of the schema_registry_converter API design.
        let record_fields = match &avro_value {
            apache_avro::types::Value::Record(fields) => fields
                .iter()
                .map(|(k, v)| (k.as_str(), v.clone()))
                .collect(),
            _ => {
                eprintln!("Error: Expected Value::Record, got {:?}", avro_value);
                // Release the message slot we claimed
                stats.messages.fetch_sub(1, Ordering::Relaxed);
                break;
            }
        };

        let buffer = match encoder.encode(record_fields, &subject_name_strategy) {
            Ok(data) => data,
            Err(e) => {
                // Only print error once per worker to avoid spam
                if message_count == 0 {
                    eprintln!(
                        "Error encoding with Schema Registry: {} (subject: {}-value)",
                        e, topic
                    );
                    eprintln!("This might be due to:");
                    eprintln!("  1. Authentication issues - check credentials in sr.properties");
                    eprintln!(
                        "  2. Schema registration failure - Schema Registry may not be accessible"
                    );
                    eprintln!("  3. Network/connection issues - check Schema Registry URL");
                    eprintln!("  4. Invalid schema format or subject name");
                    eprintln!("  5. Schema Registry returned response without ID field");
                }
                // Release the message slot we claimed
                stats.messages.fetch_sub(1, Ordering::Relaxed);
                break;
            }
        };

        let buffer_size = buffer.len();

        // Send to Kafka (key is null) - non-blocking with callback
        // The callback will handle stats updates
        // Create record with opaque data (buffer_size) using with_opaque_to
        // Key type is () for null key
        let record: rdkafka::producer::BaseRecord<(), Vec<u8>, usize> =
            rdkafka::producer::BaseRecord::with_opaque_to(&topic, buffer_size).payload(&buffer);

        // Send is non-blocking - callback will be called when delivery completes
        producer.send(record).expect("Failed to queue message");
        // Update bytes stat (messages already incremented atomically above)
        stats.bytes.fetch_add(buffer_size as u64, Ordering::Relaxed);

        message_count += 1;

        // Poll periodically to process delivery callbacks
        // This is necessary for ThreadedProducer to invoke delivery callbacks
        // We poll every N messages or every N ms to balance performance and responsiveness
        let should_poll = message_count % POLL_INTERVAL_MESSAGES == 0
            || last_poll_time.elapsed().as_millis() >= POLL_INTERVAL_MS as u128;

        if should_poll {
            producer.poll(Timeout::After(Duration::from_secs(0)));
            last_poll_time = Instant::now();
        }
    }

    // Flush remaining messages - flush() will handle polling internally
    // This ensures all messages are sent and all delivery callbacks are processed
    loop {
        let result = producer.flush(Timeout::After(Duration::from_secs(10)));
        if result.is_ok() {
            break;
        }
        thread::sleep(Duration::from_secs(1));
    }
}

fn stats_reporter(stats: Arc<Stats>, shutdown: mpsc::Receiver<()>) {
    let shutdown_rx = shutdown;

    loop {
        if let Ok(_) = shutdown_rx.try_recv() {
            break;
        }
        thread::sleep(Duration::from_secs(1));
        let (messages, bytes, msg_per_sec, bytes_per_sec) = stats.get_stats();
        println!(
            "Stats: {} messages, {} bytes, {:.2} msg/s, {:.2} bytes/s",
            messages, bytes, msg_per_sec, bytes_per_sec
        );
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Load Avro schema from file
    let schema_str = std::fs::read_to_string(&args.schema_file)?;

    // If --print-schema flag is set, print the schema and exit
    if args.print_schema {
        println!("{}", schema_str);
        return Ok(());
    }

    // Load Kafka configuration
    let kafka_config = KafkaConfig::from_file(&args.command_config)?;

    // Load Schema Registry configuration
    let sr_config = KafkaConfig::from_file(&args.schema_registry_config)?;

    // Get Schema Registry URL - prefer command line arg, then Schema Registry config file
    let schema_registry_url = args
        .schema_registry_url
        .or_else(|| sr_config.get_schema_registry_url().cloned())
        .ok_or("Schema Registry URL is required. Provide via --schema-registry-url or schema.registry.url in Schema Registry config file")?;

    let schema = apache_avro::Schema::parse_str(&schema_str)?;
    let schema = Arc::new(schema);

    // Pre-parse SuppliedSchema once to avoid recreating it in the loop
    let supplied_schema = Arc::new(SuppliedSchema {
        name: None, // Not needed for TopicNameStrategy
        schema_type: SchemaType::Avro,
        schema: schema_str, // Use the parsed schema string
        properties: None,
        references: vec![],
        tags: None,
    });

    // Create Schema Registry settings - try from Schema Registry config file first, fallback to URL only
    let sr_settings = if let Ok(settings) = sr_config.create_schema_registry_settings() {
        Arc::new(settings)
    } else {
        // Fallback: create from URL only (command line arg takes precedence)
        Arc::new(SrSettings::new(schema_registry_url))
    };

    // Load Field Spec
    let field_spec = Arc::new(FieldSpec::load(&args.field_spec)?);

    // Create shared statistics
    let stats = Arc::new(Stats::new());
    let stats_clone = stats.clone();
    // Create producers for each thread
    let mut handles = Vec::new();
    let (shutdown_tx, shutdown_rx) = mpsc::channel();

    // Start stats reporter
    let stats_reporter_handle = thread::spawn(move || stats_reporter(stats_clone, shutdown_rx));

    // Convert message limit to u64 (0 means unlimited)
    let num_messages = if args.num_messages > 0 {
        args.num_messages as u64
    } else {
        0 // 0 means unlimited
    };

    // Spawn producer workers
    for i in 0..args.threads {
        let context = StatsProducerContext {
            stats: stats.clone(),
        };
        let producer = kafka_config.create_producer(context)?;
        let topic = args.topic.clone();
        let stats_clone = stats.clone();
        let schema_clone = schema.clone();
        let supplied_schema_clone = supplied_schema.clone();
        let sr_settings_clone = sr_settings.clone();

        let field_spec_clone = field_spec.clone();

        let rate_limiter = Arc::new(RateLimiter::direct(Quota::per_second(
            std::num::NonZeroU32::new(args.rate).unwrap(),
        )));
        // Spawn worker in a separate thread since producer_worker is sync
        let handle = std::thread::spawn(move || {
            producer_worker(
                rate_limiter.clone(),
                producer,
                topic,
                stats_clone,
                schema_clone,
                supplied_schema_clone,
                sr_settings_clone,
                field_spec_clone,
                num_messages,
                i,
            )
        });
        handles.push(handle);
    }

    // Wait for all producers to finish
    for handle in handles {
        handle
            .join()
            .map_err(|e| format!("Thread panicked: {:?}", e))?;
    }

    // Stop stats reporter
    let _ = shutdown_tx.send(());

    // Print final stats
    let (messages, bytes, msg_per_sec, bytes_per_sec) = stats.get_stats();
    println!("\nFinal Stats:");
    println!("  Total messages: {}", messages);
    println!("  Total bytes: {}", bytes);
    println!("  Average messages/sec: {:.2}", msg_per_sec);
    println!("  Average bytes/sec: {:.2}", bytes_per_sec);

    // Wait a bit for stats reporter to finish
    thread::sleep(Duration::from_millis(100));
    let _ = stats_reporter_handle.join();

    Ok(())
}
