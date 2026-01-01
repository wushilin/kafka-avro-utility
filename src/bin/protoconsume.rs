use clap::Parser;
use kafka_avro_utility::kafka_config::KafkaConfig;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor};
use protox::compile;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::base_consumer::BaseConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::message::Message as KafkaMessage;
use reqwest::blocking::Client;
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;
use uuid::Uuid;

#[derive(Parser, Debug)]
#[command(name = "protoconsume")]
#[command(about = "Consume Protobuf messages from Kafka and pretty print them")]
struct Args {
    /// Kafka client configuration file
    #[arg(long)]
    client_config: PathBuf,

    /// Schema Registry configuration file
    #[arg(long)]
    sr_config: PathBuf,

    /// Kafka topic to consume
    #[arg(long)]
    topic: String,

    /// Consumer group id
    #[arg(long)]
    group: String,

    /// Optional preferred message type (fully qualified)
    #[arg(long)]
    message: Option<String>,

    /// Number of messages to consume (default unlimited)
    #[arg(long)]
    num_messages: Option<u64>,
}

#[derive(Deserialize)]
struct SchemaResponse {
    schema: String,
    #[serde(rename = "schemaType")]
    schema_type: Option<String>,
    #[serde(default)]
    references: Vec<SchemaReference>,
}

#[derive(Deserialize, Clone)]
#[allow(dead_code)]
struct SchemaReference {
    name: String,
    subject: String,
    version: i32,
}

struct SchemaEntry {
    _pool: DescriptorPool,
    message_descriptor: MessageDescriptor,
    message_name: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let consumer = create_consumer(&args)?;
    consumer.subscribe(&[&args.topic])?;

    let sr_config = KafkaConfig::from_file(&args.sr_config)?;
    let sr_url = sr_config
        .get_schema_registry_url()
        .cloned()
        .ok_or("schema.registry.url is required in sr-config")?;
    let auth_info = sr_config
        .get("schema.registry.basic.auth.user.info")
        .cloned();

    let mut schema_cache: HashMap<u32, SchemaEntry> = HashMap::new();
    let mut processed = 0u64;

    loop {
        if let Some(limit) = args.num_messages {
            if processed >= limit {
                break;
            }
        }

        match consumer.poll(Duration::from_millis(200)) {
            Some(Ok(msg)) => {
                if let Some(payload) = msg.payload() {
                    match handle_message(
                        payload,
                        &mut schema_cache,
                        &sr_url,
                        auth_info.as_deref(),
                        args.message.as_deref(),
                    ) {
                        Ok(()) => {
                            processed += 1;
                        }
                        Err(err) => {
                            eprintln!("Failed to process message: {}", err);
                        }
                    }
                }
            }
            Some(Err(err)) => eprintln!("Kafka error: {}", err),
            None => {}
        }
    }

    println!("Consumed {} messages.", processed);
    Ok(())
}

fn create_consumer(args: &Args) -> Result<BaseConsumer, Box<dyn std::error::Error>> {
    let client_config_content = fs::read_to_string(&args.client_config)?;
    let mut props = java_properties::read(client_config_content.as_bytes())?;
    props.insert("group.id".to_string(), args.group.clone());

    if !props.contains_key("bootstrap.servers") {
        return Err("bootstrap.servers is required in client config".into());
    }

    let mut client_config = ClientConfig::new();

    if let Some(jaas_config) = props.get("sasl.jaas.config") {
        if let Some((username, password)) = parse_jaas_config_local(jaas_config) {
            client_config.set("sasl.username", &username);
            client_config.set("sasl.password", &password);
        }
    }

    for (key, value) in &props {
        if key.contains("serializer") || key.contains("deserializer") {
            continue;
        }
        if key == "sasl.jaas.config" {
            continue;
        }
        let mapped = map_property_name_local(key);
        client_config.set(mapped.as_str(), value);
    }

    Ok(client_config.create()?)
}

fn parse_jaas_config_local(input: &str) -> Option<(String, String)> {
    fn extract(input: &str, key: &str) -> Option<String> {
        let pattern = format!("{}=", key);
        let start = input.find(&pattern)? + pattern.len();
        let rest = &input[start..];
        if rest.starts_with('"') {
            let end = rest[1..].find('"')?;
            Some(rest[1..end + 1].to_string())
        } else if rest.starts_with('\'') {
            let end = rest[1..].find('\'')?;
            Some(rest[1..end + 1].to_string())
        } else {
            let end = rest.find(' ').or_else(|| rest.find(';')).unwrap_or(rest.len());
            Some(rest[..end].to_string())
        }
    }

    let username = extract(input, "username")?;
    let password = extract(input, "password")?;
    Some((username, password))
}

fn map_property_name_local(java_key: &str) -> String {
    match java_key {
        "sasl.mechanism" => "sasl.mechanisms".to_string(),
        _ => java_key.to_string(),
    }
}

fn handle_message(
    payload: &[u8],
    cache: &mut HashMap<u32, SchemaEntry>,
    sr_url: &str,
    auth_info: Option<&str>,
    preferred_message: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    if payload.len() < 5 {
        return Err("message payload too short".into());
    }
    if payload[0] != 0 {
        return Err("unsupported payload format (missing magic byte)".into());
    }

    let schema_id = u32::from_be_bytes(payload[1..5].try_into().unwrap());
    let data = &payload[5..];

    if !cache.contains_key(&schema_id) {
        let schema_resp = fetch_schema_by_id(sr_url, auth_info, schema_id)?;
        if schema_resp.schema_type.as_deref() != Some("PROTOBUF") {
            return Err(format!(
                "Unsupported schema type {:?} for schema id {}",
                schema_resp.schema_type, schema_id
            )
            .into());
        }
        let entry = build_schema_entry(&schema_resp, preferred_message)?;
        cache.insert(schema_id, entry);
    }

    let entry = cache.get(&schema_id).unwrap();
    let mut cursor = data;
    let message =
        DynamicMessage::decode(entry.message_descriptor.clone(), &mut cursor)?;

    println!("Schema {} (message {}):", schema_id, entry.message_name);
    let json_value = serde_json::to_value(&message)?;
    print_json_fields(&json_value, 1);
    Ok(())
}

fn fetch_schema_by_id(
    base_url: &str,
    auth_info: Option<&str>,
    schema_id: u32,
) -> Result<SchemaResponse, Box<dyn std::error::Error>> {
    let url = format!("{}/schemas/ids/{}", base_url, schema_id);
    let client = Client::builder()
        .danger_accept_invalid_certs(true)
        .build()?;

    let mut request = client.get(url);
    if let Some(auth) = auth_info {
        let parts: Vec<&str> = auth.splitn(2, ':').collect();
        if parts.len() == 2 {
            request = request.basic_auth(parts[0], Some(parts[1]));
        }
    }

    let response = request.send()?;
    if !response.status().is_success() {
        return Err(format!(
            "Failed to fetch schema id {}: {}",
            schema_id,
            response.status()
        )
        .into());
    }

    Ok(response.json()?)
}

fn build_schema_entry(
    schema_resp: &SchemaResponse,
    preferred_message: Option<&str>,
) -> Result<SchemaEntry, Box<dyn std::error::Error>> {
    if !schema_resp.references.is_empty() {
        return Err("Schema references are not supported yet".into());
    }

    let temp_dir = std::env::temp_dir().join(format!("proto_schema_{}", Uuid::new_v4()));
    fs::create_dir_all(&temp_dir)?;
    let root_path = temp_dir.join("schema.proto");
    let mut file = fs::File::create(&root_path)?;
    file.write_all(schema_resp.schema.as_bytes())?;

    let includes = vec![temp_dir.clone()];
    let fds = compile(&[&root_path], &includes)?;
    let pool = DescriptorPool::from_file_descriptor_set(fds.clone())?;

    let message_name =
        select_message_name(&fds, "schema.proto", preferred_message)?;
    let message_descriptor = pool
        .get_message_by_name(&message_name)
        .ok_or_else(|| format!("Message '{}' not found in schema", message_name))?;

    let entry = SchemaEntry {
        _pool: pool,
        message_descriptor,
        message_name,
    };
    let _ = fs::remove_dir_all(&temp_dir);
    Ok(entry)
}

fn print_json_fields(value: &serde_json::Value, indent: usize) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, val) in map {
                match val {
                    serde_json::Value::Object(_) | serde_json::Value::Array(_) => {
                        println!("{}{}:", "    ".repeat(indent), key);
                        print_json_fields(val, indent + 1);
                    }
                    _ => {
                        println!(
                            "{}{}: {}",
                            "    ".repeat(indent),
                            key,
                            format_simple_json_value(val)
                        );
                    }
                }
            }
        }
        serde_json::Value::Array(arr) => {
            for (idx, val) in arr.iter().enumerate() {
                match val {
                    serde_json::Value::Object(_) | serde_json::Value::Array(_) => {
                        println!("{}[{}]:", "    ".repeat(indent), idx);
                        print_json_fields(val, indent + 1);
                    }
                    _ => {
                        println!(
                            "{}[{}]: {}",
                            "    ".repeat(indent),
                            idx,
                            format_simple_json_value(val)
                        );
                    }
                }
            }
        }
        _ => println!(
            "{}{}",
            "    ".repeat(indent),
            format_simple_json_value(value)
        ),
    }
}

fn format_simple_json_value(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Null => "null".to_string(),
        _ => value.to_string(),
    }
}

fn select_message_name(
    fds: &prost_types::FileDescriptorSet,
    file_name: &str,
    preferred: Option<&str>,
) -> Result<String, Box<dyn std::error::Error>> {
    if let Some(name) = preferred {
        return Ok(name.to_string());
    }

    let mut messages = Vec::new();
    for file in &fds.file {
        if file.name.as_deref() == Some(file_name) {
            let package = file.package.as_deref().unwrap_or("");
            for msg in &file.message_type {
                let msg_name = msg.name.as_deref().unwrap_or("");
                if msg_name.is_empty() {
                    continue;
                }
                if package.is_empty() {
                    messages.push(msg_name.to_string());
                } else {
                    messages.push(format!("{}.{}", package, msg_name));
                }
            }
        }
    }

    match messages.len() {
        0 => Err("No message types found in schema".into()),
        1 => Ok(messages.remove(0)),
        _ => {
            eprintln!("Schema contains multiple message types:");
            for m in &messages {
                eprintln!("  - {}", m);
            }
            Err("Please provide --message to disambiguate.".into())
        }
    }
}

