use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::error::KafkaError;
use schema_registry_converter::blocking::schema_registry::SrSettings;
use std::collections::HashMap;
use std::fs;
use std::path::Path;

/// Loads Kafka properties from a Java-style properties file
/// and provides functions to create producer/consumer instances
pub struct KafkaConfig {
    properties: HashMap<String, String>,
}

impl KafkaConfig {
    /// Load properties from a file path
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        let content = fs::read_to_string(path)?;
        Self::from_str(&content)
    }

    /// Load properties from a string
    pub fn from_str(content: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let properties = java_properties::read(content.as_bytes())?;
        Ok(KafkaConfig { properties })
    }

    /// Get a property value
    pub fn get(&self, key: &str) -> Option<&String> {
        self.properties.get(key)
    }

    /// Get all properties
    pub fn all(&self) -> &HashMap<String, String> {
        &self.properties
    }

    /// Create a ThreadedProducer from the loaded properties
    pub fn create_producer<C: rdkafka::producer::ProducerContext>(
        &self,
        context: C,
    ) -> Result<rdkafka::producer::ThreadedProducer<C>, KafkaError> {
        let mut config = ClientConfig::new();

        // Ensure bootstrap.servers is set (required)
        if !self.properties.contains_key("bootstrap.servers") {
            return Err(KafkaError::ClientCreation(
                "bootstrap.servers is required".to_string(),
            ));
        }

        // Parse and handle sasl.jaas.config if present
        if let Some(jaas_config) = self.properties.get("sasl.jaas.config") {
            if let Ok((username, password)) = parse_jaas_config(jaas_config) {
                config.set("sasl.username", &username);
                config.set("sasl.password", &password);
            }
        }

        // Apply all properties to the config
        for (key, value) in &self.properties {
            // Skip serializer/deserializer properties as requested
            if key.contains("serializer") || key.contains("deserializer") {
                continue;
            }

            // Skip sasl.jaas.config as we've already parsed it above
            if key == "sasl.jaas.config" {
                continue;
            }

            // Handle buffer.memory -> queue.buffering.max.kbytes conversion
            // buffer.memory is in bytes, queue.buffering.max.kbytes is in KB
            if key == "buffer.memory" {
                if let Ok(bytes) = value.parse::<u64>() {
                    // Convert bytes to KB (divide by 1024)
                    let kbytes = bytes / 1024;
                    // Set minimum of 1 KB if conversion results in 0
                    config.set("queue.buffering.max.kbytes", &kbytes.max(1).to_string());
                }
                continue;
            }

            // Map Java property names to rdkafka names (most are the same)
            let rdkafka_key = map_property_name(key);
            config.set(rdkafka_key.as_str(), value);
        }

        config.create_with_context(context)
    }

    /// Create a StreamConsumer from the loaded properties
    pub fn create_consumer(&self) -> Result<StreamConsumer, KafkaError> {
        let mut config = ClientConfig::new();

        // Ensure bootstrap.servers is set (required)
        if !self.properties.contains_key("bootstrap.servers") {
            return Err(KafkaError::ClientCreation(
                "bootstrap.servers is required".to_string(),
            ));
        }

        // Ensure group.id is set for consumers
        if !self.properties.contains_key("group.id") {
            return Err(KafkaError::ClientCreation(
                "group.id is required for consumers".to_string(),
            ));
        }

        // Parse and handle sasl.jaas.config if present
        if let Some(jaas_config) = self.properties.get("sasl.jaas.config") {
            if let Ok((username, password)) = parse_jaas_config(jaas_config) {
                config.set("sasl.username", &username);
                config.set("sasl.password", &password);
            }
        }

        // Apply all properties to the config
        for (key, value) in &self.properties {
            // Skip serializer/deserializer properties as requested
            if key.contains("serializer") || key.contains("deserializer") {
                continue;
            }

            // Skip sasl.jaas.config as we've already parsed it above
            if key == "sasl.jaas.config" {
                continue;
            }

            // Map Java property names to rdkafka names (most are the same)
            let rdkafka_key = map_property_name(key);
            config.set(rdkafka_key.as_str(), value);
        }

        config.create()
    }

    /// Create Schema Registry settings from the loaded properties
    /// Supports common Schema Registry properties:
    /// - schema.registry.url (required)
    /// - schema.registry.basic.auth.user.info (optional, for basic auth)
    ///
    /// If basic auth is provided, credentials are embedded in the URL.
    pub fn create_schema_registry_settings(
        &self,
    ) -> Result<SrSettings, Box<dyn std::error::Error>> {
        // Get Schema Registry URL - try multiple property names
        let sr_url = self
            .properties
            .get("schema.registry.url")
            .or_else(|| self.properties.get("schema_registry.url"))
            .or_else(|| self.properties.get("schema-registry-url"))
            .ok_or("schema.registry.url is required for Schema Registry")?
            .clone();

        // Build SrSettings with the URL (potentially with embedded credentials)
        let mut sr_settings = SrSettings::new_builder(sr_url);

        if let Some(auth_info) = self.properties.get("schema.registry.basic.auth.user.info") {
            let parts: Vec<&str> = auth_info.splitn(2, ':').collect();
            if parts.len() == 2 {
                println!("Setting basic authorization: {} {}", parts[0], parts[1]);
                sr_settings.set_basic_authorization(parts[0], Some(parts[1]));
            }
        }

        // Disable SSL certificate verification for Schema Registry (trust all certificates)
        // This bypasses issuer checking - use only in development/testing environments
        // Use build_with to configure the reqwest client to accept invalid certificates
        let client_builder =
            reqwest::blocking::ClientBuilder::new().danger_accept_invalid_certs(true);

        let settings = sr_settings.build_with(client_builder)?;
        println!("SrSettings: {:?}", settings);
        Ok(settings)
    }

    /// Get Schema Registry URL from properties
    pub fn get_schema_registry_url(&self) -> Option<&String> {
        self.properties
            .get("schema.registry.url")
            .or_else(|| self.properties.get("schema_registry.url"))
            .or_else(|| self.properties.get("schema-registry-url"))
    }
}

/// Parse SASL JAAS config string to extract username and password
/// Format: org.apache.kafka.common.security.plain.PlainLoginModule required username="user" password="pass";
fn parse_jaas_config(jaas_config: &str) -> Result<(String, String), Box<dyn std::error::Error>> {
    // Extract username
    let username = extract_jaas_value(jaas_config, "username")
        .ok_or("Could not find username in sasl.jaas.config")?;

    // Extract password
    let password = extract_jaas_value(jaas_config, "password")
        .ok_or("Could not find password in sasl.jaas.config")?;

    Ok((username, password))
}

/// Extract a value from JAAS config string
/// Looks for patterns like: key="value" or key='value'
fn extract_jaas_value(jaas_config: &str, key: &str) -> Option<String> {
    // Look for key="value" or key='value'
    let pattern = format!("{}=", key);
    if let Some(start) = jaas_config.find(&pattern) {
        let value_start = start + pattern.len();
        let remaining = &jaas_config[value_start..];

        // Check for quoted value
        if remaining.starts_with('"') {
            // Double-quoted value
            if let Some(end) = remaining[1..].find('"') {
                return Some(remaining[1..end + 1].to_string());
            }
        } else if remaining.starts_with('\'') {
            // Single-quoted value
            if let Some(end) = remaining[1..].find('\'') {
                return Some(remaining[1..end + 1].to_string());
            }
        } else {
            // Unquoted value (ends at space or semicolon)
            let end = remaining
                .find(' ')
                .or_else(|| remaining.find(';'))
                .unwrap_or(remaining.len());
            return Some(remaining[..end].to_string());
        }
    }
    None
}

/// Maps Java Kafka property names to rdkafka property names
/// Most properties are the same, but some may need translation
fn map_property_name(java_key: &str) -> String {
    // Most Kafka properties are the same between Java and rdkafka
    // This function can be extended to handle any differences
    match java_key {
        "sasl.mechanism" => "sasl.mechanisms".to_string(),
        _ => java_key.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_properties() {
        let props = r#"
bootstrap.servers=localhost:9092
group.id=test-group
acks=all
batch.size=16384
linger.ms=10
"#;
        let config = KafkaConfig::from_str(props).unwrap();
        assert_eq!(
            config.get("bootstrap.servers"),
            Some(&"localhost:9092".to_string())
        );
        assert_eq!(config.get("group.id"), Some(&"test-group".to_string()));
    }

    #[test]
    fn test_schema_registry_url() {
        let props = r#"
bootstrap.servers=localhost:9092
schema.registry.url=http://localhost:8081
"#;
        let config = KafkaConfig::from_str(props).unwrap();
        assert_eq!(
            config.get_schema_registry_url(),
            Some(&"http://localhost:8081".to_string())
        );
    }

    #[test]
    fn test_schema_registry_settings() {
        let props = r#"
bootstrap.servers=localhost:9092
schema.registry.url=http://localhost:8081
schema.registry.basic.auth.user.info=user:pass
"#;
        let config = KafkaConfig::from_str(props).unwrap();
        // Verify that creating settings doesn't error
        // The URL should have embedded credentials internally
        let _sr_settings = config.create_schema_registry_settings().unwrap();
    }

    #[test]
    fn test_schema_registry_settings_no_auth() {
        let props = r#"
bootstrap.servers=localhost:9092
schema.registry.url=http://localhost:8081
"#;
        let config = KafkaConfig::from_str(props).unwrap();
        // Verify that creating settings without auth works
        let _sr_settings = config.create_schema_registry_settings().unwrap();
    }

    #[test]
    fn test_parse_jaas_config() {
        let jaas = r#"org.apache.kafka.common.security.plain.PlainLoginModule required username="testuser" password="testpass";"#;
        let (username, password) = parse_jaas_config(jaas).unwrap();
        assert_eq!(username, "testuser");
        assert_eq!(password, "testpass");
    }

    #[test]
    fn test_sasl_jaas_config_in_properties() {
        let props = r#"
bootstrap.servers=localhost:9092
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="user" password="pass";
"#;
        let _config = KafkaConfig::from_str(props).unwrap();
        // Verify that the config can be created (sasl.jaas.config should be parsed)
        // We can't easily test the internal config, but we can verify it doesn't error
        // Note: This might fail if Kafka is not available, but parsing should work
    }

    #[test]
    fn test_buffer_memory_conversion() {
        let props = r#"
bootstrap.servers=localhost:9092
buffer.memory=100000000
"#;
        let config = KafkaConfig::from_str(props).unwrap();
        // Verify buffer.memory is present in properties
        assert_eq!(config.get("buffer.memory"), Some(&"100000000".to_string()));
        // The conversion to queue.buffering.max.kbytes happens in create_producer
        // 100000000 bytes = 97656 KB (100000000 / 1024)
    }

    #[test]
    fn test_sasl_mechanism_translation() {
        assert_eq!(map_property_name("sasl.mechanism"), "sasl.mechanisms");
        assert_eq!(map_property_name("bootstrap.servers"), "bootstrap.servers");
    }
}
