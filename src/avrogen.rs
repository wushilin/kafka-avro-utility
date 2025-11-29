use crate::fieldspec::{FieldGenerator, FieldSpec};
use apache_avro::types::Value;
use std::str::FromStr;
use uuid::Uuid;

/// Generate a random Avro record based on the schema
pub fn generate_random_record(
    record_schema: &apache_avro::schema::RecordSchema,
    field_spec: &FieldSpec,
) -> Result<apache_avro::types::Value, Box<dyn std::error::Error>> {
    let mut record = Vec::new();

    for field in &record_schema.fields {
        let field_name = &field.name;
        let field_type = &field.schema;

        // Special handling for container types to ensure recursion works correctly
        // If the field has a template, it's used for primitive generation.
        // For arrays/maps, we pass the field name down so items can use the template.
        let value = generate_value_for_type(field_type, field_name, field_spec)?;
        record.push((field_name.clone(), value));
    }
    Ok(Value::Record(record))
}

pub fn generate_value_for_type(
    schema: &apache_avro::Schema,
    field_name: &str,
    field_spec: &FieldSpec,
) -> Result<apache_avro::types::Value, Box<dyn std::error::Error>> {
    match schema {
        apache_avro::Schema::Record(inner_schema) => {
            generate_random_record(inner_schema, field_spec)
        }
        apache_avro::Schema::Array(array_schema) => {
            // Generate 1-5 items
            // We pass the same field_name so items use the template if defined
            let count = rand::random_range(1..5);
            let mut items = Vec::with_capacity(count);
            for _ in 0..count {
                items.push(generate_value_for_type(
                    &array_schema.items,
                    field_name,
                    field_spec,
                )?);
            }
            Ok(Value::Array(items))
        }
        apache_avro::Schema::Map(map_schema) => {
            // Generate 1-5 items
            let count = rand::random_range(1..5);
            let mut map = std::collections::HashMap::new();
            for i in 0..count {
                let key = format!("key_{}", i);
                let val = generate_value_for_type(&map_schema.types, field_name, field_spec)?;
                map.insert(key, val);
            }
            Ok(Value::Map(map))
        }
        _ => {
            // Primitive or other types: check template
            if let Some(FieldGenerator::Template(template)) = field_spec.get_generator(field_name) {
                // Implicit 10% null probability for Union types containing Null
                if let apache_avro::Schema::Union(u) = schema {
                    let has_null = u.variants().iter().any(|s| matches!(s, apache_avro::Schema::Null));
                    if has_null && rand::random_bool(0.1) {
                        if let Some(idx) = u.variants().iter().position(|s| matches!(s, apache_avro::Schema::Null)) {
                            return Ok(Value::Union(idx as u32, Box::new(Value::Null)));
                        }
                    }
                }

                let val_str = FieldSpec::generate_from_template(template);
                match parse_value(schema, &val_str) {
                    Ok(val) => Ok(val),
                    Err(e) => {
                        eprintln!(
                            "Warning: Failed to parse '{}' for field {}: {}. Using random default.",
                            val_str, field_name, e
                        );
                        generate_random_value(schema)
                    }
                }
            } else {
                generate_random_value(schema)
            }
        }
    }
}

fn parse_value(schema: &apache_avro::Schema, val_str: &str) -> Result<Value, String> {
    match schema {
        apache_avro::Schema::Null => {
            if val_str == "null" || val_str.is_empty() {
                Ok(Value::Null)
            } else {
                Err(format!("Expected null, got {}", val_str))
            }
        }
        apache_avro::Schema::Boolean => bool::from_str(val_str)
            .map(Value::Boolean)
            .map_err(|e| e.to_string()),
        apache_avro::Schema::Int => i32::from_str(val_str)
            .map(Value::Int)
            .map_err(|e| e.to_string()),
        apache_avro::Schema::Long => i64::from_str(val_str)
            .map(Value::Long)
            .map_err(|e| e.to_string()),
        apache_avro::Schema::Float => f32::from_str(val_str)
            .map(Value::Float)
            .map_err(|e| e.to_string()),
        apache_avro::Schema::Double => f64::from_str(val_str)
            .map(Value::Double)
            .map_err(|e| e.to_string()),
        apache_avro::Schema::Bytes => Ok(Value::Bytes(val_str.as_bytes().to_vec())),
        apache_avro::Schema::String => Ok(Value::String(val_str.to_string())),
        apache_avro::Schema::Fixed(f) => {
            let bytes = val_str.as_bytes().to_vec();
            if bytes.len() == f.size {
                Ok(Value::Fixed(f.size, bytes))
            } else {
                Err(format!(
                    "Fixed size mismatch: expected {}, got {}",
                    f.size,
                    bytes.len()
                ))
            }
        }
        apache_avro::Schema::Enum(e) => {
            if let Some(idx) = e.symbols.iter().position(|s| s == val_str) {
                Ok(Value::Enum(idx as u32, val_str.to_string()))
            } else {
                Err(format!("Invalid symbol '{}' for enum {}", val_str, e.name))
            }
        }
        apache_avro::Schema::Union(u) => {
            if val_str == "null" {
                // Find null variant
                if let Some(idx) = u
                    .variants()
                    .iter()
                    .position(|s| matches!(s, apache_avro::Schema::Null))
                {
                    return Ok(Value::Union(idx as u32, Box::new(Value::Null)));
                }
            }

            // Try to parse as non-null variants
            for (i, variant) in u.variants().iter().enumerate() {
                if matches!(variant, apache_avro::Schema::Null) {
                    continue;
                }
                if let Ok(val) = parse_value(variant, val_str) {
                    return Ok(Value::Union(i as u32, Box::new(val)));
                }
            }
            Err(format!(
                "Could not parse '{}' against any union variant",
                val_str
            ))
        }
        apache_avro::Schema::Uuid => Uuid::parse_str(val_str)
            .map(Value::Uuid)
            .map_err(|e| e.to_string()),
        apache_avro::Schema::Date => i32::from_str(val_str)
            .map(Value::Date)
            .map_err(|e| e.to_string()),
        apache_avro::Schema::TimeMillis => i32::from_str(val_str)
            .map(Value::TimeMillis)
            .map_err(|e| e.to_string()),
        apache_avro::Schema::TimeMicros => i64::from_str(val_str)
            .map(Value::TimeMicros)
            .map_err(|e| e.to_string()),
        apache_avro::Schema::TimestampMillis => i64::from_str(val_str)
            .map(Value::TimestampMillis)
            .map_err(|e| e.to_string()),
        apache_avro::Schema::TimestampMicros => i64::from_str(val_str)
            .map(Value::TimestampMicros)
            .map_err(|e| e.to_string()),
        // For other types or complex structures, basic string parsing might not be enough
        _ => Err(format!(
            "Parsing not supported for schema type: {:?}",
            schema
        )),
    }
}

fn generate_random_value(
    schema: &apache_avro::Schema,
) -> Result<Value, Box<dyn std::error::Error>> {
    match schema {
        apache_avro::Schema::Null => Ok(Value::Null),
        apache_avro::Schema::Boolean => Ok(Value::Boolean(rand::random_bool(0.5))),
        apache_avro::Schema::Int => Ok(Value::Int(rand::random::<i32>())),
        apache_avro::Schema::Long => Ok(Value::Long(rand::random::<i64>())),
        apache_avro::Schema::Float => Ok(Value::Float(rand::random::<f32>())),
        apache_avro::Schema::Double => Ok(Value::Double(rand::random::<f64>())),
        apache_avro::Schema::Bytes => {
            let len = rand::random_range(1..20);
            Ok(Value::Bytes(
                (0..len).map(|_| rand::random::<u8>()).collect(),
            ))
        }
        apache_avro::Schema::String => Ok(Value::String(format!(
            "random_{}",
            rand::random_range(0..1000)
        ))),
        apache_avro::Schema::Fixed(f) => Ok(Value::Fixed(
            f.size,
            (0..f.size).map(|_| rand::random::<u8>()).collect(),
        )),
        apache_avro::Schema::Enum(e) => {
            let idx = rand::random_range(0..e.symbols.len());
            Ok(Value::Enum(idx as u32, e.symbols[idx].clone()))
        }
        apache_avro::Schema::Union(u) => {
            let variants = u.variants();
            // Bias towards non-null
            let non_null_indices: Vec<usize> = variants
                .iter()
                .enumerate()
                .filter(|(_, s)| !matches!(s, apache_avro::Schema::Null))
                .map(|(i, _)| i)
                .collect();

            if !non_null_indices.is_empty() && rand::random_bool(0.9) {
                let idx = non_null_indices[rand::random_range(0..non_null_indices.len())];
                let val = generate_random_value(&variants[idx])?;
                Ok(Value::Union(idx as u32, Box::new(val)))
            } else if let Some(idx) = variants
                .iter()
                .position(|s| matches!(s, apache_avro::Schema::Null))
            {
                Ok(Value::Union(idx as u32, Box::new(Value::Null)))
            } else {
                // No null, pick random
                let idx = rand::random_range(0..variants.len());
                let val = generate_random_value(&variants[idx])?;
                Ok(Value::Union(idx as u32, Box::new(val)))
            }
        }
        apache_avro::Schema::Uuid => Ok(Value::Uuid(Uuid::new_v4())),
        apache_avro::Schema::Date => Ok(Value::Date(rand::random_range(0..20000))),
        apache_avro::Schema::TimeMillis => Ok(Value::TimeMillis(rand::random_range(0..86400000))),
        apache_avro::Schema::TimeMicros => Ok(Value::TimeMicros(rand::random_range(0..86400000000))),
        apache_avro::Schema::TimestampMillis => Ok(Value::TimestampMillis(rand::random_range(
            1600000000000..1700000000000,
        ))),
        apache_avro::Schema::TimestampMicros => Ok(Value::TimestampMicros(rand::random_range(
            1600000000000000..1700000000000000,
        ))),
        apache_avro::Schema::Duration => Ok(Value::Duration(apache_avro::Duration::new(
            apache_avro::Months::new(1),
            apache_avro::Days::new(1),
            apache_avro::Millis::new(1000),
        ))),
        _ => Err(format!("Random generation not supported for {:?}", schema).into()),
    }
}
