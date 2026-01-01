use crate::fieldspec::{FieldGenerator, FieldSpec};
use prost_reflect::{DynamicMessage, FieldDescriptor, Kind, MessageDescriptor, Value};
use std::str::FromStr;

pub fn generate_random_message(
    descriptor: &MessageDescriptor,
    field_spec: &FieldSpec,
) -> Result<DynamicMessage, Box<dyn std::error::Error>> {
    let mut message = DynamicMessage::new(descriptor.clone());

    for field in descriptor.fields() {
        let value = generate_value_for_field(&field, field_spec)?;
        message.set_field(&field, value);
    }

    Ok(message)
}

fn generate_value_for_field(
    field: &FieldDescriptor,
    field_spec: &FieldSpec,
) -> Result<Value, Box<dyn std::error::Error>> {
    let field_name = field.name();
    let kind = field.kind();

    if field.is_list() {
        // Generate 1-5 items for repeated fields
        let count = rand::random_range(1..5);
        let mut items = Vec::with_capacity(count);
        for _ in 0..count {
            items.push(generate_single_value(&kind, field_name, field_spec)?);
        }
        Ok(Value::List(items))
    } else if field.is_map() {
        // Generate 1-5 items for map fields
        let count = rand::random_range(1..5);
        let mut map = std::collections::HashMap::new();
        
        // Map entry kind is a Message, but we need key/value kinds
        // The `kind` for a map field is `Message` pointing to the MapEntry.
        if let Kind::Message(entry_desc) = &kind {
            let key_field = entry_desc.get_field(1).unwrap();
            let val_field = entry_desc.get_field(2).unwrap();
            let key_kind = key_field.kind();
            let val_kind = val_field.kind();

            for i in 0..count {
                // For keys, we usually want simple generation or from template if specified?
                // Map keys in proto are usually strings or ints.
                // Let's generate a simple key based on index to ensure uniqueness easily
                let map_key = match key_kind {
                    Kind::String => prost_reflect::MapKey::String(format!("key_{}", i)),
                    Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => prost_reflect::MapKey::I32(i as i32),
                    Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => prost_reflect::MapKey::I64(i as i64),
                    Kind::Uint32 | Kind::Fixed32 => prost_reflect::MapKey::U32(i as u32),
                    Kind::Uint64 | Kind::Fixed64 => prost_reflect::MapKey::U64(i as u64),
                    Kind::Bool => prost_reflect::MapKey::Bool(i % 2 == 0),
                    _ => return Err(format!("Unsupported map key kind: {:?}", key_kind).into()),
                };

                let val = generate_single_value(&val_kind, field_name, field_spec)?;
                map.insert(map_key, val);
            }
            Ok(Value::Map(map))
        } else {
            Err("Map field kind is not Message".into())
        }
    } else {
        generate_single_value(&kind, field_name, field_spec)
    }
}

fn generate_single_value(
    kind: &Kind,
    field_name: &str,
    field_spec: &FieldSpec,
) -> Result<Value, Box<dyn std::error::Error>> {
    match kind {
        Kind::Message(desc) => generate_random_message(desc, field_spec).map(Value::Message),
        _ => {
            println!("field_name: {}, kind: {:?}", field_name, kind);
            // Check template
            if let Some(FieldGenerator::Template(template)) = field_spec.get_generator(field_name) {
                let val_str = FieldSpec::generate_from_template(template);
                println!("field_name: {}, val_str: {}", field_name, val_str);
                match parse_value(kind, &val_str) {
                    Ok(val) => Ok(val),
                    Err(e) => {
                        eprintln!(
                            "Warning: Failed to parse '{}' for field {}: {}. Using random default.",
                            val_str, field_name, e
                        );
                        generate_random_primitive(kind)
                    }
                }
            } else {
                generate_random_primitive(kind)
            }
        }
    }
}

fn parse_value(kind: &Kind, val_str: &str) -> Result<Value, String> {
    match kind {
        Kind::Double => f64::from_str(val_str)
            .map(Value::F64)
            .map_err(|e| e.to_string()),
        Kind::Float => f32::from_str(val_str)
            .map(Value::F32)
            .map_err(|e| e.to_string()),
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => i32::from_str(val_str)
            .map(Value::I32)
            .map_err(|e| e.to_string()),
        Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => i64::from_str(val_str)
            .map(Value::I64)
            .map_err(|e| e.to_string()),
        Kind::Uint32 | Kind::Fixed32 => u32::from_str(val_str)
            .map(Value::U32)
            .map_err(|e| e.to_string()),
        Kind::Uint64 | Kind::Fixed64 => u64::from_str(val_str)
            .map(Value::U64)
            .map_err(|e| e.to_string()),
        Kind::Bool => bool::from_str(val_str)
            .map(Value::Bool)
            .map_err(|e| e.to_string()),
        Kind::String => Ok(Value::String(val_str.to_string())),
        Kind::Bytes => Ok(Value::Bytes(val_str.as_bytes().to_vec().into())),
        Kind::Enum(desc) => {
            if let Some(val) = desc.get_value_by_name(val_str) {
                Ok(Value::EnumNumber(val.number()))
            } else if let Ok(num) = i32::from_str(val_str) {
                if desc.get_value(num).is_some() {
                    Ok(Value::EnumNumber(num))
                } else {
                    Err(format!("Invalid enum number {} for {}", num, desc.name()))
                }
            } else {
                Err(format!(
                    "Invalid enum value '{}' for {}",
                    val_str,
                    desc.name()
                ))
            }
        }
        _ => Err(format!("Parsing not supported for kind: {:?}", kind)),
    }
}

fn generate_random_primitive(kind: &Kind) -> Result<Value, Box<dyn std::error::Error>> {
    match kind {
        Kind::Double => Ok(Value::F64(rand::random::<f64>())),
        Kind::Float => Ok(Value::F32(rand::random::<f32>())),
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => Ok(Value::I32(rand::random::<i32>())),
        Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => Ok(Value::I64(rand::random::<i64>())),
        Kind::Uint32 | Kind::Fixed32 => Ok(Value::U32(rand::random::<u32>())),
        Kind::Uint64 | Kind::Fixed64 => Ok(Value::U64(rand::random::<u64>())),
        Kind::Bool => Ok(Value::Bool(rand::random_bool(0.5))),
        Kind::String => Ok(Value::String(format!(
            "random_{}",
            rand::random_range(0..1000)
        ))),
        Kind::Bytes => {
            let len = rand::random_range(1..20);
            Ok(Value::Bytes(
                (0..len)
                    .map(|_| rand::random::<u8>())
                    .collect::<Vec<u8>>()
                    .into(),
            ))
        }
        Kind::Enum(desc) => {
            let values: Vec<_> = desc.values().collect();
            if values.is_empty() {
                Ok(Value::EnumNumber(0)) // Should not happen for valid enums
            } else {
                let idx = rand::random_range(0..values.len());
                Ok(Value::EnumNumber(values[idx].number()))
            }
        }
        Kind::Message(_) => Err("Should be handled by caller".into()),
    }
}
