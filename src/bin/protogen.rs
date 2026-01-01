use clap::Parser;
use humansize::{format_size, DECIMAL};
use kafka_avro_utility::fieldspec::FieldSpec;
use kafka_avro_utility::protogen::generate_random_message;
use prost::Message;
use prost_reflect::DescriptorPool;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

#[derive(Parser, Debug)]
#[command(name = "protogen")]
#[command(about = "Generate random Protobuf records and write to a file")]
struct Args {
    /// Path to Protobuf schema file (.proto)
    #[arg(long, required = true)]
    schema: PathBuf,

    /// Name of the message type to generate (fully qualified, e.g. package.Message). Optional if schema contains only one message.
    #[arg(long)]
    message: Option<String>,

    /// Include paths for .proto imports
    #[arg(long)]
    include: Vec<PathBuf>,

    /// Number of messages to generate
    #[arg(long, default_value_t = 100)]
    num_messages: usize,

    /// Output file path
    #[arg(long, required = true)]
    out: PathBuf,

    /// Disable zstd compression
    #[arg(long, default_value_t = false)]
    disable_compression: bool,

    /// Compression level (default: 3)
    #[arg(long, default_value_t = 3)]
    compress_level: i32,

    /// Path to field specification file
    #[arg(long, default_value = "field_spec.txt")]
    field_spec: PathBuf,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let enable_compression = !args.disable_compression;
    let out_filename = args.out.to_string_lossy();

    if enable_compression {
        if !out_filename.ends_with(".pb.zst") {
             eprintln!("Error: Output file must end with .pb.zst when compression is enabled.");
             std::process::exit(1);
        }
    } else if !out_filename.ends_with(".pb") {
         eprintln!("Error: Output file must end with .pb when compression is disabled.");
         std::process::exit(1);
    }

    // Compile Schema
    let mut includes = args.include.clone();
    if let Some(parent) = args.schema.parent() {
        if includes.is_empty() {
            includes.push(parent.to_path_buf());
        }
    }
    
    let schema_text = std::fs::read_to_string(&args.schema)?;
    let file_descriptor_set = protox::compile(&[&args.schema], &includes)?;
    let pool = DescriptorPool::from_file_descriptor_set(file_descriptor_set.clone())?;
    
    // Determine message name
    let message_name = match args.message {
        Some(name) => name,
        None => {
            let schema_basename = args
                .schema
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or_default();

            let mut preferred_messages = Vec::new();
            let mut fallback_messages = Vec::new();

            for file in &file_descriptor_set.file {
                let package = file.package.as_deref().unwrap_or("");
                let collect_messages = |target: &mut Vec<String>| {
                    for msg in &file.message_type {
                        let msg_name = msg.name.as_deref().unwrap_or("");
                        if msg_name.is_empty() {
                            continue;
                        }
                        let fq_name = if package.is_empty() {
                            msg_name.to_string()
                        } else {
                            format!("{}.{}", package, msg_name)
                        };
                        target.push(fq_name);
                    }
                };

                if let Some(name) = &file.name {
                    let file_basename = Path::new(name)
                        .file_name()
                        .and_then(|n| n.to_str())
                        .unwrap_or_default();
                    if !schema_basename.is_empty() && file_basename == schema_basename {
                        collect_messages(&mut preferred_messages);
                        continue;
                    }
                }

                collect_messages(&mut fallback_messages);
            }

            let messages = if !preferred_messages.is_empty() {
                preferred_messages
            } else {
                fallback_messages
            };

            if messages.len() == 1 {
                messages.into_iter().next().unwrap()
            } else {
                eprintln!(
                    "Error: --message not specified and schema contains {} candidate messages:",
                    messages.len()
                );
                for m in messages {
                    eprintln!("  - {}", m);
                }
                std::process::exit(1);
            }
        }
    };

    let message_descriptor = pool.get_message_by_name(&message_name)
        .ok_or_else(|| format!("Message '{}' not found in schema", message_name))?;

    // Load Field Spec
    let field_spec = FieldSpec::load(&args.field_spec)?;

    // Open Output File
    let file = File::create(&args.out)?;

    let mut writer: Box<dyn Write> = if enable_compression {
        Box::new(zstd::stream::write::Encoder::new(file, args.compress_level)?.auto_finish())
    } else {
        Box::new(file)
    };

    let mut total_uncompressed_bytes = 0u64;

    // Write Schema Text Length and Bytes
    let schema_bytes = schema_text.as_bytes();
    let schema_len = schema_bytes.len() as u32;
    writer.write_all(&schema_len.to_be_bytes())?;
    writer.write_all(schema_bytes)?;
    total_uncompressed_bytes += 4 + schema_bytes.len() as u64;

    // Write FileDescriptorSet Length and Bytes
    let fds_bytes = file_descriptor_set.encode_to_vec();
    let fds_len = fds_bytes.len() as u32;
    writer.write_all(&fds_len.to_be_bytes())?;
    writer.write_all(&fds_bytes)?;
    total_uncompressed_bytes += 4 + fds_bytes.len() as u64;

    // Write Message Name Length and Bytes
    let msg_name_bytes = message_name.as_bytes();
    let msg_name_len = msg_name_bytes.len() as u32;
    writer.write_all(&msg_name_len.to_be_bytes())?;
    writer.write_all(msg_name_bytes)?;
    total_uncompressed_bytes += 4 + msg_name_bytes.len() as u64;

    println!(
        "Generating {} messages of type '{}'...",
        args.num_messages, message_name
    );

    for i in 0..args.num_messages {
        let record = generate_random_message(&message_descriptor, &field_spec)?;
        
        // Encode Record
        let bytes = record.encode_to_vec();

        // Write Record Length and Record
        let record_len = bytes.len() as u32;
        writer.write_all(&record_len.to_be_bytes())?;
        writer.write_all(&bytes)?;
        total_uncompressed_bytes += 4 + bytes.len() as u64;

         if (i + 1) % 1000 == 0 {
            println!(
                "Generated {} messages ({} uncompressed)...",
                i + 1,
                format_size(total_uncompressed_bytes, DECIMAL)
            );
        }
    }

    writer.flush()?;
    drop(writer);

    let file_size = std::fs::metadata(&args.out)?.len();
    let ratio = if file_size > 0 {
        total_uncompressed_bytes as f64 / file_size as f64
    } else {
        0.0
    };

    println!(
        "Done. Output written to {:?}\nTotal Uncompressed: {}\nFinal File Size: {}\nCompression Ratio: {:.2}x",
        args.out,
        format_size(total_uncompressed_bytes, DECIMAL),
        format_size(file_size, DECIMAL),
        ratio
    );

    Ok(())
}

