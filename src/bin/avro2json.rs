use clap::Parser;
use humansize::{format_size, DECIMAL};
use kafka_avro_utility::avrofile::load_avro_file;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "avro2json")]
#[command(about = "Convert Avro records to length-prefixed JSON format")]
struct Args {
    /// Input Avro file path
    #[arg(long, required = true)]
    in_file: PathBuf,

    /// Output JSON file path
    #[arg(long, required = true)]
    out: PathBuf,

    /// Disable zstd compression
    #[arg(long, default_value_t = false)]
    disable_compression: bool,

    /// Compression level (default: 3)
    #[arg(long, default_value_t = 3)]
    compress_level: i32,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let enable_compression = !args.disable_compression;
    let out_filename = args.out.to_string_lossy();

    if enable_compression {
        if !out_filename.ends_with(".json.zst") {
            eprintln!("Error: Output file must end with .json.zst when compression is enabled.");
            std::process::exit(1);
        }
    } else if !out_filename.ends_with(".json") {
        eprintln!("Error: Output file must end with .json when compression is disabled.");
        std::process::exit(1);
    }

    let (schema_str, _schema, mut iterator) = load_avro_file(&args.in_file)?;

    println!("Schema loaded: {}", schema_str);

    let file = File::create(&args.out)?;
    let mut writer: Box<dyn Write> = if enable_compression {
        Box::new(zstd::stream::write::Encoder::new(file, args.compress_level)?.auto_finish())
    } else {
        Box::new(file)
    };

    let mut count = 0;
    let mut total_uncompressed_bytes = 0u64;

    while let Some(record) = iterator.next() {
        match record {
            Ok(value) => {
                // Convert Avro Value to serde_json::Value
                let json_value: serde_json::Value = match value.try_into() {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("Error converting record {} to JSON: {}", count + 1, e);
                        break;
                    }
                };

                // Serialize to JSON string (compact)
                let json_bytes = serde_json::to_vec(&json_value)?;

                // Write 4-byte length prefix (Big Endian)
                let len = json_bytes.len() as u32;
                writer.write_all(&len.to_be_bytes())?;
                writer.write_all(&json_bytes)?;

                total_uncompressed_bytes += 4 + json_bytes.len() as u64;
                count += 1;

                if count % 10000 == 0 {
                    println!(
                        "Processed {} records (Uncompressed JSON size: {})...",
                        count,
                        format_size(total_uncompressed_bytes, DECIMAL)
                    );
                }
            }
            Err(e) => {
                eprintln!("Error reading record {}: {}", count + 1, e);
                break;
            }
        }
    }

    writer.flush()?;
    // Drop writer to ensure finish is called if it's an encoder (handled by auto_finish)
    drop(writer);

    let file_size = std::fs::metadata(&args.out)?.len();
    let ratio = if file_size > 0 {
        total_uncompressed_bytes as f64 / file_size as f64
    } else {
        0.0
    };
    
    println!(
        "Done. Converted {} records.\nTotal JSON Output Size: {}\nFinal File Size: {}\nCompression Ratio: {:.2}x",
        count,
        format_size(total_uncompressed_bytes, DECIMAL),
        format_size(file_size, DECIMAL),
        ratio
    );

    Ok(())
}

