use clap::Parser;
use humansize::{format_size, DECIMAL};
use kafka_avro_utility::avrogen::generate_random_record;
use kafka_avro_utility::fieldspec::FieldSpec;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "avrogen")]
#[command(about = "Generate random Avro records and write to a file")]
struct Args {
    /// Path to Avro schema file (.avsc)
    #[arg(long, required = true)]
    schema: PathBuf,

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
        if !out_filename.ends_with(".avro.zst") {
            eprintln!("Error: Output file must end with .avro.zst when compression is enabled.");
            std::process::exit(1);
        }
    } else if !out_filename.ends_with(".avro") {
        eprintln!("Error: Output file must end with .avro when compression is disabled.");
        std::process::exit(1);
    }

    // Load Schema
    let schema_str = std::fs::read_to_string(&args.schema)?;
    let schema = apache_avro::Schema::parse_str(&schema_str)?;

    // Extract RecordSchema for generation
    let record_schema = if let apache_avro::Schema::Record(ref inner) = schema {
        inner
    } else {
        return Err("Schema must be a Record".into());
    };

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

    // Write Schema Length and Schema
    let schema_bytes = schema_str.as_bytes();
    let schema_len = schema_bytes.len() as u32;
    writer.write_all(&schema_len.to_be_bytes())?;
    writer.write_all(schema_bytes)?;
    total_uncompressed_bytes += 4 + schema_bytes.len() as u64;

    println!(
        "Generating {} messages using schema from {:?}...",
        args.num_messages, args.schema
    );

    for i in 0..args.num_messages {
        // Generate Record
        let record = generate_random_record(record_schema, &field_spec)?;

        // Encode Record (Raw Avro)
        let bytes = apache_avro::to_avro_datum(&schema, record)?;

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
    // Drop writer to ensure finish is called if it's an encoder (handled by auto_finish)
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
