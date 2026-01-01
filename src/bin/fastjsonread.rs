use clap::Parser;
use kafka_avro_utility::fastjsonfile::load_json_file;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "fastjsonread")]
#[command(
    about = "Fast reading of length-prefixed JSON records from a file (skips parsing)"
)]
struct Args {
    /// Input file path
    #[arg(long, required = true)]
    in_file: PathBuf,

    /// Print the raw byte length of each record
    #[arg(long, default_value_t = false)]
    print: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let iterator = load_json_file(&args.in_file)?;

    let mut count = 0;
    for entry in iterator {
        match entry {
            Ok(bytes_entry) => {
                if args.print {
                    println!(
                        "Record {}: {} bytes",
                        count + 1,
                        bytes_entry.get_data().len()
                    );
                }
                count += 1;
                if count % 10000 == 0 {
                    println!("Processed {} records...", count);
                }
            }
            Err(e) => {
                eprintln!("Error reading record {}: {}", count + 1, e);
                break;
            }
        }
    }

    println!("Total records read: {}", count);

    Ok(())
}

