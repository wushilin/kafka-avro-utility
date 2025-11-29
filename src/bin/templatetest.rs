use clap::Parser;
use kafka_avro_utility::fieldspec::FieldSpec;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the field spec file
    #[arg(long, default_value = "field_spec.txt")]
    field_spec: PathBuf,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    println!("Loading field spec from: {:?}", args.field_spec);
    let field_spec = FieldSpec::load(&args.field_spec)?;

    // We need to know the field names to generate values.
    // FieldSpec stores generators in a HashMap, but doesn't expose keys directly publicly maybe?
    // Let's check src/fieldspec.rs to see if we can iterate keys.
    // FieldSpec has `generators: HashMap<String, FieldGenerator>`.
    // It doesn't seem to have a public iterator.
    // However, FieldSpec::load parses the file.
    // If we cannot iterate, we might need to parse the file again ourselves or expose an iterator.

    // Since I can modify the codebase, I should add `get_field_names` to FieldSpec.
    // But first let's check if I can access it.

    // If I cannot modify FieldSpec right now (I want to just write the test),
    // I can read the file to get keys.

    // Actually, let's just iterate the file to get keys, it's simple enough for a test tool.
    let content = std::fs::read_to_string(&args.field_spec)?;
    let mut field_names = Vec::new();

    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with("----------") {
            // Stop at separator
            if line.starts_with("----------") {
                break;
            }
            continue;
        }
        if line.starts_with("field_name") {
            continue;
        }

        let parts: Vec<&str> = line.splitn(2, ',').collect();
        if parts.len() >= 1 {
            field_names.push(parts[0].trim().to_string());
        }
    }
    for iteration in 1..=3 {
        println!("Iteration {} - Generating values for {} fields:", iteration, field_names.len());
        println!("----------------------------------------");

        for field_name in &field_names {
            if let Some(generator) = field_spec.get_generator(field_name) {
                // We need to generate a value.
                // FieldGenerator is an enum.
                // FieldSpec has `generate_from_template`. But it takes `&Template`.
                // And `FieldGenerator` wraps `Template`.
                // We need to match on FieldGenerator.

                // Wait, `FieldSpec::generate_from_template` is public.
                // `FieldGenerator` is public.
                // `Template` is public.

                use kafka_avro_utility::fieldspec::FieldGenerator;

                let value = match generator {
                    FieldGenerator::Template(template) => {
                        FieldSpec::generate_from_template(template)
                    }
                };

                println!("{} -> {}", field_name, value);
            } else {
                println!("{} -> [NO GENERATOR FOUND]", field_name);
            }
        }
    }

    Ok(())
}
