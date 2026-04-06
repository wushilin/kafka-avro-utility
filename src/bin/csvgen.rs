use clap::{ArgAction, Parser};
use kafka_avro_utility::fieldspec::{FieldGenerator, FieldSpec};
use precise_rate_limiter::FastQuotaSync;
use std::fs;
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(name = "csvgen")]
#[command(about = "Generate CSV rows from a simple column schema and field spec")]
struct Args {
    /// Path to CSV schema file containing comma-separated column names
    #[arg(long, required = true)]
    schema: PathBuf,

    /// Path to field specification file
    #[arg(long, default_value = "field_spec.txt")]
    field_spec: PathBuf,

    /// Number of rows to generate
    #[arg(long, default_value_t = 100)]
    num_rows: usize,

    /// Include the schema header row in the output
    #[arg(long = "add-header", action = ArgAction::SetTrue)]
    add_header: bool,

    /// Number of generation threads
    #[arg(long, default_value_t = 5)]
    threads: usize,

    /// Rows per generated batch sent to the printer thread
    #[arg(long, default_value_t = 10_000)]
    batch: usize,

    /// Output row rate limit in rows per second. Unset means no limit.
    #[arg(long)]
    rows_per_second: Option<f64>,

    /// Output throughput limit in MiB per second. Unset means no limit.
    #[arg(long)]
    mbytes_per_second: Option<f64>,
}

struct RateLimit {
    limiter: Arc<FastQuotaSync>,
    max_per_sec: usize,
}

struct BatchPayload {
    data: String,
    row_count: usize,
    byte_count: usize,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    if args.threads == 0 {
        return Err("--threads must be at least 1".into());
    }
    if args.batch == 0 {
        return Err("--batch must be at least 1".into());
    }

    let columns = Arc::new(load_columns(&args.schema)?);
    let field_spec = Arc::new(FieldSpec::load(&args.field_spec)?);
    ensure_all_columns_have_specs(&columns, &field_spec)?;

    let row_rate_limiter = create_row_rate_limiter(args.rows_per_second);
    let byte_rate_limiter = create_byte_rate_limiter(args.mbytes_per_second);

    let (tx, rx) = mpsc::sync_channel::<Result<BatchPayload, String>>(1);
    let next_row = Arc::new(AtomicUsize::new(0));
    let total_rows = args.num_rows;
    let add_header = args.add_header;
    let thread_count = args.threads;
    let batch_size = args.batch;
    let printer_columns = Arc::clone(&columns);

    let printer = thread::spawn(move || -> Result<(), String> {
        let stdout = io::stdout();
        let mut writer = BufWriter::new(stdout.lock());

        if add_header {
            let header = render_csv_row(printer_columns.iter().map(String::as_str));
            writer
                .write_all(header.as_bytes())
                .map_err(|e| format!("failed to write header: {e}"))?;
        }

        while let Ok(batch_result) = rx.recv() {
            let batch = batch_result?;
            acquire_units(&row_rate_limiter, batch.row_count);
            acquire_units(&byte_rate_limiter, batch.byte_count);
            writer
                .write_all(batch.data.as_bytes())
                .map_err(|e| format!("failed to write batch: {e}"))?;
        }

        writer
            .flush()
            .map_err(|e| format!("failed to flush output: {e}"))?;
        Ok(())
    });

    let mut workers = Vec::with_capacity(thread_count);
    for _ in 0..thread_count {
        let tx = tx.clone();
        let next_row = Arc::clone(&next_row);
        let columns = Arc::clone(&columns);
        let field_spec = Arc::clone(&field_spec);
        let batch_size = batch_size;

        workers.push(thread::spawn(move || -> Result<(), String> {
            loop {
                let start = next_row.fetch_add(batch_size, Ordering::Relaxed);
                if start >= total_rows {
                    break;
                }

                let end = (start + batch_size).min(total_rows);
                let row_count = end - start;
                let mut data = String::new();

                for _ in 0..row_count {
                    let row = match columns
                        .iter()
                        .map(|column| generate_field_value(&field_spec, column))
                        .collect::<Result<Vec<_>, _>>()
                    {
                        Ok(row) => row,
                        Err(err) => {
                            let _ = tx.send(Err(err.to_string()));
                            return Ok(());
                        }
                    };
                    data.push_str(&render_csv_row(row.iter().map(String::as_str)));
                }

                let byte_count = data.len();
                tx.send(Ok(BatchPayload {
                    data,
                    row_count,
                    byte_count,
                }))
                .map_err(|e| format!("failed to send batch to printer: {e}"))?;
            }

            Ok(())
        }));
    }
    drop(tx);

    for worker in workers {
        match worker.join() {
            Ok(Ok(())) => {}
            Ok(Err(err)) => return Err(err.into()),
            Err(_) => return Err("generation thread panicked".into()),
        }
    }

    match printer.join() {
        Ok(Ok(())) => Ok(()),
        Ok(Err(err)) => Err(err.into()),
        Err(_) => Err("printer thread panicked".into()),
    }
}

fn create_row_rate_limiter(rows_per_second: Option<f64>) -> Option<RateLimit> {
    let rows_per_second = rows_per_second?;
    if rows_per_second <= 0.0 {
        return None;
    }

    let quota = rows_per_second.max(1.0) as usize;
    let refill_interval = Duration::from_millis(100);
    let refill_amount = (quota / 10).max(1);
    Some(RateLimit {
        limiter: FastQuotaSync::new(quota, refill_amount, refill_interval),
        max_per_sec: quota,
    })
}

fn create_byte_rate_limiter(mbytes_per_second: Option<f64>) -> Option<RateLimit> {
    let mbytes_per_second = mbytes_per_second?;
    if mbytes_per_second <= 0.0 {
        return None;
    }

    let bytes_per_sec = (mbytes_per_second * 1024.0 * 1024.0).max(1.0) as usize;
    let refill_interval = Duration::from_millis(100);
    let refill_amount = (bytes_per_sec / 10).max(1);
    Some(RateLimit {
        limiter: FastQuotaSync::new(bytes_per_sec, refill_amount, refill_interval),
        max_per_sec: bytes_per_sec,
    })
}

fn acquire_units(rate_limiter: &Option<RateLimit>, units: usize) {
    if let Some(limiter) = rate_limiter {
        let chunk_size = limiter.max_per_sec.max(1);
        let mut remaining = units.max(1);
        while remaining > 0 {
            let next = remaining.min(chunk_size);
            limiter.limiter.acquire(next);
            remaining -= next;
        }
    }
}

fn load_columns(path: &std::path::Path) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let content = fs::read_to_string(path)?;
    let non_empty_lines = content
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>();

    if non_empty_lines.is_empty() {
        return Err("schema file is empty".into());
    }

    if non_empty_lines.len() != 1 {
        return Err(
            "schema file must contain exactly one non-empty line of comma-separated columns".into(),
        );
    }

    let columns = non_empty_lines[0]
        .split(',')
        .map(str::trim)
        .filter(|column| !column.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();

    if columns.is_empty() {
        return Err("schema file did not contain any columns".into());
    }

    Ok(columns)
}

fn ensure_all_columns_have_specs(
    columns: &[String],
    field_spec: &FieldSpec,
) -> Result<(), Box<dyn std::error::Error>> {
    let missing = columns
        .iter()
        .filter(|column| field_spec.get_generator(column).is_none())
        .cloned()
        .collect::<Vec<_>>();

    if missing.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "missing field_spec entries for columns: {}",
            missing.join(", ")
        )
        .into())
    }
}

fn generate_field_value(
    field_spec: &FieldSpec,
    column: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    match field_spec.get_generator(column) {
        Some(FieldGenerator::Template(template)) => Ok(FieldSpec::generate_from_template(template)),
        None => Err(format!("no generator found for column '{column}'").into()),
    }
}

fn render_csv_row<'a, I, S>(fields: I) -> String
where
    I: IntoIterator<Item = S>,
    S: AsRef<str> + 'a,
{
    let mut row = String::new();
    let mut first = true;
    for field in fields {
        if !first {
            row.push(',');
        }
        first = false;
        row.push_str(&escape_csv_field(field.as_ref()));
    }
    row.push('\n');
    row
}

fn escape_csv_field(field: &str) -> String {
    let needs_quotes = field.contains([',', '"', '\n', '\r'])
        || field.chars().next().is_some_and(char::is_whitespace)
        || field.chars().last().is_some_and(char::is_whitespace);

    if !needs_quotes {
        return field.to_string();
    }

    let escaped = field.replace('"', "\"\"");
    format!("\"{escaped}\"")
}
