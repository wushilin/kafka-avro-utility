use apache_avro::types::Value;
use apache_avro::Schema;
use std::fs::File;
use std::io::{self, BufReader, Read, Seek};
use std::path::Path;
use std::sync::Arc;

pub struct AvroFileIterator {
    reader: BufReader<Box<dyn Read + Send>>,
    schema: Arc<Schema>,
    pub total_bytes_read: u64,
}

impl Iterator for AvroFileIterator {
    type Item = io::Result<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut len_buf = [0u8; 4];
        match self.reader.read(&mut len_buf) {
            Ok(0) => return None, // EOF cleanly
            Ok(n) => {
                if n < 4 {
                    // Partial read of length. Try to read the rest.
                    if let Err(e) = self.reader.read_exact(&mut len_buf[n..]) {
                        if e.kind() == io::ErrorKind::UnexpectedEof {
                            return None; // Partial length at EOF, skip
                        }
                        return Some(Err(e));
                    }
                }
                self.total_bytes_read += 4;

                let len = u32::from_be_bytes(len_buf) as usize;

                let mut buf = vec![0u8; len];
                if let Err(e) = self.reader.read_exact(&mut buf) {
                    if e.kind() == io::ErrorKind::UnexpectedEof {
                        return None; // Partial record, skip
                    }
                    return Some(Err(e));
                }
                self.total_bytes_read += len as u64;

                let mut reader = &buf[..];
                match apache_avro::from_avro_datum(&self.schema, &mut reader, Some(&self.schema)) {
                    Ok(v) => Some(Ok(v)),
                    Err(e) => Some(Err(io::Error::new(io::ErrorKind::InvalidData, e))),
                }
            }
            Err(e) => Some(Err(e)), // Handles actual I/O errors (e.g. disk failure), not EOF
        }
    }
}

pub fn load_avro_file<P: AsRef<Path>>(
    path: P,
) -> io::Result<(String, Arc<Schema>, AvroFileIterator)> {
    let mut file = File::open(path)?;

    // Check for zstd magic number: 0xFD2FB528 (LE) -> 28 B5 2F FD
    let mut magic = [0u8; 4];
    let count = file.read(&mut magic)?;
    file.seek(io::SeekFrom::Start(0))?;

    let is_zstd = if count == 4 {
        magic == [0x28, 0xB5, 0x2F, 0xFD]
    } else {
        false
    };

    let reader: Box<dyn Read + Send> = if is_zstd {
        Box::new(zstd::stream::read::Decoder::new(file)?)
    } else {
        Box::new(file)
    };

    // Use 20 MiB buffer for better performance
    let mut reader = BufReader::with_capacity(20 * 1024 * 1024, reader);

    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf)?;
    let schema_len = u32::from_be_bytes(len_buf) as usize;

    let mut schema_bytes = vec![0u8; schema_len];
    reader.read_exact(&mut schema_bytes)?;
    let schema_str = String::from_utf8(schema_bytes)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let initial_bytes_read = 4 + schema_len as u64;

    let schema = Schema::parse_str(&schema_str)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let schema = Arc::new(schema);

    Ok((
        schema_str,
        schema.clone(),
        AvroFileIterator { reader, schema, total_bytes_read: initial_bytes_read },
    ))
}
