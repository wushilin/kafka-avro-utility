use prost_reflect::{DescriptorPool, DynamicMessage};
use std::fs::File;
use std::io::{self, BufReader, Read, Seek};
use std::path::Path;

pub struct ProtoFileIterator {
    reader: BufReader<Box<dyn Read + Send>>,
    #[allow(dead_code)]
    descriptor_pool: DescriptorPool,
    message_descriptor: prost_reflect::MessageDescriptor,
    pub total_bytes_read: u64,
}

impl Iterator for ProtoFileIterator {
    type Item = io::Result<DynamicMessage>;

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
                match DynamicMessage::decode(self.message_descriptor.clone(), &mut reader) {
                    Ok(msg) => Some(Ok(msg)),
                    Err(e) => Some(Err(io::Error::new(io::ErrorKind::InvalidData, e))),
                }
            }
            Err(e) => Some(Err(e)),
        }
    }
}

pub fn load_proto_file<P: AsRef<Path>>(
    path: P,
    expected_message_name: Option<&str>,
) -> io::Result<(DescriptorPool, String, ProtoFileIterator)> {
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

    // Read FDS
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf)?;
    let schema_len = u32::from_be_bytes(len_buf) as usize;
    let mut schema_bytes = vec![0u8; schema_len];
    reader.read_exact(&mut schema_bytes)?;
    let _schema_text = String::from_utf8(schema_bytes)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let mut initial_bytes_read = 4 + schema_len as u64;

    reader.read_exact(&mut len_buf)?;
    let fds_len = u32::from_be_bytes(len_buf) as usize;
    let mut fds_bytes = vec![0u8; fds_len];
    reader.read_exact(&mut fds_bytes)?;

    initial_bytes_read += 4 + fds_len as u64;

    // Read Message Name
    reader.read_exact(&mut len_buf)?;
    let msg_name_len = u32::from_be_bytes(len_buf) as usize;
    
    let mut msg_name_bytes = vec![0u8; msg_name_len];
    reader.read_exact(&mut msg_name_bytes)?;
    let message_name = String::from_utf8(msg_name_bytes)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    
    initial_bytes_read += 4 + msg_name_len as u64;

    // Verify message name if expected one is provided
    if let Some(expected) = expected_message_name {
        if message_name != expected {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData, 
                format!("File contains message '{}', but expected '{}'", message_name, expected)
            ));
        }
    }

    let pool = DescriptorPool::decode(fds_bytes.as_slice())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let message_descriptor = pool
        .get_message_by_name(&message_name)
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, format!("Message '{}' not found in schema", message_name)))?;

    Ok((
        pool.clone(),
        message_name,
        ProtoFileIterator {
            reader,
            descriptor_pool: pool,
            message_descriptor,
            total_bytes_read: initial_bytes_read,
        },
    ))
}

