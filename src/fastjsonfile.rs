use std::fs::File;
use std::io::{self, BufReader, Read, Seek};
use std::path::Path;

pub struct BytesFileIterator {
    reader: BufReader<Box<dyn Read + Send>>,
}

pub struct BytesEntry {
    buffer: Vec<u8>,
}

impl BytesEntry {
    pub fn new(size: usize) -> Self {
        Self {
            buffer: vec![0u8; size + 4], // 4 bytes length prefix + data
        }
    }

    pub fn get_data_mut(&mut self) -> &mut [u8] {
        &mut self.buffer[4..]
    }

    pub fn get_data(&self) -> &[u8] {
        &self.buffer[4..]
    }

    pub fn take(self) -> Vec<u8> {
        self.buffer
    }
}

impl Iterator for BytesFileIterator {
    type Item = io::Result<BytesEntry>;

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

                let len = u32::from_be_bytes(len_buf) as usize;

                let mut entry = BytesEntry::new(len);
                // Copy length back to buffer if needed (though usually we just want the data)
                entry.buffer[0..4].copy_from_slice(&len_buf);
                
                if let Err(e) = self.reader.read_exact(entry.get_data_mut()) {
                    if e.kind() == io::ErrorKind::UnexpectedEof {
                        return None; // Partial record, skip
                    }
                    return Some(Err(e));
                }

                Some(Ok(entry))
            }
            Err(e) => Some(Err(e)), // Handles actual I/O errors (e.g. disk failure), not EOF
        }
    }
}

pub fn load_json_file<P: AsRef<Path>>(path: P) -> io::Result<BytesFileIterator> {
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
    let reader = BufReader::with_capacity(20 * 1024 * 1024, reader);

    Ok(BytesFileIterator { reader })
}

