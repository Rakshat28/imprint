use crate::types::Hash;
use anyhow::{Context, Result};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

const SPARSE_CHUNK: usize = 4 * 1024;
const SPARSE_TOTAL: u64 = 12 * 1024;
const FULL_BUF: usize = 128 * 1024;

pub fn sparse_hash(path: &Path, size: u64) -> Result<Hash> {
    if size <= SPARSE_TOTAL {
        return full_hash(path);
    }

    let mut file = File::open(path).with_context(|| format!("open file {:?}", path))?;
    let mut hasher = blake3::Hasher::new();

    let mut buffer = vec![0u8; SPARSE_CHUNK];

    read_at(&mut file, 0, &mut buffer)?;
    hasher.update(&buffer);

    let middle = (size / 2).saturating_sub((SPARSE_CHUNK / 2) as u64);
    read_at(&mut file, middle, &mut buffer)?;
    hasher.update(&buffer);

    let end = size.saturating_sub(SPARSE_CHUNK as u64);
    read_at(&mut file, end, &mut buffer)?;
    hasher.update(&buffer);

    Ok(hasher.finalize().into())
}

pub fn full_hash(path: &Path) -> Result<Hash> {
    let mut file = File::open(path).with_context(|| format!("open file {:?}", path))?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0u8; FULL_BUF];

    loop {
        let read = file.read(&mut buffer).with_context(|| "read file")?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }

    Ok(hasher.finalize().into())
}

fn read_at(file: &mut File, offset: u64, buffer: &mut [u8]) -> Result<()> {
    file.seek(SeekFrom::Start(offset))
        .with_context(|| "seek file")?;
    file.read_exact(buffer).with_context(|| "read sparse chunk")?;
    Ok(())
}
