use serde::{Deserialize, Serialize};

pub type Hash = [u8; 32];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileMetadata {
    pub size: u64,
    pub modified: u64,
    pub hash: Hash,
}

pub fn hash_to_hex(hash: &Hash) -> String {
    blake3::Hash::from_bytes(*hash).to_hex().to_string()
}
