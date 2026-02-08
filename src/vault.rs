use crate::types::{hash_to_hex, Hash};
use anyhow::{Context, Result};
use std::path::{Path, PathBuf};

pub fn vault_root() -> Result<PathBuf> {
    let home = std::env::var("HOME").with_context(|| "HOME not set")?;
    Ok(PathBuf::from(home).join(".imprint").join("store"))
}

pub fn shard_path(hash: &Hash) -> Result<PathBuf> {
    let hex = hash_to_hex(hash);
    let shard_a = &hex[0..2];
    let shard_b = &hex[2..4];
    let root = vault_root()?;
    Ok(root.join(shard_a).join(shard_b).join(hex))
}

pub fn ensure_in_vault(hash: &Hash, src: &Path) -> Result<PathBuf> {
    let dest = shard_path(hash)?;
    if dest.exists() {
        return Ok(dest);
    }
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create vault directory {:?}", parent))?;
    }

    match std::fs::rename(src, &dest) {
        Ok(_) => Ok(dest),
        Err(_) => {
            std::fs::copy(src, &dest).with_context(|| "copy into vault")?;
            std::fs::remove_file(src).with_context(|| "remove original after copy")?;
            Ok(dest)
        }
    }
}
