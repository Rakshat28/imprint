use crate::types::{FileMetadata, Hash};
use anyhow::{Context, Result};
use redb::{Database, TableDefinition};
use std::path::{Path, PathBuf};

const FILE_INDEX: TableDefinition<&[u8], &[u8]> = TableDefinition::new("file_index");
const CAS_INDEX: TableDefinition<&[u8], &[u8]> = TableDefinition::new("cas_index");

pub struct State {
    db: Database,
}

impl State {
    pub fn open_default() -> Result<Self> {
        let db_path = default_db_path()?;
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create state directory {:?}", parent))?;
        }
        let db = Database::create(&db_path).with_context(|| "open redb database")?;
        Ok(Self { db })
    }

    pub fn upsert_file(&self, path: &Path, metadata: &FileMetadata) -> Result<()> {
        let key = path.to_string_lossy().as_bytes().to_vec();
        let value = bincode::serialize(metadata).with_context(|| "serialize file metadata")?;
        let txn = self.db.begin_write().with_context(|| "begin write transaction")?;
        {
            let mut table = txn.open_table(FILE_INDEX)?;
            table.insert(key.as_slice(), value.as_slice())?;
        }
        txn.commit().with_context(|| "commit file index write")?;
        Ok(())
    }

    pub fn set_cas_refcount(&self, hash: &Hash, count: u64) -> Result<()> {
        let key = hash.to_vec();
        let value = count.to_le_bytes().to_vec();
        let txn = self.db.begin_write().with_context(|| "begin write transaction")?;
        {
            let mut table = txn.open_table(CAS_INDEX)?;
            table.insert(key.as_slice(), value.as_slice())?;
        }
        txn.commit().with_context(|| "commit cas index write")?;
        Ok(())
    }
}

pub fn default_db_path() -> Result<PathBuf> {
    let home = std::env::var("HOME").with_context(|| "HOME not set")?;
    Ok(PathBuf::from(home).join(".imprint").join("state.redb"))
}
