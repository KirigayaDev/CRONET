use crate::{
    config::MIN_SPOOL_BYTES,
    error::{storage, Error, Result},
};
use contracts::netmon::v1::RawSyslogEvent;
use prost::Message;
use redb::{
    backends::FileBackend, Database, Durability, ReadableTable, ReadableTableMetadata, StorageBackend, TableDefinition,
};
use std::{
    fs::OpenOptions,
    io,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

const PENDING: TableDefinition<u64, &[u8]> = TableDefinition::new("pending_v1");
const QUARANTINE: TableDefinition<u64, &[u8]> = TableDefinition::new("quarantine_v1");
const META: TableDefinition<&str, u64> = TableDefinition::new("metadata_v1");
const RECORD_OVERHEAD: u64 = 16 * 1024;
const HEADER: usize = 13; // version + created_at milliseconds + CRC32(header + protobuf)

/// The redb file (including allocation growth) cannot cross this physical cap.
/// FileBackend retains redb's exclusive file lock and native sync semantics.
#[derive(Debug)]
struct CappedFile {
    inner: FileBackend,
    max: u64,
}
impl CappedFile {
    fn check(&self, end: u64) -> io::Result<()> {
        if end > self.max {
            return Err(io::Error::other("spool physical byte limit reached"));
        }
        Ok(())
    }
}
impl StorageBackend for CappedFile {
    fn len(&self) -> io::Result<u64> {
        self.inner.len()
    }
    fn read(&self, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        self.inner.read(offset, len)
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        self.check(len)?;
        self.inner.set_len(len)
    }
    fn sync_data(&self, eventual: bool) -> io::Result<()> {
        self.inner.sync_data(eventual)
    }
    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.check(offset.checked_add(data.len() as u64).ok_or_else(|| io::Error::other("spool offset overflow"))?)?;
        self.inner.write(offset, data)
    }
}

pub struct Spool {
    db: Database,
    capacity: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct Stats {
    pub pending: u64,
    pub quarantined: u64,
    pub used_bytes: u64,
    pub capacity: u64,
}

pub struct Entry {
    pub key: u64,
    pub event: RawSyslogEvent,
}

fn encode(event: &RawSyslogEvent) -> Result<Vec<u8>> {
    let created = SystemTime::now().duration_since(UNIX_EPOCH).map_err(storage)?.as_millis();
    let mut record = Vec::with_capacity(HEADER + event.encoded_len());
    record.push(1);
    record.extend_from_slice(&(created as u64).to_be_bytes());
    record.extend_from_slice(&[0; 4]);
    event.encode(&mut record).map_err(storage)?;
    let crc = checksum(&record);
    record[9..13].copy_from_slice(&crc.to_be_bytes());
    Ok(record)
}

fn checksum(record: &[u8]) -> u32 {
    let mut crc = crc32fast::Hasher::new();
    crc.update(&record[..9]);
    crc.update(&record[HEADER..]);
    crc.finalize()
}

fn decode(record: &[u8]) -> Result<RawSyslogEvent> {
    if record.len() < HEADER || record.len() > 70 * 1024 || record[0] != 1 {
        return Err(Error::Corrupt);
    }
    let expected = u32::from_be_bytes(record[9..13].try_into().map_err(|_| Error::Corrupt)?);
    if expected != checksum(record) {
        return Err(Error::Corrupt);
    }
    let event = RawSyslogEvent::decode(&record[HEADER..]).map_err(|_| Error::Corrupt)?;
    if uuid::Uuid::parse_str(&event.event_id).is_err()
        || event.site.is_empty()
        || event.site.len() > 128
        || !event.site.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
        || event.payload.len() > 65536
        || !matches!(event.transport, 1 | 2)
    {
        return Err(Error::Corrupt);
    }
    Ok(event)
}

impl Spool {
    pub fn open(path: &Path, max_bytes: u64) -> Result<Self> {
        if max_bytes < MIN_SPOOL_BYTES {
            return Err(Error::Spool("physical cap must be at least 16 MiB".into()));
        }
        let parent = path.parent().filter(|p| !p.as_os_str().is_empty()).unwrap_or_else(|| Path::new("."));
        #[cfg(unix)]
        let created_parent = !parent.exists();
        std::fs::create_dir_all(parent)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if created_parent {
                std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))?;
            }
        }
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options.open(path)?;
        if file.metadata()?.len() > max_bytes {
            return Err(Error::Spool("existing file exceeds configured physical cap".into()));
        }
        let backend = CappedFile { inner: FileBackend::new(file).map_err(storage)?, max: max_bytes };
        let db = Database::builder().set_cache_size(8 * 1024 * 1024).create_with_backend(backend).map_err(storage)?;
        let mut tx = db.begin_write().map_err(storage)?;
        tx.set_durability(Durability::Immediate);
        {
            let mut meta = tx.open_table(META).map_err(storage)?;
            if meta.get("used").map_err(storage)?.is_none() {
                meta.insert("used", 0).map_err(storage)?;
                meta.insert("next", 0).map_err(storage)?;
            }
        }
        tx.open_table(PENDING).map_err(storage)?;
        tx.open_table(QUARANTINE).map_err(storage)?;
        tx.commit().map_err(storage)?;
        // Persist the directory entry too, including on first creation in a new volume.
        #[cfg(unix)]
        std::fs::File::open(parent)?.sync_all()?;
        Ok(Self { db, capacity: max_bytes / 4 - 1024 * 1024 })
    }

    pub fn stats(&self) -> Result<Stats> {
        let tx = self.db.begin_read().map_err(storage)?;
        let meta = tx.open_table(META).map_err(storage)?;
        let used_bytes = meta.get("used").map_err(storage)?.ok_or(Error::Corrupt)?.value();
        Ok(Stats {
            pending: tx.open_table(PENDING).map_err(storage)?.len().map_err(storage)?,
            quarantined: tx.open_table(QUARANTINE).map_err(storage)?.len().map_err(storage)?,
            used_bytes,
            capacity: self.capacity,
        })
    }

    /// Only a successful Immediate commit constitutes durable acceptance.
    pub fn insert(&self, event: &RawSyslogEvent) -> Result<u64> {
        let record = encode(event)?;
        let cost = record.len() as u64 + RECORD_OVERHEAD;
        let mut tx = self.db.begin_write().map_err(storage)?;
        tx.set_durability(Durability::Immediate);
        let key;
        {
            let mut meta = tx.open_table(META).map_err(storage)?;
            let used = meta.get("used").map_err(storage)?.ok_or(Error::Corrupt)?.value();
            if used.saturating_add(cost) > self.capacity {
                return Err(Error::Full);
            }
            key = meta.get("next").map_err(storage)?.ok_or(Error::Corrupt)?.value();
            let next = key.checked_add(1).ok_or(Error::Corrupt)?;
            meta.insert("used", used + cost).map_err(storage)?;
            meta.insert("next", next).map_err(storage)?;
            tx.open_table(PENDING).map_err(storage)?.insert(key, record.as_slice()).map_err(storage)?;
        }
        tx.commit().map_err(storage)?;
        Ok(key)
    }

    /// Each read transaction ends before the caller performs network I/O.
    pub fn batch(&self, limit: usize, newest: bool) -> Result<Vec<Entry>> {
        let mut entries = Vec::new();
        let mut bad = Vec::new();
        {
            let tx = self.db.begin_read().map_err(storage)?;
            let table = tx.open_table(PENDING).map_err(storage)?;
            let mut iter = table.iter().map_err(storage)?;
            for _ in 0..limit {
                let Some(row) = (if newest { iter.next_back() } else { iter.next() }) else {
                    break;
                };
                let (key, value) = row.map_err(storage)?;
                match decode(value.value()) {
                    Ok(event) => entries.push(Entry { key: key.value(), event }),
                    Err(_) => bad.push(key.value()),
                }
            }
        }
        for key in bad {
            self.quarantine(key)?;
            tracing::error!(spool_key = key, status = "quarantined", "invalid spool entry retained for recovery");
        }
        Ok(entries)
    }

    fn quarantine(&self, key: u64) -> Result<()> {
        let mut tx = self.db.begin_write().map_err(storage)?;
        tx.set_durability(Durability::Immediate);
        {
            let mut pending = tx.open_table(PENDING).map_err(storage)?;
            if let Some(value) = pending.remove(key).map_err(storage)? {
                tx.open_table(QUARANTINE).map_err(storage)?.insert(key, value.value()).map_err(storage)?;
            };
        }
        // Quarantined bytes still count against the admission limit.
        tx.commit().map_err(storage)?;
        Ok(())
    }

    /// Caller must have received the JetStream publication ACK for this key.
    pub fn delete(&self, key: u64) -> Result<()> {
        let mut tx = self.db.begin_write().map_err(storage)?;
        tx.set_durability(Durability::Immediate);
        {
            let mut pending = tx.open_table(PENDING).map_err(storage)?;
            if let Some(value) = pending.remove(key).map_err(storage)? {
                let cost = value.value().len() as u64 + RECORD_OVERHEAD;
                let mut meta = tx.open_table(META).map_err(storage)?;
                let used = meta.get("used").map_err(storage)?.ok_or(Error::Corrupt)?.value();
                meta.insert("used", used.checked_sub(cost).ok_or(Error::Corrupt)?).map_err(storage)?;
            };
        }
        tx.commit().map_err(storage)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use crate::{config::Config, model::envelope};
    use contracts::netmon::v1::SyslogTransport;
    fn event() -> RawSyslogEvent {
        let cfg = Config::from_map([("NETMON_SITE".into(), "test".into())].into()).unwrap();
        envelope(&cfg, "127.0.0.1:42".parse().unwrap(), SyslogTransport::Udp, vec![0xff; 65536])
    }

    #[test]
    fn crc_detects_header_and_payload_corruption() {
        let event = event();
        let record = encode(&event).unwrap();
        assert_eq!(decode(&record).unwrap(), event);
        for index in [0, 1, 9, HEADER, record.len() - 1] {
            let mut bad = record.clone();
            bad[index] ^= 1;
            assert!(decode(&bad).is_err());
        }
        assert!(decode(&[]).is_err());
    }

    #[test]
    fn insert_restart_batch_delete_and_exclusive_lock() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("spool.redb");
        let event = event();
        let key;
        {
            let spool = Spool::open(&path, MIN_SPOOL_BYTES).unwrap();
            key = spool.insert(&event).unwrap();
            assert!(Spool::open(&path, MIN_SPOOL_BYTES).is_err());
            assert_eq!(spool.stats().unwrap().pending, 1);
        }
        let spool = Spool::open(&path, MIN_SPOOL_BYTES).unwrap();
        let entries = spool.batch(1, false).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].key, key);
        assert_eq!(entries[0].event, event);
        spool.delete(key).unwrap();
        spool.delete(key).unwrap();
        assert_eq!(spool.stats().unwrap().used_bytes, 0);
        assert!(spool.batch(10, false).unwrap().is_empty());
    }

    #[test]
    fn corrupt_record_is_quarantined_without_blocking_good_records() {
        let dir = tempfile::tempdir().unwrap();
        let spool = Spool::open(&dir.path().join("spool.redb"), MIN_SPOOL_BYTES).unwrap();
        let event = event();
        let bad_key = spool.insert(&event).unwrap();
        let good = spool.insert(&event).unwrap();
        let before = spool.stats().unwrap().used_bytes;
        {
            let tx = spool.db.begin_write().unwrap();
            let mut table = tx.open_table(PENDING).unwrap();
            let mut bad = table.get(bad_key).unwrap().unwrap().value().to_vec();
            bad[HEADER] ^= 1;
            table.insert(bad_key, bad.as_slice()).unwrap();
            drop(table);
            tx.commit().unwrap();
        }
        let entries = spool.batch(10, false).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].key, good);
        let stats = spool.stats().unwrap();
        assert_eq!((stats.pending, stats.quarantined), (1, 1));
        assert_eq!(stats.used_bytes, before);
    }

    #[test]
    fn admission_limit_and_file_cap_survive_churn() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("spool.redb");
        let spool = Spool::open(&path, MIN_SPOOL_BYTES).unwrap();
        for _ in 0..3 {
            let mut keys = Vec::new();
            loop {
                match spool.insert(&event()) {
                    Ok(key) => keys.push(key),
                    Err(error) => {
                        assert!(matches!(error, Error::Full), "{error}");
                        break;
                    }
                }
            }
            assert!(!keys.is_empty());
            assert!(spool.stats().unwrap().used_bytes <= spool.stats().unwrap().capacity);
            assert!(std::fs::metadata(&path).unwrap().len() <= MIN_SPOOL_BYTES);
            let first = spool.batch(2, false).unwrap();
            let last = spool.batch(2, true).unwrap();
            assert_eq!(first[0].key, keys[0]);
            assert_eq!(last[0].key, *keys.last().unwrap());
            for key in keys {
                spool.delete(key).unwrap();
            }
        }
    }

    #[test]
    fn backend_refuses_physical_overflow() {
        let file = tempfile::tempfile().unwrap();
        let capped = CappedFile { inner: FileBackend::new(file).unwrap(), max: 1024 };
        capped.set_len(1024).unwrap();
        assert!(capped.set_len(1025).is_err());
        assert!(capped.write(1024, &[1]).is_err());
        assert_eq!(capped.len().unwrap(), 1024);
    }
}
