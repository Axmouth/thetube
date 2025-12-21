use crate::*;
use fibril_util::unix_millis;

use async_trait::async_trait;
use rocksdb::{
    BoundColumnFamily, ColumnFamilyDescriptor, DBWithThreadMode, IteratorMode, MultiThreaded,
    Options, WriteBatch, WriteOptions,
};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct RocksStorage {
    db: Arc<DBWithThreadMode<MultiThreaded>>,
    sync_write: bool,
}


// TODO: Use spawn blocking or equivalent?
impl RocksStorage {
    const META_NEXT_EXPIRY_TS: &'static [u8] = b"NEXT_EXPIRY_TS"; // global hint (UnixMillis)

    fn write_opts(&self) -> WriteOptions {
        let mut write_opts = WriteOptions::default();
        write_opts.set_sync(self.sync_write); // fsync WAL before returning
        write_opts
    }

    pub fn open(path: &str, sync_write: bool) -> Result<Self, StorageError> {
        let cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);

        let bg_jobs = match cpus {
            0..=2 => 2,
            3..=4 => 4,
            5..=8 => 6,
            _ => 8,
        };

        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        // TODO: based on num CPUs?
        opts.set_max_background_jobs(bg_jobs);
        opts.set_enable_pipelined_write(true);
        // TODO: Configurable?
        opts.set_write_buffer_size(128 * 1024 * 1024); // 64MB
        opts.set_max_write_buffer_number(3);
        opts.set_min_write_buffer_number_to_merge(2);

        let mut block_opts = rocksdb::BlockBasedOptions::default();
        block_opts.set_bloom_filter(10.0, false);
        block_opts.set_cache_index_and_filter_blocks(true);
        block_opts.set_pin_l0_filter_and_index_blocks_in_cache(true);

        opts.set_block_based_table_factory(&block_opts);

        // If key layout is stable:
        // TODO: Might be doable if we make a map from names to stable length IDs here.
        // const PREFIX_LEN: usize = topic.len() + 1 + 4, // topic + 0 + partition;
        // opts.set_prefix_extractor(
        //     rocksdb::SliceTransform::create_fixed_prefix(PREFIX_LEN)
        // );

        let cfs = vec![
            ColumnFamilyDescriptor::new("messages", Options::default()),
            ColumnFamilyDescriptor::new("inflight", Options::default()),
            ColumnFamilyDescriptor::new("acked", Options::default()),
            ColumnFamilyDescriptor::new("meta", Options::default()),
            ColumnFamilyDescriptor::new("groups", Options::default()),
        ];

        let db = DBWithThreadMode::open_cf_descriptors(&opts, path, cfs)?;
        let storage = Self { db: Arc::new(db), sync_write };

        Ok(storage)
    }

    fn next_offset_key(topic: &Topic, partition: LogId) -> String {
        format!("NEXT_OFFSET:{}:{}", topic, partition)
    }

    fn encode_msg_key(topic: &Topic, partition: LogId, offset: Offset) -> Vec<u8> {
        let mut v = Vec::new();
        v.extend_from_slice(topic.as_bytes());
        v.push(0);
        v.extend_from_slice(&partition.to_be_bytes());
        v.extend_from_slice(&offset.to_be_bytes());
        v
    }

    fn encode_group_key(
        topic: &Topic,
        partition: LogId,
        group: &Group,
        offset: Offset,
    ) -> Vec<u8> {
        let mut v = Vec::new();
        v.extend_from_slice(topic.as_bytes());
        v.push(0);
        v.extend_from_slice(&partition.to_be_bytes());
        v.extend_from_slice(group.as_bytes());
        v.push(0);
        v.extend_from_slice(&offset.to_be_bytes());
        v
    }

    #[inline]
    fn cf(&self, name: &'static str) -> Result<Arc<BoundColumnFamily<'_>>, StorageError> {
        self.db
            .cf_handle(name)
            .ok_or(StorageError::MissingColumnFamily(name))
    }

    #[inline]
    fn be_u64(bytes: &[u8], ctx: &str) -> Result<u64, StorageError> {
        let arr: [u8; 8] = bytes.try_into().map_err(|_| {
            StorageError::KeyDecode(format!("{ctx}: expected 8 bytes, got {}", bytes.len()))
        })?;
        Ok(u64::from_be_bytes(arr))
    }

    #[inline]
    fn be_u32(bytes: &[u8], ctx: &str) -> Result<u32, StorageError> {
        let arr: [u8; 4] = bytes.try_into().map_err(|_| {
            StorageError::KeyDecode(format!("{ctx}: expected 4 bytes, got {}", bytes.len()))
        })?;
        Ok(u32::from_be_bytes(arr))
    }
    #[inline]
    async fn meta_get_u64(&self, key: &[u8]) -> Result<Option<u64>, StorageError> {
        let meta_cf = self.cf("meta")?;
        match self.db.get_cf(&meta_cf, key)? {
            Some(v) => Ok(Some(Self::be_u64(&v, "meta u64")?)),
            None => Ok(None),
        }
    }

    #[inline]
    fn meta_put_u64(
        batch: &mut WriteBatch,
        meta_cf: &Arc<BoundColumnFamily<'_>>,
        key: &[u8],
        v: u64,
    ) {
        batch.put_cf(meta_cf, key, v.to_be_bytes());
    }

    /// Best-effort "earliest deadline wins" update.
    /// Only moves the hint earlier (or sets it if missing).
    async fn maybe_advance_next_expiry_hint_batch(
        &self,
        batch: &mut WriteBatch,
        meta_cf: &Arc<BoundColumnFamily<'_>>,
        new_deadline: u64,
    ) -> Result<(), StorageError> {
        let cur = self.meta_get_u64(Self::META_NEXT_EXPIRY_TS).await?;
        if cur.is_none_or(|c| new_deadline < c) {
            Self::meta_put_u64(batch, meta_cf, Self::META_NEXT_EXPIRY_TS, new_deadline);
        }
        Ok(())
    }
}

#[async_trait]
impl Storage for RocksStorage {
    async fn append(
        &self,
        topic: &Topic,
        partition: LogId,
        payload: &[u8],
    ) -> Result<Offset, StorageError> {
        let meta_cf = self
            .db
            .cf_handle("meta")
            .ok_or(StorageError::MissingColumnFamily("meta"))?;
        let messages_cf = self
            .db
            .cf_handle("messages")
            .ok_or(StorageError::MissingColumnFamily("messages"))?;

        // Read next offset, default 0
        let key = Self::next_offset_key(topic, partition);
        let next = self.db.get_cf(&meta_cf, key.as_bytes())?;
        let offset = match &next {
            Some(v) => u64::from_be_bytes(v.as_slice().try_into().map_err(|_| {
                StorageError::KeyDecode(format!("invalid next offset length: {}", v.len()))
            })?),
            None => 0,
        };

        let msg_key = Self::encode_msg_key(topic, partition, offset);

        let mut batch = WriteBatch::default();
        batch.put_cf(&messages_cf, msg_key, payload);
        batch.put_cf(&meta_cf, key.as_bytes(), (offset + 1).to_be_bytes());
        self.db.write_opt(batch, &self.write_opts())?;

        Ok(offset)
    }

    async fn append_batch(
        &self,
        topic: &Topic,
        partition: LogId,
        payloads: &[Vec<u8>],
    ) -> Result<Vec<Offset>, StorageError> {
        if payloads.is_empty() {
            return Ok(Vec::new());
        }

        let meta_cf = self.cf("meta")?;
        let messages_cf = self.cf("messages")?;

        // Read next offset
        let next_key = Self::next_offset_key(topic, partition);
        let next = self.db.get_cf(&meta_cf, next_key.as_bytes())?;

        let mut offset = match next {
            Some(v) => Self::be_u64(&v, "next_offset")?,
            None => 0,
        };

        let mut batch = WriteBatch::default();
        let mut out = Vec::with_capacity(payloads.len());

        for payload in payloads {
            let msg_key = Self::encode_msg_key(topic, partition, offset);
            batch.put_cf(&messages_cf, msg_key, payload);
            out.push(offset);
            offset += 1;
        }

        // Update next_offset
        batch.put_cf(&meta_cf, next_key.as_bytes(), offset.to_be_bytes());

        self.db.write_opt(batch, &self.write_opts())?;

        Ok(out)
    }

    async fn register_group(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Group,
    ) -> Result<(), StorageError> {
        let groups_cf = self.cf("groups")?;

        let mut key = Vec::new();
        key.extend_from_slice(topic.as_bytes());
        key.push(0);
        key.extend_from_slice(&partition.to_be_bytes());
        key.extend_from_slice(group.as_bytes());

        self.db.put_cf(&groups_cf, key, [])?;
        Ok(())
    }

    async fn fetch_by_offset(
        &self,
        topic: &Topic,
        partition: LogId,
        offset: Offset,
    ) -> Result<StoredMessage, StorageError> {
        let messages_cf = self
            .db
            .cf_handle("messages")
            .ok_or(StorageError::MissingColumnFamily("messages"))?;

        let msg_key = RocksStorage::encode_msg_key(topic, partition, offset);

        let val = self
            .db
            .get_cf(&messages_cf, msg_key)?
            .ok_or(StorageError::MessageNotFound { offset })?;

        Ok(StoredMessage {
            topic: topic.clone(),
            partition,
            offset,
            timestamp: unix_millis(),
            payload: val.to_vec(),
        })
    }

    async fn fetch_available(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Group,
        from_offset: Offset,
        max: usize,
    ) -> Result<Vec<DeliverableMessage>, StorageError> {
        let messages_cf = self
            .db
            .cf_handle("messages")
            .ok_or(StorageError::MissingColumnFamily("messages"))?;
        let inflight_cf = self
            .db
            .cf_handle("inflight")
            .ok_or(StorageError::MissingColumnFamily("inflight"))?;
        let acked_cf = self
            .db
            .cf_handle("acked")
            .ok_or(StorageError::MissingColumnFamily("acked"))?;

        let start_key = Self::encode_msg_key(topic, partition, from_offset);

        let mut prefix = Vec::new();
        prefix.extend_from_slice(topic.as_bytes());
        prefix.push(0);
        prefix.extend_from_slice(&partition.to_be_bytes());

        let mut iter = self.db.iterator_cf(
            &messages_cf,
            IteratorMode::From(&start_key, rocksdb::Direction::Forward),
        );

        let mut out = Vec::new();

        for pair in iter.by_ref() {
            let (key, value) = pair?;
            if !key.starts_with(&prefix) {
                break;
            }

            if out.len() >= max {
                break;
            }

            let off = u64::from_be_bytes(key[key.len() - 8..].try_into().map_err(|e| {
                StorageError::KeyDecode(format!("invalid message key length: {}", e))
            })?);

            // Skip if inflight
            let inflight_key = Self::encode_group_key(topic, partition, group, off);
            if self
                .db
                .get_cf(&inflight_cf, inflight_key.clone())?
                .is_some()
            {
                continue;
            }

            // Skip if ACKed
            let acked_key = Self::encode_group_key(topic, partition, group, off);
            if self.db.get_cf(&acked_cf, acked_key.clone())?.is_some() {
                continue;
            }

            debug_assert!(
                self.db.get_cf(&acked_cf, &acked_key)?.is_none(),
                "fetch_available returning ACKed message {}",
                off
            );

            debug_assert!(
                self.db.get_cf(&inflight_cf, &inflight_key)?.is_none(),
                "fetch_available returned inflight message {}",
                off
            );

            out.push(DeliverableMessage {
                message: StoredMessage {
                    topic: topic.clone(),
                    partition,
                    offset: off,
                    timestamp: unix_millis(),
                    payload: value.to_vec(),
                },
                delivery_tag: off,
                group: group.clone(),
            });
        }

        Ok(out)
    }

    async fn current_next_offset(
        &self,
        topic: &Topic,
        partition: LogId,
    ) -> Result<Offset, StorageError> {
        let meta_cf = self
            .db
            .cf_handle("meta")
            .ok_or(StorageError::MissingColumnFamily("meta"))?;

        let key = Self::next_offset_key(topic, partition);

        match self.db.get_cf(&meta_cf, key.as_bytes())? {
            Some(v) => {
                let o = u64::from_be_bytes(v.as_slice().try_into().map_err(|_| {
                    StorageError::KeyDecode(format!("invalid next_offset for {}", key))
                })?);
                Ok(o)
            }
            None => Ok(0),
        }
    }

    async fn fetch_available_clamped(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Group,
        from_offset: Offset,
        max_offset_exclusive: Offset,
        max: usize,
    ) -> Result<Vec<DeliverableMessage>, StorageError> {
        if from_offset >= max_offset_exclusive {
            return Ok(vec![]);
        }

        let msgs = self
            .fetch_available(topic, partition, group, from_offset, max)
            .await?;

        // clamp based on next-offset snapshot
        let clamped = msgs
            .into_iter()
            .take_while(|m| m.delivery_tag < max_offset_exclusive)
            .collect();

        Ok(clamped)
    }

    async fn fetch_range(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Group,
        from_offset: Offset,
        to_offset: Offset,
        max: usize,
    ) -> Result<Vec<DeliverableMessage>, StorageError> {
        let messages_cf = self
            .db
            .cf_handle("messages")
            .ok_or(StorageError::MissingColumnFamily("messages"))?;
        let inflight_cf = self
            .db
            .cf_handle("inflight")
            .ok_or(StorageError::MissingColumnFamily("inflight"))?;
        let acked_cf = self
            .db
            .cf_handle("acked")
            .ok_or(StorageError::MissingColumnFamily("acked"))?;

        let start_key = Self::encode_msg_key(topic, partition, from_offset);

        let mut prefix = Vec::new();
        prefix.extend_from_slice(topic.as_bytes());
        prefix.push(0);
        prefix.extend_from_slice(&partition.to_be_bytes());

        let mut iter = self.db.iterator_cf(
            &messages_cf,
            IteratorMode::From(&start_key, rocksdb::Direction::Forward),
        );

        let mut out = Vec::new();

        for pair in iter.by_ref() {
            let (key, value) = pair?;
            if !key.starts_with(&prefix) {
                break;
            }

            if out.len() >= max {
                break;
            }

            let off = u64::from_be_bytes(key[key.len() - 8..].try_into().map_err(|e| {
                StorageError::KeyDecode(format!("invalid message key length: {}", e))
            })?);

            if off >= to_offset {
                break;
            }

            // Skip if inflight
            let inflight_key = Self::encode_group_key(topic, partition, group, off);
            if self
                .db
                .get_cf(&inflight_cf, inflight_key.clone())?
                .is_some()
            {
                continue;
            }

            // Skip if ACKed
            let acked_key = Self::encode_group_key(topic, partition, group, off);
            if self.db.get_cf(&acked_cf, acked_key.clone())?.is_some() {
                continue;
            }

            out.push(DeliverableMessage {
                message: StoredMessage {
                    topic: topic.clone(),
                    partition,
                    offset: off,
                    timestamp: unix_millis(),
                    payload: value.to_vec(),
                },
                delivery_tag: off,
                group: group.clone(),
            });
        }

        Ok(out)
    }

    async fn mark_inflight(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Group,
        offset: Offset,
        deadline_ts: UnixMillis,
    ) -> Result<(), StorageError> {
        let inflight_cf = self.cf("inflight")?;
        let meta_cf = self.cf("meta")?;

        let key = Self::encode_group_key(topic, partition, group, offset);

        let mut batch = WriteBatch::default();
        batch.put_cf(&inflight_cf, key, deadline_ts.to_be_bytes());

        self.maybe_advance_next_expiry_hint_batch(&mut batch, &meta_cf, deadline_ts)
            .await?;
        self.db.write_opt(batch, &self.write_opts())?;
        Ok(())
    }

    async fn mark_inflight_batch(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Group,
        entries: &[(Offset, UnixMillis)],
    ) -> Result<(), StorageError> {
        if entries.is_empty() {
            return Ok(());
        }

        let inflight_cf = self.cf("inflight")?;
        let meta_cf = self.cf("meta")?;

        let mut batch = WriteBatch::default();

        // Track min deadline in this batch
        let mut min_deadline = u64::MAX;

        for (offset, deadline) in entries {
            let key = Self::encode_group_key(topic, partition, group, *offset);
            batch.put_cf(&inflight_cf, key, deadline.to_be_bytes());
            min_deadline = min_deadline.min(*deadline);
        }

        self.maybe_advance_next_expiry_hint_batch(&mut batch, &meta_cf, min_deadline)
            .await?;
        self.db.write_opt(batch, &self.write_opts())?;
        Ok(())
    }

    async fn ack(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Group,
        offset: Offset,
    ) -> Result<(), StorageError> {
        let inflight_cf = self
            .db
            .cf_handle("inflight")
            .ok_or(StorageError::MissingColumnFamily("inflight"))?;
        let acked_cf = self
            .db
            .cf_handle("acked")
            .ok_or(StorageError::MissingColumnFamily("acked"))?;

        let key = Self::encode_group_key(topic, partition, group, offset);

        debug_assert!(
            self.db.get_cf(&acked_cf, &key)?.is_none(),
            "double ACK for offset {}",
            offset
        );

        debug_assert!(
            self.db.get_cf(&inflight_cf, &key)?.is_some(),
            "ACK on non-inflight message {}",
            offset
        );

        let key = Self::encode_group_key(topic, partition, group, offset);

        let mut batch = WriteBatch::default();
        batch.delete_cf(&inflight_cf, &key);
        batch.put_cf(&acked_cf, &key, []);
        self.db.write_opt(batch, &self.write_opts())?;

        Ok(())
    }

    async fn ack_batch(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Group,
        offsets: &[Offset],
    ) -> Result<(), StorageError> {
        if offsets.is_empty() {
            return Ok(());
        }

        let inflight_cf = self
            .db
            .cf_handle("inflight")
            .ok_or(StorageError::MissingColumnFamily("inflight"))?;
        let acked_cf = self
            .db
            .cf_handle("acked")
            .ok_or(StorageError::MissingColumnFamily("acked"))?;

        let mut batch = WriteBatch::default();

        // (Optional) If you ever send duplicates in the same batch, you can de-dupe.
        // For perf we can skip this.
        // let mut v = offsets.to_vec();
        // v.sort_unstable();
        // v.dedup();
        // for off in v { ... }

        for &offset in offsets {
            let key = Self::encode_group_key(topic, partition, group, offset);

            // Idempotent:
            // - delete inflight even if missing
            // - put acked even if already exists
            batch.delete_cf(&inflight_cf, &key);
            batch.put_cf(&acked_cf, &key, []);
        }

        self.db.write_opt(batch, &self.write_opts())?;
        Ok(())
    }

    async fn list_expired(&self, now_ts: u64) -> Result<Vec<DeliverableMessage>, StorageError> {
        let inflight_cf = self.cf("inflight")?;
        let messages_cf = self.cf("messages")?;

        let mut out = Vec::new();

        let iter = self
            .db
            .iterator_cf(&inflight_cf, rocksdb::IteratorMode::Start);
        for pair in iter {
            let (key, value) = pair?;
            let deadline = u64::from_be_bytes(boxed_slice_to_array(value)?);
            if deadline > now_ts {
                continue;
            }

            // Extract topic, partition, offset by decoding key
            let (topic, partition, group, offset) = decode_inflight_key(&key)?;

            let msg_key = RocksStorage::encode_msg_key(&topic, partition, offset);

            // Fetch the message
            let Some(msg_val) = self.db.get_cf(&messages_cf, msg_key)? else {
                // stale inflight entry pointing to a deleted/missing message
                // don't poison the whole scan
                // TODO: handle better?
                tracing::warn!(
                    "[WARN] stale inflight entry: topic={} partition={} group={} offset={} (message missing)",
                    topic, partition, group, offset
                );
                self.db.delete_cf(&inflight_cf, &key)?;
                continue;
            };

            out.push(DeliverableMessage {
                message: StoredMessage {
                    topic: topic.clone(),
                    partition,
                    offset,
                    timestamp: unix_millis(),
                    payload: msg_val.to_vec(),
                },
                delivery_tag: offset,
                group,
            });
        }

        Ok(out)
    }

    async fn lowest_unacked_offset(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Group,
    ) -> Result<Offset, StorageError> {
        let messages_cf = self.cf("messages")?;
        let acked_cf = self.cf("acked")?;
        let inflight_cf = self.cf("inflight")?;

        // prefix: topic + 0x00 + partition bytes
        let mut prefix = Vec::new();
        prefix.extend_from_slice(topic.as_bytes());
        prefix.push(0);
        prefix.extend_from_slice(&partition.to_be_bytes());

        let iter = self.db.iterator_cf(
            &messages_cf,
            rocksdb::IteratorMode::From(&prefix, rocksdb::Direction::Forward),
        );

        for pair in iter {
            let (key, _) = pair?;

            if !key.starts_with(&prefix) {
                break;
            }

            let offset = Self::be_u64(&key[key.len() - 8..], "message key offset")?;

            let inflight_key = Self::encode_group_key(topic, partition, group, offset);
            if self.db.get_cf(&inflight_cf, &inflight_key)?.is_some() {
                continue;
            }

            let ack_key = Self::encode_group_key(topic, partition, group, offset);
            if self.db.get_cf(&acked_cf, &ack_key)?.is_some() {
                continue;
            }

            return Ok(offset);
        }

        // No unacked messages remain -> everything is acked
        // Need to compute next_offset from meta CF
        let meta_cf = self.cf("meta")?;
        let next_key = Self::next_offset_key(topic, partition);
        let next = self.db.get_cf(&meta_cf, next_key.as_bytes())?;
        let next_offset = match next {
            Some(v) => Self::be_u64(&v, "next_offset")?,
            None => 0,
        };

        Ok(next_offset)
    }

    async fn cleanup_topic(&self, topic: &Topic, partition: LogId) -> Result<(), StorageError> {
        let messages_cf = self.cf("messages")?;

        // Find all groups for this topic/partition
        let groups_cf = self.cf("groups")?;
        let mut groups = Vec::new();

        // Build prefix for this topic + partition
        let mut prefix = Vec::new();
        prefix.extend_from_slice(topic.as_bytes());
        prefix.push(0);
        prefix.extend_from_slice(&partition.to_be_bytes());

        // Iterate groups CF looking for matching entries
        let iter = self.db.iterator_cf(
            &groups_cf,
            IteratorMode::From(&prefix, rocksdb::Direction::Forward),
        );

        for pair in iter {
            let (key, _) = pair?;

            if !key.starts_with(&prefix) {
                break;
            }

            // group name starts after prefix
            let group_bytes = &key[prefix.len()..];
            let g = String::from_utf8(group_bytes.to_vec())
                .map_err(|_| StorageError::Internal("invalid group".into()))?;

            groups.push(g);
        }

        // If no groups exist → no consumer ever → nothing can be deleted
        if groups.is_empty() {
            return Ok(());
        }

        // Compute min unacked offset across all groups
        let mut min_unacked = u64::MAX;

        for g in groups {
            let unacked = self.lowest_not_acked_offset(topic, partition, &g).await?;
            if unacked < min_unacked {
                min_unacked = unacked;
            }
        }

        if min_unacked == 0 {
            // nothing deletable
            return Ok(());
        }

        // Range delete messages < min_unacked
        let start_key = Self::encode_msg_key(topic, partition, 0);
        let end_key = Self::encode_msg_key(topic, partition, min_unacked);

        self.db.delete_range_cf(&messages_cf, start_key, end_key)?;

        Ok(())
    }

    async fn clear_inflight(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Group,
        offset: Offset,
    ) -> Result<(), StorageError> {
        let inflight_cf = self
            .db
            .cf_handle("inflight")
            .ok_or(StorageError::MissingColumnFamily("inflight"))?;
        let key = Self::encode_group_key(topic, partition, group, offset);

        self.db.delete_cf(&inflight_cf, key)?;
        Ok(())
    }

    async fn clear_all_inflight(&self) -> Result<(), StorageError> {
        // Drop and re-create CF
        self.db.drop_cf("inflight")?;
        self.db.create_cf("inflight", &Options::default())?;

        if let Ok(meta_cf) = self.cf("meta") {
            self.db.delete_cf(&meta_cf, Self::META_NEXT_EXPIRY_TS)?;
        }

        Ok(())
    }

    async fn count_inflight(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Group,
    ) -> Result<usize, StorageError> {
        let inflight_cf = self
            .db
            .cf_handle("inflight")
            .ok_or(StorageError::MissingColumnFamily("inflight"))?;

        // Build prefix = topic + 0 + partition + group + 0
        let mut prefix = Vec::new();
        prefix.extend_from_slice(topic.as_bytes());
        prefix.push(0);
        prefix.extend_from_slice(&partition.to_be_bytes());
        prefix.extend_from_slice(group.as_bytes());
        prefix.push(0);

        let iter = self.db.iterator_cf(
            &inflight_cf,
            IteratorMode::From(&prefix, rocksdb::Direction::Forward),
        );

        let mut count = 0usize;

        for pair in iter {
            let (key, _) = pair?;
            if !key.starts_with(&prefix) {
                break;
            }
            count += 1;
        }

        Ok(count)
    }

    async fn dump_meta_keys(&self) {
        if let Ok(meta_cf) = self.cf("meta") {
            let iter = self.db.iterator_cf(&meta_cf, IteratorMode::Start);
            for (key, _) in iter.flatten() {
                tracing::info!("META KEY = {:?}", String::from_utf8_lossy(&key));
            }
        }
    }

    async fn is_inflight_or_acked(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Group,
        offset: Offset,
    ) -> Result<bool, StorageError> {
        let inflight_cf = self
            .db
            .cf_handle("inflight")
            .ok_or(StorageError::MissingColumnFamily("inflight"))?;
        let acked_cf = self
            .db
            .cf_handle("acked")
            .ok_or(StorageError::MissingColumnFamily("acked"))?;

        let key = Self::encode_group_key(topic, partition, group, offset);

        if self.db.get_cf(&acked_cf, &key)?.is_some() {
            return Ok(true);
        }

        if self.db.get_cf(&inflight_cf, &key)?.is_some() {
            return Ok(true);
        }

        Ok(false)
    }

    async fn is_acked(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Group,
        offset: Offset,
    ) -> Result<bool, StorageError> {
        let acked_cf = self
            .db
            .cf_handle("acked")
            .ok_or(StorageError::MissingColumnFamily("acked"))?;

        let key = Self::encode_group_key(topic, partition, group, offset);

        if self.db.get_cf(&acked_cf, &key)?.is_some() {
            return Ok(true);
        }

        Ok(false)
    }

    async fn list_topics(&self) -> Result<Vec<Topic>, StorageError> {
        let groups_cf = self
            .db
            .cf_handle("groups")
            .ok_or(StorageError::MissingColumnFamily("groups"))?;

        let iter = self.db.iterator_cf(&groups_cf, IteratorMode::Start);

        let mut topics = std::collections::HashSet::new();

        for pair in iter {
            let (key, _) = pair?;

            // key format: topic \0 partition(4) group
            let mut i = 0;
            while i < key.len() && key[i] != 0 {
                i += 1;
            }

            if i == 0 || i >= key.len() {
                continue;
            }

            let topic = String::from_utf8(key[..i].to_vec())
                .map_err(|_| StorageError::KeyDecode("invalid topic".into()))?;

            topics.insert(topic);
        }

        Ok(topics.into_iter().collect())
    }

    async fn list_groups(&self) -> Result<Vec<(Topic, LogId, Group)>, StorageError> {
        let groups_cf = self
            .db
            .cf_handle("groups")
            .ok_or(StorageError::MissingColumnFamily("groups"))?;

        let iter = self.db.iterator_cf(&groups_cf, IteratorMode::Start);

        let mut out = Vec::new();

        for pair in iter {
            let (key, _) = pair?;

            // find topic terminator
            let Some(zero) = key.iter().position(|&b| b == 0) else {
                continue;
            };

            if zero + 1 + 4 > key.len() {
                continue;
            }

            let topic = String::from_utf8(key[..zero].to_vec())
                .map_err(|_| StorageError::KeyDecode("invalid topic".into()))?;

            let partition = Self::be_u32(&key[zero + 1..zero + 5], "partition")?;

            let group = String::from_utf8(key[zero + 5..].to_vec())
                .map_err(|_| StorageError::KeyDecode("invalid group".into()))?;

            out.push((topic, partition, group));
        }

        Ok(out)
    }

    async fn lowest_not_acked_offset(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Group,
    ) -> Result<Offset, StorageError> {
        let messages_cf = self.cf("messages")?;
        let acked_cf = self.cf("acked")?;

        let mut prefix = Vec::new();
        prefix.extend_from_slice(topic.as_bytes());
        prefix.push(0);
        prefix.extend_from_slice(&partition.to_be_bytes());

        let iter = self.db.iterator_cf(
            &messages_cf,
            rocksdb::IteratorMode::From(&prefix, rocksdb::Direction::Forward),
        );

        for pair in iter {
            let (key, _) = pair?;
            if !key.starts_with(&prefix) {
                break;
            }

            let offset = RocksStorage::be_u64(&key[key.len() - 8..], "offset")?;
            let ack_key = Self::encode_group_key(topic, partition, group, offset);

            if self.db.get_cf(&acked_cf, &ack_key)?.is_some() {
                continue;
            }

            // first offset that is NOT acked (could be inflight or never delivered)
            return Ok(offset);
        }

        // everything acked => return next_offset
        let meta_cf = self.cf("meta")?;
        let next_key = Self::next_offset_key(topic, partition);
        let next = self.db.get_cf(&meta_cf, next_key.as_bytes())?;
        Ok(match next {
            Some(v) => Self::be_u64(&v, "next")?,
            None => 0,
        })
    }

    async fn flush(&self) -> Result<(), StorageError> {
        self.db.flush()?;
        Ok(())
    }

    async fn next_expiry_hint(&self) -> Result<Option<u64>, StorageError> {
        self.meta_get_u64(Self::META_NEXT_EXPIRY_TS).await
    }

    async fn recompute_and_store_next_expiry_hint(&self) -> Result<Option<u64>, StorageError> {
        let inflight_cf = self.cf("inflight")?;
        let meta_cf = self.cf("meta")?;

        let mut min_deadline: Option<u64> = None;

        let iter = self.db.iterator_cf(&inflight_cf, IteratorMode::Start);
        for pair in iter {
            let (_key, value) = pair?;
            let deadline = Self::be_u64(&value, "inflight deadline")?;
            min_deadline = Some(match min_deadline {
                None => deadline,
                Some(cur) => cur.min(deadline),
            });
        }

        let mut batch = WriteBatch::default();
        if let Some(d) = min_deadline {
            Self::meta_put_u64(&mut batch, &meta_cf, Self::META_NEXT_EXPIRY_TS, d);
        } else {
            // No inflight → remove hint (or set to 0 if you prefer)
            batch.delete_cf(&meta_cf, Self::META_NEXT_EXPIRY_TS);
        }
        self.db.write_opt(batch, &self.write_opts())?;
        Ok(min_deadline)
    }

    // TODO: Better timestamp support
    // Stored messages currently use timestamp: 0.
    // Eventually we want:
    // broker receipt time
    // producer timestamp (optional)
    // Stub: keep as zero or SystemTime::now()

    // TODO: . Message metadata
    // Even just:
    // content_type
    // headers
    // priority
    // ttl

    // TODO: Delete inflight entries on redelivery

    // Storage correctly identifies expired inflight messages but does NOT delete them.
    // Usually the broker layer does that, not the storage layer.
    // Meaning:
    // - list_expired() should not delete
    // - Broker should delete inflight entries and then re-queue for delivery
    // This is correct.

    // TODO: Consistent snapshots or checkpointing (for future clustering)
    // For replication we will eventually need:
    // durable metadata for consumer groups
    // durable write-ahead-log boundaries (segment numbers)
    // support for Raft log entry application
    // Stub: no action needed now, but design broker around explicit "state applies".
}

fn decode_inflight_key(key: &[u8]) -> Result<(Topic, LogId, Group, Offset), StorageError> {
    // topic: up to first 0
    let Some(z1) = key.iter().position(|&b| b == 0) else {
        return Err(StorageError::KeyDecode(
            "inflight key missing topic terminator".into(),
        ));
    };
    let topic = String::from_utf8(key[..z1].to_vec())
        .map_err(|_| StorageError::KeyDecode("invalid topic utf8".into()))?;

    // skip delimiter
    let p_start = z1 + 1;
    // partition: 4 bytes
    let p_end = p_start + 4;
    if p_end > key.len() {
        return Err(StorageError::KeyDecode(
            "inflight key missing partition".into(),
        ));
    }
    debug_assert!(p_start < p_end);
    let partition = RocksStorage::be_u32(&key[p_start..p_end], "partition")?;

    // group: up to next 0
    let g_start = p_end;
    let Some(z2_rel) = key[g_start..].iter().position(|&b| b == 0) else {
        return Err(StorageError::KeyDecode(
            "inflight key missing group terminator".into(),
        ));
    };
    let z2 = g_start + z2_rel;

    debug_assert!(g_start < z2);
    let group = String::from_utf8(key[g_start..z2].to_vec())
        .map_err(|_| StorageError::KeyDecode("invalid group utf8".into()))?;

    let o_start = z2 + 1;
    // offset: last 8 bytes
    let o_end = o_start + 8;
    if o_end > key.len() {
        return Err(StorageError::KeyDecode(
            "inflight key missing offset".into(),
        ));
    }
    debug_assert!(o_start < o_end);
    let offset = RocksStorage::be_u64(&key[o_start..o_end], "offset")?;

    Ok((topic, partition, group, offset))
}

#[derive(Debug)]
pub struct BoxedSliceToArrayError {
    expected: usize,
    found: usize,
}

impl std::fmt::Display for BoxedSliceToArrayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "expected slice of length {}, got {}",
            self.expected, self.found
        )
    }
}

impl std::error::Error for BoxedSliceToArrayError {}

impl From<BoxedSliceToArrayError> for StorageError {
    fn from(e: BoxedSliceToArrayError) -> Self {
        StorageError::KeyDecode(e.to_string())
    }
}

fn boxed_slice_to_array<const N: usize>(b: Box<[u8]>) -> Result<[u8; N], BoxedSliceToArrayError> {
    if b.len() != N {
        return Err(BoxedSliceToArrayError {
            expected: N,
            found: b.len(),
        });
    }

    let mut arr = [0u8; N];
    arr.copy_from_slice(&b); // length checked above, so no panic
    Ok(arr)
}
