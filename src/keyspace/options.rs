// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    config::{
        BlockSizePolicy, BloomConstructionPolicy, CompressionPolicy, FilterPolicy,
        FilterPolicyEntry, HashRatioPolicy, PartioningPolicy, PinningPolicy, RestartIntervalPolicy,
    },
    keyspace::{config::DecodeConfig, InternalKeyspaceId},
    meta_keyspace::{encode_config_key, MetaKeyspace},
};
use byteorder::ReadBytesExt;
use lsm_tree::{CompressionType, KvPair, KvSeparationOptions};
use std::sync::Arc;

/// Options to configure a keyspace
#[allow(clippy::module_name_repetitions)]
#[derive(Clone)]
pub struct CreateOptions {
    /// Amount of levels of the LSM tree (depth of tree).
    #[allow(unused)]
    pub(crate) level_count: u8,

    /// Maximum size of this keyspace's memtable - can be changed during runtime
    pub(crate) max_memtable_size: u64,

    /// Data block hash ratio
    pub data_block_hash_ratio_policy: HashRatioPolicy,

    /// Block size of data blocks.
    #[doc(hidden)]
    pub data_block_size_policy: BlockSizePolicy,

    #[doc(hidden)]
    pub data_block_restart_interval_policy: RestartIntervalPolicy,

    #[doc(hidden)]
    pub index_block_restart_interval_policy: RestartIntervalPolicy,

    #[doc(hidden)]
    pub index_block_pinning_policy: PinningPolicy,

    #[doc(hidden)]
    pub filter_block_pinning_policy: PinningPolicy,

    #[doc(hidden)]
    pub filter_block_partitioning_policy: PartioningPolicy,

    #[doc(hidden)]
    pub index_block_partitioning_policy: PartioningPolicy,

    /// If `true`, the last level will not build filters, reducing the filter size of a database
    /// by ~90% typically.
    #[doc(hidden)]
    pub expect_point_read_hits: bool,

    /// Filter construction policy.
    #[doc(hidden)]
    pub filter_policy: FilterPolicy,

    /// Compression to use for data blocks.
    #[doc(hidden)]
    pub data_block_compression_policy: CompressionPolicy,

    /// Compression to use for index blocks.
    #[doc(hidden)]
    pub index_block_compression_policy: CompressionPolicy,

    pub(crate) manual_journal_persist: bool,

    #[doc(hidden)]
    pub compaction_strategy: Arc<dyn lsm_tree::compaction::CompactionStrategy + Send + Sync>,

    #[doc(hidden)]
    pub kv_separation_opts: Option<KvSeparationOptions>,
}

impl Default for CreateOptions {
    fn default() -> Self {
        let default_tree_config = lsm_tree::Config::default();

        Self {
            manual_journal_persist: false,

            max_memtable_size: /* 64 MiB */ 64 * 1_024 * 1_024,

            data_block_hash_ratio_policy: HashRatioPolicy::all(0.0),

            data_block_size_policy: BlockSizePolicy::all(/* 4 KiB */ 4 * 1_024),

            data_block_restart_interval_policy:  RestartIntervalPolicy::all(16),
            index_block_restart_interval_policy:  RestartIntervalPolicy::all(1),

            index_block_pinning_policy: PinningPolicy::new([true, true, false]),
            filter_block_pinning_policy: PinningPolicy::new([true, false]),

            index_block_partitioning_policy: PinningPolicy::new([false, false, false, true]),
            filter_block_partitioning_policy: PinningPolicy::new([false, false, false, true]),

            expect_point_read_hits: false,

            filter_policy: FilterPolicy::new(&[
                FilterPolicyEntry::Bloom(BloomConstructionPolicy::FalsePositiveRate(0.0001)),
                FilterPolicyEntry::Bloom(BloomConstructionPolicy::BitsPerKey(10.0)),
            ]),

            level_count: default_tree_config.level_count,

            #[cfg(feature = "lz4")]
            data_block_compression_policy: CompressionPolicy::new([CompressionType::None, CompressionType::None, CompressionType::Lz4]),

            #[cfg(not(feature = "lz4"))]
            data_block_compression_policy: CompressionPolicy::new(&[CompressionType::None]),

            index_block_compression_policy: CompressionPolicy::all(CompressionType::None),

            compaction_strategy: Arc::new(
                crate::compaction::Leveled::default()
            ),

            kv_separation_opts: None,
        }
    }
}

macro_rules! policy {
    ($keyspace_id:expr, $name:expr, $field:expr) => {{
        let key = encode_config_key($keyspace_id, $name);
        (key.into(), $field.encode())
    }};
}

impl CreateOptions {
    #[allow(clippy::expect_used, clippy::too_many_lines)]
    pub(crate) fn from_kvs(
        keyspace_id: InternalKeyspaceId,
        meta_keyspace: &MetaKeyspace,
    ) -> crate::Result<Self> {
        let blob = meta_keyspace.get_kv_for_config(keyspace_id, "blob")?;

        let data_block_compression_policy = meta_keyspace
            .get_kv_for_config(keyspace_id, "data_block_compression_policy")?
            .expect("should exist");
        let data_block_compression_policy =
            CompressionPolicy::decode(&data_block_compression_policy);

        let index_block_compression_policy = meta_keyspace
            .get_kv_for_config(keyspace_id, "index_block_compression_policy")?
            .expect("should exist");
        let index_block_compression_policy =
            CompressionPolicy::decode(&index_block_compression_policy);

        let data_block_size_policy = meta_keyspace
            .get_kv_for_config(keyspace_id, "data_block_size_policy")?
            .expect("should exist");
        let data_block_size_policy = BlockSizePolicy::decode(&data_block_size_policy);

        let filter_block_partitioning_policy = meta_keyspace
            .get_kv_for_config(keyspace_id, "filter_block_partitioning_policy")?
            .expect("should exist");
        let filter_block_partitioning_policy =
            PinningPolicy::decode(&filter_block_partitioning_policy);

        let index_block_partitioning_policy = meta_keyspace
            .get_kv_for_config(keyspace_id, "index_block_partitioning_policy")?
            .expect("should exist");
        let index_block_partitioning_policy =
            PinningPolicy::decode(&index_block_partitioning_policy);

        let filter_block_pinning_policy = meta_keyspace
            .get_kv_for_config(keyspace_id, "filter_block_pinning_policy")?
            .expect("should exist");
        let filter_block_pinning_policy = PinningPolicy::decode(&filter_block_pinning_policy);

        let index_block_pinning_policy = meta_keyspace
            .get_kv_for_config(keyspace_id, "index_block_pinning_policy")?
            .expect("should exist");
        let index_block_pinning_policy = PinningPolicy::decode(&index_block_pinning_policy);

        let data_block_restart_interval_policy = meta_keyspace
            .get_kv_for_config(keyspace_id, "data_block_restart_interval_policy")?
            .expect("should exist");
        let data_block_restart_interval_policy =
            RestartIntervalPolicy::decode(&data_block_restart_interval_policy);

        let index_block_restart_interval_policy = meta_keyspace
            .get_kv_for_config(keyspace_id, "index_block_restart_interval_policy")?
            .expect("should exist");
        let index_block_restart_interval_policy =
            RestartIntervalPolicy::decode(&index_block_restart_interval_policy);

        let data_block_hash_ratio_policy = meta_keyspace
            .get_kv_for_config(keyspace_id, "data_block_hash_ratio_policy")?
            .expect("should exist");
        let data_block_hash_ratio_policy = HashRatioPolicy::decode(&data_block_hash_ratio_policy);

        let expect_point_read_hits = meta_keyspace
            .get_kv_for_config(keyspace_id, "expect_point_read_hits")?
            .expect("should exist");
        let expect_point_read_hits = expect_point_read_hits == [1];

        let filter_policy = meta_keyspace
            .get_kv_for_config(keyspace_id, "filter_policy")?
            .expect("should exist");
        let filter_policy = FilterPolicy::decode(&filter_policy);

        let blob_opts = blob.map(|_| {
            use byteorder::LE;
            use lsm_tree::coding::Decode;

            let blob_age_cutoff = meta_keyspace
                .get_kv_for_config(keyspace_id, "blob_age_cutoff")?
                .expect("blob_age_cutoff should be defined");
            let blob_age_cutoff = (&mut &blob_age_cutoff[..]).read_f32::<LE>()?;

            let blob_compression = meta_keyspace
                .get_kv_for_config(keyspace_id, "blob_compression")?
                .expect("blob_compression should be defined");
            let blob_compression = CompressionType::decode_from(&mut &blob_compression[..])?;

            let file_target_size = meta_keyspace
                .get_kv_for_config(keyspace_id, "blob_file_target_size")?
                .expect("blob_file_target_size should be defined");
            let file_target_size = (&mut &file_target_size[..]).read_u64::<LE>()?;

            let separation_threshold = meta_keyspace
                .get_kv_for_config(keyspace_id, "blob_separation_threshold")?
                .expect("blob_separation_threshold should be defined");
            let separation_threshold = (&mut &separation_threshold[..]).read_u32::<LE>()?;

            let staleness_threshold = meta_keyspace
                .get_kv_for_config(keyspace_id, "blob_staleness_threshold")?
                .expect("blob_staleness_threshold should be defined");
            let staleness_threshold = (&mut &staleness_threshold[..]).read_f32::<LE>()?;

            Ok::<_, crate::Error>(
                KvSeparationOptions::default()
                    .compression(blob_compression)
                    .file_target_size(file_target_size)
                    .separation_threshold(separation_threshold)
                    .staleness_threshold(staleness_threshold)
                    .age_cutoff(blob_age_cutoff),
            )
        });

        let compaction_strategy_name = meta_keyspace
            .get_kv_for_config(keyspace_id, "compaction_strategy")?
            .expect("compaction_strategy should be defined");

        let compaction_strategy_name = std::str::from_utf8(&compaction_strategy_name)
            .expect("compaction_strategy should be UTF-8");

        let compaction_strategy = match compaction_strategy_name {
            lsm_tree::compaction::LEVELED_COMPACTION_NAME => {
                use byteorder::LE;

                let l0_threshold = meta_keyspace
                    .get_kv_for_config(keyspace_id, "leveled_l0_threshold")?
                    .expect("leveled_l0_threshold should be defined");
                let l0_threshold = (&mut &l0_threshold[..]).read_u8()?;

                let target_size = meta_keyspace
                    .get_kv_for_config(keyspace_id, "leveled_target_size")?
                    .expect("leveled_target_size should be defined");
                let target_size = (&mut &target_size[..]).read_u64::<LE>()?;

                let level_ratio_policy_bytes = meta_keyspace
                    .get_kv_for_config(keyspace_id, "leveled_level_ratio_policy")?
                    .expect("leveled_level_ratio_policy should be defined");
                let level_ratio_policy_bytes = &mut &level_ratio_policy_bytes[..];

                let level_ratio_policy_len = level_ratio_policy_bytes.read_u8()?;

                let mut ratios = vec![];

                for _ in 0..level_ratio_policy_len {
                    ratios.push(level_ratio_policy_bytes.read_f32::<LE>()?);
                }

                Arc::new(
                    crate::compaction::Leveled::default()
                        .with_l0_threshold(l0_threshold)
                        .with_table_target_size(target_size)
                        .with_level_ratio_policy(ratios),
                ) as Arc<dyn lsm_tree::compaction::CompactionStrategy + Send + Sync>
            }
            lsm_tree::compaction::FIFO_COMPACTION_NAME => {
                use byteorder::LE;

                let fifo_limit = meta_keyspace
                    .get_kv_for_config(keyspace_id, "fifo_limit")?
                    .expect("fifo_limit should be defined");
                let fifo_limit = (&mut &fifo_limit[..]).read_u64::<LE>()?;

                let has_ttl = meta_keyspace
                    .get_kv_for_config(keyspace_id, "fifo_ttl")?
                    .expect("fifo_ttl should be defined")
                    == [1];

                let ttl_seconds = if has_ttl {
                    let fifo_ttl_seconds = meta_keyspace
                        .get_kv_for_config(keyspace_id, "fifo_ttl_seconds")?
                        .expect("fifo_ttl_seconds should be defined");
                    let fifo_ttl_seconds = (&mut &fifo_ttl_seconds[..]).read_u64::<LE>()?;

                    Some(fifo_ttl_seconds)
                } else {
                    None
                };

                Arc::new(crate::compaction::Fifo::new(fifo_limit, ttl_seconds))
            }
            name => {
                panic!("Invalid/unsupported compaction strategy: {name:?}");
            }
        };

        Ok(Self {
            data_block_hash_ratio_policy,

            filter_block_partitioning_policy,
            index_block_partitioning_policy,

            filter_block_pinning_policy,
            index_block_pinning_policy,

            data_block_compression_policy,
            index_block_compression_policy,

            data_block_size_policy,

            data_block_restart_interval_policy,
            index_block_restart_interval_policy,

            expect_point_read_hits,
            filter_policy,

            level_count: 7, // TODO:

            manual_journal_persist: false, // TODO: 3.0.0

            max_memtable_size: 64_000_000, // TODO: 3.0.0

            compaction_strategy,

            kv_separation_opts: blob_opts.transpose()?,
        })
    }

    #[allow(clippy::too_many_lines)]
    pub(crate) fn encode_kvs(&self, keyspace_id: InternalKeyspaceId) -> Vec<KvPair> {
        use crate::keyspace::config::EncodeConfig;

        let mut kvs = vec![
            {
                let key = encode_config_key(keyspace_id, "compaction_strategy");
                (key, self.compaction_strategy.get_name().into())
            },
            policy!(
                keyspace_id,
                "data_block_compression_policy",
                self.data_block_compression_policy
            ),
            policy!(
                keyspace_id,
                "data_block_hash_ratio_policy",
                self.data_block_hash_ratio_policy
            ),
            policy!(
                keyspace_id,
                "data_block_restart_interval_policy",
                self.data_block_restart_interval_policy
            ),
            policy!(
                keyspace_id,
                "data_block_size_policy",
                self.data_block_size_policy
            ),
            {
                let key = encode_config_key(keyspace_id, "expect_point_read_hits");

                let value = (if self.expect_point_read_hits {
                    [1u8]
                } else {
                    [0u8]
                })
                .into();

                (key, value)
            },
            policy!(
                keyspace_id,
                "filter_block_partitioning_policy",
                self.filter_block_partitioning_policy
            ),
            policy!(
                keyspace_id,
                "filter_block_pinning_policy",
                self.filter_block_pinning_policy
            ),
            policy!(keyspace_id, "filter_policy", self.filter_policy),
            policy!(
                keyspace_id,
                "index_block_compression_policy",
                self.index_block_compression_policy
            ),
            policy!(
                keyspace_id,
                "index_block_partitioning_policy",
                self.index_block_partitioning_policy
            ),
            policy!(
                keyspace_id,
                "index_block_pinning_policy",
                self.index_block_pinning_policy
            ),
            policy!(
                keyspace_id,
                "index_block_restart_interval_policy",
                self.index_block_restart_interval_policy
            ),
            {
                let key = encode_config_key(keyspace_id, "version");
                (key, [3u8].into())
            },
        ];

        match self.compaction_strategy.get_name() {
            "LeveledCompaction" | "FifoCompaction" => {
                kvs.extend(
                    self.compaction_strategy
                        .get_config()
                        .into_iter()
                        .map(|(k, v)| (encode_config_key(keyspace_id, k), v)),
                );
            }
            name => {
                panic!("Invalid/unsupported compaction stratey: {name:?}");
            }
        }

        if let Some(blob_opts) = &self.kv_separation_opts {
            kvs.extend([
                {
                    let key = encode_config_key(keyspace_id, "blob");
                    (key, [1u8].into())
                },
                {
                    let key = encode_config_key(keyspace_id, "blob_age_cutoff");
                    (key, blob_opts.age_cutoff.to_le_bytes().into())
                },
                {
                    use lsm_tree::coding::Encode;

                    let key = encode_config_key(keyspace_id, "blob_compression");
                    (key, blob_opts.compression.encode_into_vec().into())
                },
                {
                    let key = encode_config_key(keyspace_id, "blob_file_target_size");
                    (key, blob_opts.file_target_size.to_le_bytes().into())
                },
                {
                    let key = encode_config_key(keyspace_id, "blob_separation_threshold");
                    (key, blob_opts.separation_threshold.to_le_bytes().into())
                },
                {
                    let key = encode_config_key(keyspace_id, "blob_staleness_threshold");
                    (key, blob_opts.staleness_threshold.to_le_bytes().into())
                },
            ]);
        }

        kvs
    }

    /// Toggles key-value separation.
    #[must_use]
    pub fn with_kv_separation(mut self, opts: Option<KvSeparationOptions>) -> Self {
        self.kv_separation_opts = opts;
        self
    }

    /// Sets the restart interval inside data blocks.
    ///
    /// A higher restart interval saves space while increasing lookup times
    /// inside data blocks.
    ///
    /// Default = 16
    #[must_use]
    pub fn data_block_restart_interval_policy(mut self, policy: RestartIntervalPolicy) -> Self {
        self.data_block_restart_interval_policy = policy;
        self
    }

    // TODO: not supported yet in lsm-tree
    // /// Sets the restart interval inside index blocks.
    // ///
    // /// A higher restart interval saves space while increasing lookup times
    // /// inside index blocks.
    // ///
    // /// Default = 1
    // #[must_use]
    // #[doc(hidden)]
    // pub fn index_block_restart_interval_policy(mut self, policy: RestartIntervalPolicy) -> Self {
    //     self.index_block_restart_interval_policy = policy;
    //     self
    // }

    /// Sets the pinning policy for filter blocks.
    ///
    /// By default, L0 filter blocks are pinned.
    #[must_use]
    pub fn filter_block_pinning_policy(mut self, policy: PinningPolicy) -> Self {
        self.filter_block_pinning_policy = policy;
        self
    }

    /// Sets the pinning policy for index blocks.
    ///
    /// By default, L0 and L1 index blocks are pinned.
    #[must_use]
    pub fn index_block_pinning_policy(mut self, policy: PinningPolicy) -> Self {
        self.index_block_pinning_policy = policy;
        self
    }

    /// TODO:
    #[must_use]
    pub fn filter_block_partitioning_policy(mut self, policy: PartioningPolicy) -> Self {
        self.filter_block_partitioning_policy = policy;
        self
    }

    /// TODO:
    #[must_use]
    pub fn index_block_partitioning_policy(mut self, policy: PartioningPolicy) -> Self {
        self.index_block_partitioning_policy = policy;
        self
    }

    /// Sets the hash ratio for the hash index in data blocks.
    ///
    /// The hash index speeds up point queries by using an embedded
    /// hash map in data blocks, but uses more space/memory.
    ///
    /// In-memory or heavily cached workloads benefit more from a higher hash ratio.
    ///
    /// If 0.0, the hash index is not constructed.
    #[must_use]
    #[doc(hidden)]
    pub fn data_block_hash_ratio_policy(mut self, policy: HashRatioPolicy) -> Self {
        self.data_block_hash_ratio_policy = policy;
        self
    }

    /// Sets the filter policy for data blocks.
    #[must_use]
    pub fn filter_policy(mut self, policy: FilterPolicy) -> Self {
        self.filter_policy = policy;
        self
    }

    /// If `true`, the last level will not build filters, reducing the filter size of a database
    /// by ~90% typically.
    ///
    /// **Enable this only if you know that point reads generally are expected to find a key-value pair.**
    #[must_use]
    pub fn expect_point_read_hits(mut self, b: bool) -> Self {
        self.expect_point_read_hits = b;
        self
    }

    /// Sets the compression policy for data blocks.
    #[must_use]
    pub fn data_block_compression_policy(mut self, policy: CompressionPolicy) -> Self {
        self.data_block_compression_policy = policy;
        self
    }

    /// Sets the compression policy for index blocks.
    #[must_use]
    pub fn index_block_compression_policy(mut self, policy: CompressionPolicy) -> Self {
        self.index_block_compression_policy = policy;
        self
    }

    /// Sets the compaction strategy.
    ///
    /// Default = Leveled
    #[must_use]
    pub fn compaction_strategy(
        mut self,
        compaction_strategy: Arc<dyn lsm_tree::compaction::CompactionStrategy + Send + Sync>,
    ) -> Self {
        self.compaction_strategy = compaction_strategy;
        self
    }

    /// If `false`, writes will flush data to the operating system.
    ///
    /// Default = false
    ///
    /// Set to `true` to handle persistence manually, e.g. manually using `PersistMode::SyncData`.
    #[must_use]
    pub fn manual_journal_persist(mut self, flag: bool) -> Self {
        self.manual_journal_persist = flag;
        self
    }

    /// Sets the maximum memtable size.
    ///
    /// Default = 64 MiB
    ///
    /// Recommended size 8 - 64 MiB, depending on how much memory
    /// is available.
    ///
    /// Conversely, if `max_memtable_size` is larger than 64 MiB,
    /// it may require increasing the database's `max_write_buffer_size`.
    #[must_use]
    pub fn max_memtable_size(mut self, bytes: u64) -> Self {
        self.max_memtable_size = bytes;
        self
    }

    /// Sets the block size.
    ///
    /// Once set for a keyspace, this property is not considered in the future.
    ///
    /// Default = 4 KiB
    ///
    /// For point read heavy workloads (get) a sensible default is
    /// somewhere between 4 - 8 KiB, depending on the average value size.
    ///
    /// For more space efficiency, block size between 16 - 64 KiB are sensible.
    ///
    /// # Panics
    ///
    /// Panics if the block size is smaller than 1 KiB or larger than 1 MiB.
    #[must_use]
    pub fn data_block_size_policy(mut self, policy: BlockSizePolicy) -> Self {
        self.data_block_size_policy = policy;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    #[cfg(not(feature = "lz4"))]
    fn keyspace_opts_compression_none() {
        let mut c = CreateOptions::default();
        assert_eq!(
            c.data_block_compression_policy,
            CompressionPolicy::disabled(),
        );
        assert_eq!(c.kv_separation_opts, None);

        c = c.with_kv_separation(KvSeparationOptions::default());
        assert_eq!(
            c.kv_separation_opts.as_ref().unwrap().compression,
            CompressionType::None,
        );

        c = c.data_block_compression_policy(CompressionPolicy::disabled());
        assert_eq!(
            c.data_block_compression_policy,
            CompressionPolicy::disabled(),
        );
        assert_eq!(
            c.kv_separation_opts.unwrap().compression,
            CompressionType::None,
        );
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    #[cfg(feature = "lz4")]
    fn keyspace_opts_compression_default() {
        use CompressionType::{Lz4, None as Uncompressed};

        let mut c = CreateOptions::default();
        assert_eq!(
            c.data_block_compression_policy,
            CompressionPolicy::new([Uncompressed, Uncompressed, Lz4]),
        );
        assert_eq!(c.kv_separation_opts, None);

        c = c.with_kv_separation(Some(KvSeparationOptions::default()));
        assert_eq!(c.kv_separation_opts.as_ref().unwrap().compression, Lz4);

        c = c.data_block_compression_policy(CompressionPolicy::disabled());
        assert_eq!(
            c.data_block_compression_policy,
            CompressionPolicy::disabled(),
        );
        assert_eq!(c.kv_separation_opts.as_ref().unwrap().compression, Lz4);
    }
}
