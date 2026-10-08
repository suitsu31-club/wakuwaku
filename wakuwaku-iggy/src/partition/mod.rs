//! Iggy-level partitioning of events: the super partition (main topic) of a
//! key type, key hashing, per-key reordering of a polled batch and the
//! algebra used to fold runs of events.

pub mod algebra;
pub mod plan;

use std::hash::{Hash, Hasher};

/// Name of the Iggy topic that carries every event of a key type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IggySuperPartitionName(pub &'static str);

pub trait PartitionKey: Hash {
    /// Hasher that turns a key into its 64-bit [wire key](key_hash).
    type PartitionMerge: Hasher + Default;
    /// The main topic. The stream comes from the consumer or publisher
    /// configuration.
    const SUPER_PARTITION_NAME: IggySuperPartitionName;
}

/// Hash `key` into the 64-bit value used as the message key, the planner key
/// and the partitioning key.
pub fn key_hash<K: PartitionKey>(key: &K) -> u64 {
    let mut hasher = K::PartitionMerge::default();
    key.hash(&mut hasher);
    hasher.finish()
}
