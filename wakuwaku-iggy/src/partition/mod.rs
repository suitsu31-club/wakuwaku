//! Iggy-level partitioning of events.
//!
//! - A [`PartitionKey`] type owns one *super partition*: the Iggy topic
//!   ([`IggySuperPartitionName`]) carrying every event keyed by it.
//! - [`key_hash`] maps a key to the 64-bit value that picks the Iggy partition
//!   and scopes ordering.
//! - [`plan`] reorders a polled batch by key, keeping only the order the
//!   events' [`EventAtomicOrdering`](crate::events::EventAtomicOrdering)
//!   requires.
//! - [`algebra`] folds runs of same-key events before they are handled.

pub mod algebra;
pub mod plan;

use std::hash::{Hash, Hasher};

/// Name of the Iggy topic that carries every event of a key type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IggySuperPartitionName(pub &'static str);

/// A key that routes events to a super partition.
///
/// Every event type whose [`Event::Key`](crate::events::Event::Key) is this
/// type goes to the same main topic, and events with equal keys go to the same
/// partition of it.
pub trait PartitionKey: Hash {
    /// Hasher that turns a key into its 64-bit [wire key](key_hash).
    ///
    /// Must give the same result in every publishing process, or events of one
    /// key may land in different partitions.
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
