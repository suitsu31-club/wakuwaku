#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IggySuperPartitionName(pub &'static str);

pub trait PartitionKey: std::hash::Hash {
    type PartitionMerge: std::hash::Hasher;
    const SUPER_PARTITION_NAME: IggySuperPartitionName;
}
