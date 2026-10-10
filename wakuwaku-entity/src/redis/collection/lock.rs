pub struct Lock {}

impl Lock {
    pub const PREFIX: &str = concat!(COLLECTION_PREFIX, "Lock:");
}

pub trait LockIdentifier {}
