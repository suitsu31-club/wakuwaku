pub struct CacheLine<T: CacheBody> {
    pub body: T,
}

impl CacheLine {
    pub const PREFIX: &str = concat!(COLLECTION_PREFIX, "CacheLine:");
}

pub trait CacheBody {
    const CACHE_NAMESPACE: &'static str;
}
