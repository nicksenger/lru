use std::hash::{BuildHasher, Hash, Hasher, RandomState};
use std::num::NonZeroUsize;
use std::sync::mpsc;

use futures::channel::oneshot;

#[derive(Clone)]
pub struct Cache<Key, Value, Read, H> {
    txs: Vec<mpsc::Sender<Message<Key, Value, Read>>>,
    hash_builder: H,
    num_partitions: NonZeroUsize,
}

impl<Key, Value, Read> Cache<Key, Value, Read, RandomState>
where
    Key: Eq + Hash + Send + Sync + Clone + 'static,
    Value: Send + Sync + 'static,
    for<'a> Read: From<&'a Value> + Send + Sync + 'static,
{
    pub fn new(capacity: NonZeroUsize, partitions: NonZeroUsize) -> Self {
        Self::with_hasher(RandomState::new(), capacity, partitions)
    }
}

impl<Key, Value, Read, H> Cache<Key, Value, Read, H>
where
    Key: Eq + Hash + Send + Sync + Clone + 'static,
    Value: Send + Sync + 'static,
    H: BuildHasher + Send + Sync + Clone + 'static,
    for<'a> Read: From<&'a Value> + Send + Sync + 'static,
{
    pub fn with_hasher(
        hash_builder: H,
        capacity: NonZeroUsize,
        num_partitions: NonZeroUsize,
    ) -> Self {
        // AFAICT there's no reason to have the number of partitions exceed
        // the cache's capactity or the maximum # of parallel computations
        let num_partitions = num_partitions
            .min(capacity)
            .min(std::thread::available_parallelism().unwrap_or(NonZeroUsize::MAX));

        let partition_capacity =
            NonZeroUsize::new(capacity.get() / num_partitions.get()).expect("capacity");
        let (txs, rxs): (Vec<_>, Vec<_>) =
            (0..num_partitions.get()).map(|_| mpsc::channel()).unzip();

        rxs.into_iter().for_each(|rx| {
            let hash_builder = hash_builder.clone();
            std::thread::spawn(move || {
                let mut cache = super::Cache::with_hasher(hash_builder, partition_capacity);

                for message in rx {
                    match message {
                        Message::Get { key, tx } => {
                            let _ = tx.send(cache.with(&key, |val| Read::from(val)));
                        }

                        Message::Insert { key, value, tx } => {
                            let _ = tx.send(cache.insert(key, value));
                        }
                    };
                }
            });
        });

        Self {
            txs,
            hash_builder,
            num_partitions,
        }
    }

    pub async fn get(&self, key: Key) -> Result<Option<Read>, Error> {
        let (tx, rx) = oneshot::channel();
        let msg = Message::Get { key, tx };
        self.txs[msg.partition_idx(self.hash_builder.build_hasher(), self.num_partitions.get())]
            .send(msg)?;

        rx.await.map_err(|_| Error::Canceled)
    }

    pub async fn insert(&self, key: Key, value: Value) -> Result<Option<Value>, Error> {
        let (tx, rx) = oneshot::channel();
        let msg = Message::Insert { key, value, tx };
        self.txs[msg.partition_idx(self.hash_builder.build_hasher(), self.num_partitions.get())]
            .send(msg)?;

        rx.await.map_err(|_| Error::Canceled)
    }
}

enum Message<Key, Value, Read> {
    Get {
        key: Key,
        tx: oneshot::Sender<Option<Read>>,
    },
    Insert {
        key: Key,
        value: Value,
        tx: oneshot::Sender<Option<Value>>,
    },
}

impl<Key, Value, Read> Message<Key, Value, Read>
where
    Key: Hash,
{
    fn key(&self) -> &Key {
        match self {
            Self::Get { key, .. } | Self::Insert { key, .. } => key,
        }
    }

    fn partition_idx(&self, mut hasher: impl Hasher, num_partitions: usize) -> usize {
        self.key().hash(&mut hasher);
        let hash = hasher.finish();
        (hash.saturating_sub(1) / (u64::MAX / num_partitions as u64)) as usize
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    ChannelClosed,
    Canceled,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::ChannelClosed => write!(f, "Channel closed!"),
            Self::Canceled => write!(f, "Operation canceled!"),
        }
    }
}

impl std::error::Error for Error {}

impl<T> From<mpsc::SendError<T>> for Error {
    fn from(_: mpsc::SendError<T>) -> Self {
        Self::ChannelClosed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_futures() -> Result<(), Box<dyn std::error::Error>> {
        const NUM_TASKS: usize = 16;
        const NUM_KEYS_PER_TASK: usize = 64;
        let num_partitions = std::thread::available_parallelism().expect("parallelism");

        fn value(n: usize) -> String {
            format!("value {n}")
        }

        let cache = Cache::new(NonZeroUsize::new(10_000).expect("capacity"), num_partitions);

        let tasks: Vec<_> = (0..NUM_TASKS)
            .map(|i| {
                let cache = cache.clone();
                let start = i * NUM_KEYS_PER_TASK;
                let end = (i + 1) * NUM_KEYS_PER_TASK;

                async move {
                    let mut res = vec![];
                    for key in start..end {
                        let _ = cache.insert(key, value(key)).await;
                        res.push(cache.get(key).await);
                    }
                    res
                }
            })
            .collect();

        let results = futures::future::join_all(tasks).await;

        for (i, res) in results.into_iter().enumerate() {
            let res = res;
            let start = i * NUM_KEYS_PER_TASK;
            let end = (i + 1) * NUM_KEYS_PER_TASK;

            for (j, key) in (0..res.len()).zip(start..end) {
                assert_eq!(res[j], Ok(Some(value(key))));
            }
        }

        Ok(())
    }
}
