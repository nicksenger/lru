use std::num::NonZeroUsize;
use std::time::Instant;

use ahash::RandomState;
use criterion::{criterion_group, criterion_main, Criterion};
use rand::Rng;

const CAPACITIES: &[usize] = &[1_000_000];

/// Unsync cache get & full insertion times
fn unsync(c: &mut Criterion) {
    let mut rng = rand::thread_rng();

    for &capacity in CAPACITIES {
        let get_cache = || {
            let mut cache = lru::Cache::with_hasher(
                RandomState::new(),
                NonZeroUsize::new(capacity).expect("capacity"),
            );
            for n in 0..capacity {
                let _ = cache.insert(n, n);
            }
            cache
        };

        let mut cache = get_cache();
        c.bench_function(
            format!(
                "this crate full insert (unsync, {} entries)",
                commas(capacity)
            )
            .as_str(),
            |b| {
                b.iter_batched(
                    || rng.gen(),
                    |n| cache.insert(n, n),
                    criterion::BatchSize::SmallInput,
                )
            },
        );

        let cache = get_cache();
        c.bench_function(
            format!(
                "this crate cache hit (unsync, {} entries)",
                commas(capacity)
            )
            .as_str(),
            |b| {
                b.iter_batched(
                    || rng.gen_range(0..capacity),
                    |n| cache.with(&n, |m| *m),
                    criterion::BatchSize::SmallInput,
                )
            },
        );
    }
}

/// Full partitioned cache get & insert times
fn sync(c: &mut Criterion) {
    use tokio::runtime::{self, Runtime};

    #[derive(Clone, Debug)]
    struct Read;

    impl From<&usize> for Read {
        fn from(_: &usize) -> Self {
            Self
        }
    }

    // Benchmark on all available threads & just a single thread
    let thread_counts = [
        std::thread::available_parallelism().expect("parallelism"),
        NonZeroUsize::MIN,
    ];

    for (thread_count, capacity) in thread_counts
        .into_iter()
        .flat_map(|t| CAPACITIES.iter().map(move |c| (t, *c)))
    {
        let runtime = Runtime::new().expect("runtime");
        let get_cache = |num_partitions| {
            runtime.block_on(async move {
                let cache = lru::sync::Cache::<_, _, Read, _>::with_hasher(
                    RandomState::new(),
                    NonZeroUsize::new(capacity).expect("capacity"),
                    num_partitions,
                );
                for n in 0..capacity {
                    let _ = cache.insert(n, n).await;
                }
                cache
            })
        };

        let cache = get_cache(thread_count);
        c.bench_function(
            format!(
                "this crate full insert ({} threads, {} entries)",
                thread_count,
                commas(capacity)
            )
            .as_str(),
            |b| {
                let cache = cache.clone();
                b.to_async(
                    runtime::Builder::new_multi_thread()
                        .build()
                        .expect("runtime"),
                )
                .iter_custom(|iters| {
                    let fut = futures::future::join_all((0..iters).scan(
                        rand::thread_rng(),
                        |rng, _i| {
                            let n = rng.gen();
                            Some(cache.insert(n, n))
                        },
                    ));

                    async move {
                        let start = Instant::now();
                        let _ = fut.await;
                        start.elapsed()
                    }
                })
            },
        );

        let cache = get_cache(thread_count);
        c.bench_function(
            format!(
                "this crate cache hit ({} threads, {} entries)",
                thread_count,
                commas(capacity)
            )
            .as_str(),
            |b| {
                let cache = cache.clone();
                b.to_async(
                    runtime::Builder::new_multi_thread()
                        .build()
                        .expect("runtime"),
                )
                .iter_custom(|iters| {
                    let fut = futures::future::join_all((0..iters).scan(
                        rand::thread_rng(),
                        |rng, _i| {
                            let n = rng.gen_range(0..capacity);
                            Some(cache.get(n))
                        },
                    ));

                    async move {
                        let start = Instant::now();
                        let _ = fut.await;
                        start.elapsed()
                    }
                })
            },
        );
    }
}

/// Full moka cache get & insert times
fn moka(c: &mut Criterion) {
    use tokio::runtime::{self, Runtime};

    for &capacity in CAPACITIES {
        let runtime = Runtime::new().expect("runtime");
        let get_cache = || {
            let cache = moka::future::Cache::new(capacity as u64);
            runtime.block_on(async move {
                for n in 0..capacity {
                    let _ = cache.insert(n, n).await;
                }
                cache
            })
        };

        let parallelism = std::thread::available_parallelism().expect("parallelism");
        let cache = get_cache();
        c.bench_function(
            format!(
                "moka full insert ({} threads, {} entries)",
                parallelism,
                commas(capacity)
            )
            .as_str(),
            |b| {
                let cache = cache.clone();
                b.to_async(
                    runtime::Builder::new_multi_thread()
                        .build()
                        .expect("runtime"),
                )
                .iter_custom(|iters| {
                    let fut = futures::future::join_all((0..iters).scan(
                        rand::thread_rng(),
                        |rng, _i| {
                            let n = rng.gen();
                            Some(cache.insert(n, n))
                        },
                    ));

                    async move {
                        let start = Instant::now();
                        let _ = fut.await;
                        start.elapsed()
                    }
                })
            },
        );

        let cache = moka::sync::Cache::new(capacity as u64);
        let mut rng = rand::thread_rng();
        c.bench_function(
            format!("moka full insert (unsync {} entries)", commas(capacity)).as_str(),
            |b| {
                b.iter_batched(
                    || rng.gen(),
                    |n: usize| cache.insert(n, n),
                    criterion::BatchSize::SmallInput,
                )
            },
        );
    }
}

/// Full quick cache get & insert times
fn quick_cache(c: &mut Criterion) {
    use std::sync::Arc;
    use tokio::runtime::{self, Runtime};

    for &capacity in CAPACITIES {
        let runtime = Runtime::new().expect("runtime");
        let get_cache = || {
            let cache = Arc::new(quick_cache::sync::Cache::new(capacity));
            runtime.block_on(async move {
                for n in 0..capacity {
                    let _ = cache.insert(n, n);
                }
                cache
            })
        };

        let parallelism = std::thread::available_parallelism().expect("parallelism");
        let cache = get_cache();
        c.bench_function(
            format!(
                "quick-cache full insert ({} threads, {} entries)",
                parallelism,
                commas(capacity)
            )
            .as_str(),
            |b| {
                let cache = cache.clone();
                b.to_async(
                    runtime::Builder::new_multi_thread()
                        .build()
                        .expect("runtime"),
                )
                .iter_custom(|iters| {
                    let fut = futures::future::join_all((0..iters).scan(
                        rand::thread_rng(),
                        |rng, _i| {
                            let n = rng.gen();
                            let cache = cache.clone();
                            Some(tokio::task::spawn_blocking(move || cache.insert(n, n)))
                        },
                    ));

                    async move {
                        let start = Instant::now();
                        let _ = fut.await;
                        start.elapsed()
                    }
                })
            },
        );

        let mut cache = quick_cache::unsync::Cache::new(capacity);
        let mut rng = rand::thread_rng();
        c.bench_function(
            format!(
                "quick-cache full insert (unsync {} entries)",
                commas(capacity)
            )
            .as_str(),
            |b| {
                b.iter_batched(
                    || rng.gen(),
                    |n: usize| cache.insert(n, n),
                    criterion::BatchSize::SmallInput,
                )
            },
        );
    }
}

fn commas(x: impl ToString) -> String {
    x.to_string()
        .as_bytes()
        .rchunks(3)
        .rev()
        .map(std::str::from_utf8)
        .collect::<Result<Vec<&str>, _>>()
        .unwrap()
        .join(",")
}

criterion_group!(cache, quick_cache, moka, sync, unsync);
criterion_main!(cache);
