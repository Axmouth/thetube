use fibril_storage::Storage;
use fibril_storage::rocksdb_store::RocksStorage;
use fibril_util::init_tracing;
use std::time::Instant;

const ITERATIONS: usize = 2_000_000;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    run_bench(256).await?;
    run_bench(1024).await?;
    run_bench(2048).await?;
    run_bench(4096).await?;

    Ok(())
}

async fn run_bench(max_msg_size: usize) -> anyhow::Result<()> {
    init_tracing();

    let messages = (0..ITERATIONS)
        .map(|_| {
            let size = fastrand::usize(32..max_msg_size);
            let mut buf = vec![0u8; size];
            fastrand::fill(&mut buf);
            buf
        })
        .collect::<Vec<_>>();

    // First delete any existing test DB
    let _ = std::fs::remove_dir_all("test_data/bench_db");

    let store = RocksStorage::open("test_data/bench_db" , false)?;
    let start = Instant::now();

    for buf in messages {
        store.append(&"t".into(), 0, &buf).await?;
    }

    tracing::info!(
        "append/sec = {}, max msg size = {} bytes",
        ITERATIONS as f64 / start.elapsed().as_secs_f64(),
        max_msg_size
    );

    Ok(())
}
