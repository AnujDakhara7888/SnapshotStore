mod support;

use std::env;

use support::run_seeded_history;

#[tokio::test]
async fn s30_reproducible_seed() {
    let seed = env::var("SNAPSHOTSTORE_S30_SEED")
        .unwrap_or_else(|_| "0".to_owned())
        .parse::<u64>()
        .expect("SNAPSHOTSTORE_S30_SEED must be an unsigned integer");
    run_seeded_history(seed, 20).await;
}
