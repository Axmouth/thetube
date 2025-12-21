use fibril_util::init_tracing;

fn main() {
    init_tracing();

    tracing::info!("Hello, world!");
}
