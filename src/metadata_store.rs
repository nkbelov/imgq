// Where final-image METADATA is recorded. Stage 2 -> real DB.
#[allow(async_fn_in_trait)]
pub trait MetadataStore: Send + Sync {
    async fn record(&self, id: &str, output_path: &str) -> anyhow::Result<()>;
}

pub struct NoopMetadataStore;

impl MetadataStore for NoopMetadataStore {
    async fn record(&self, id: &str, output_path: &str) -> anyhow::Result<()> {
        Ok(())
    }
}