use std::collections::HashMap;

use tokio::sync::Mutex;

use crate::Job;

// Where job STATE lives, shared across workers. Stage 4 -> Redis.
// Must be durable enough that recovery can find Processing-stuck jobs on restart.
#[allow(async_fn_in_trait)]
pub trait StateStore: Send + Sync {
    async fn put(&self, job: &Job) -> anyhow::Result<()>;
    async fn get(&self, id: &str) -> anyhow::Result<Option<Job>>;
    async fn all(&self) -> anyhow::Result<Vec<Job>>; // for recovery scan
}

pub struct InMemoryStateStore {
    inner: Mutex<HashMap<String, Job>>,
}

impl InMemoryStateStore {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new())
        }
    }
}

impl StateStore for InMemoryStateStore {
    async fn put(&self, job: &Job) -> anyhow::Result<()> {
        let mut inner = self.inner.lock().await;
        inner.insert(job.id.to_owned(), job.to_owned());
        Ok(())
    }

    async fn get(&self, id: &str) -> anyhow::Result<Option<Job>> {
        let inner = self.inner.lock().await;
        Ok(inner.get(id).cloned())
    }

    async fn all(&self) -> anyhow::Result<Vec<Job>> {
        let inner = self.inner.lock().await;
        Ok(inner.iter().map(|e| e.1).cloned().collect::<Vec<_>>())
    }
}
