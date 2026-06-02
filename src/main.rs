mod metadata_store;
mod state_store;

use axum::{
    Json, Router,
    extract::{Multipart, State},
    routing::{get, post},
};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use std::{collections::VecDeque, sync::Arc};
use tokio::sync::{Mutex, Semaphore, mpsc};

use crate::{
    metadata_store::{MetadataStore, NoopMetadataStore},
    state_store::{InMemoryStateStore, StateStore},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum JobState {
    // Request durably accepted, not yet picked up.
    Queued,
    // A worker has claimed it and is processing.
    Processing,
    // Output written to FS, metadata recorded. Terminal success.
    Done,
    // Processing failed (bad image, transform error). Terminal failure, with
    // reason retained.
    Failed,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Job {
    pub id: String,
    pub source_path: String,  // where the uploaded image lives
    pub transform: Transform, // what to do to it
    pub state: JobState,
    pub error: Option<String>, // populated iff state == Failed
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum Transform {
    Grayscale,
    Resize { width: u32, height: u32 },
    Blur { sigma: f32 },
}

// A job queue with an asynchronous push/pop
// interface.
//
// TODO: Real version with Kafka
pub struct JobQueue {
    inner: Mutex<VecDeque<Job>>,
}

impl JobQueue {
    async fn push(&self, job: Job) {
        self.inner.lock().await.push_back(job);
    }

    async fn pop(&self) -> Option<Job> {
        self.inner.lock().await.pop_front()
    }
}

struct Pipeline {
    state: Arc<InMemoryStateStore>,
    metadata: Arc<NoopMetadataStore>,
    queue: Arc<JobQueue>,
    // bounds concurrent in-flight (decoded) images -> bounds memory
    permits: Arc<Semaphore>,
}

impl Pipeline {
    // The consumer loop: pull job ids, process each under a concurrency permit.
    async fn run(self: Arc<Self>, mut rx: mpsc::Receiver<String>) {
        while let Some(job_id) = rx.recv().await {
            let permit = self.permits.clone().acquire_owned().await.unwrap();

            let pipeline = self.clone();

            tokio::spawn(async {
                pipeline.process_job(job_id).await;
                drop(permit);
            });
        }
    }

    // Upon relaunch, inspect outstanding non-terminal jobs and enqueue them into `tx`.
    async fn recover(&self, tx: &mpsc::Sender<String>) -> anyhow::Result<()> {
        for job in self.state.all().await? {
            if job.state == JobState::Processing || job.state == JobState::Queued {
                let _ = tx.send(job.id).await; // re-schedule
            }
        }
        Ok(())
    }

    async fn process_job(self: Arc<Self>, job_id: String) {
        let mut job = match self.state.get(&job_id).await {
            Ok(Some(j)) => j,
            _ => return, // unknown id; nothing to do
        };
        job.state = JobState::Processing;
        let _ = self.state.put(&job).await;

        let transform = job.transform.clone();
        let source = job.source_path.clone();
        let result = tokio::task::spawn_blocking(move || {
            do_transform(&source, &transform) // returns Result<output_path>
        })
        .await;

        match result {
            Ok(Ok(output_path)) => {
                let _ = self.metadata.record(&job.id, &output_path).await;
                job.state = JobState::Done;
                let _ = self.state.put(&job).await;
            }
            Ok(Err(e)) => {
                job.state = JobState::Failed;
                job.error = Some(e.to_string());
                let _ = self.state.put(&job).await;
            }
            Err(_) => {
                job.state = JobState::Failed;
                job.error = Some("worker panicked".into());
                let _ = self.state.put(&job).await;
            }
        }
    }
}

fn do_transform(source: &str, transform: &Transform) -> anyhow::Result<String> {
    let img = image::open(source)?;
    let out = match transform {
        Transform::Grayscale => img.grayscale(),
        Transform::Resize { width, height } => {
            img.resize_exact(*width, *height, image::imageops::FilterType::Lanczos3)
        }
        Transform::Blur { sigma } => img.blur(*sigma),
    };
    // idempotent output path: deterministic from id/params, overwrite-safe.
    let output_path = format!("outputs/{}.png", uuid::Uuid::new_v4());
    out.save(&output_path)?;
    Ok(output_path)
}

#[derive(Debug, Clone)]
struct RequestParams {
    transform: Transform,
    image: Vec<u8>,
}

async fn process_request(State(pipeline): State<Arc<Pipeline>>, mut body: Multipart) -> StatusCode {
    let mut image: Option<Vec<u8>> = None;
    let mut transform: Option<Transform> = None;

    while let Ok(Some(field)) = body.next_field().await {
        match field.name() {
            Some("image") => match field.bytes().await {
                Ok(b) => image = Some(b.to_vec()),
                Err(_) => return StatusCode::BAD_REQUEST,
            },
            Some("transform") => match field.bytes().await {
                Ok(b) => match serde_json::from_slice::<Transform>(&b) {
                    Ok(t) => transform = Some(t),
                    Err(_) => return StatusCode::BAD_REQUEST,
                },
                Err(_) => return StatusCode::BAD_REQUEST,
            },
            _ => {}
        }
    }

    let _params = match (image, transform) {
        (Some(image), Some(transform)) => RequestParams { image, transform },
        _ => return StatusCode::BAD_REQUEST,
    };

    StatusCode::OK
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    const QUEUE_CAPACITY: usize = 1024;
    let max_inflight: usize = num_cpus::get(); // ~1 per core for CPU work

    let state = Arc::new(InMemoryStateStore::new());
    let metadata = Arc::new(NoopMetadataStore);
    let permits = Arc::new(Semaphore::new(max_inflight));

    let queue = Arc::new(JobQueue {
        inner: Mutex::new(VecDeque::new()),
    });
    let pipeline = Arc::new(Pipeline {
        state,
        metadata,
        queue,
        permits,
    });
    let (tx, rx) = mpsc::channel::<String>(QUEUE_CAPACITY);

    // Recover before serving
    pipeline.recover(&tx).await?;

    // Start the worker pool
    tokio::spawn(pipeline.clone().run(rx));

    let app = Router::new()
        .route("/", get(|| async { "Hello, World!" }))
        .route("/enqueue", post(process_request))
        .with_state(pipeline);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();

    Ok(())
}
