use crate::sqlx_store::SqlxEventStore;
use crate::types::event_read::EventRead;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::future::Future;
use tokio::sync::broadcast;

/// Result summary for a projection processing batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProjectionRunStats {
    pub received: usize,
    pub applied: usize,
    pub skipped: usize,
}

impl ProjectionRunStats {
    fn new() -> Self {
        Self {
            received: 0,
            applied: 0,
            skipped: 0,
        }
    }
}

impl SqlxEventStore {
    /// Processes up to `max_messages` events from a typed event receiver and applies
    /// the projection with idempotency guards.
    ///
    /// This is a lightweight utility to wire:
    /// - `EventStore::event_appended()`
    /// - `apply_projection_once(...)`
    ///
    /// Returns stats with received/applied/skipped counts.
    pub async fn process_projection_messages<Payload, Meta, Apply, Fut>(
        &self,
        projection_name: &str,
        receiver: &mut broadcast::Receiver<EventRead<Payload, Meta>>,
        max_messages: usize,
        apply: Apply,
    ) -> Result<ProjectionRunStats>
    where
        Payload: Send + Sync + 'static + Clone + Serialize + for<'de> Deserialize<'de>,
        Meta: Send + Sync + 'static + Clone + Serialize + for<'de> Deserialize<'de>,
        Apply: Fn(&EventRead<Payload, Meta>) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        let mut stats = ProjectionRunStats::new();
        if max_messages == 0 {
            return Ok(stats);
        }

        for _ in 0..max_messages {
            let event = match receiver.recv().await {
                Ok(event) => event,
                Err(broadcast::error::RecvError::Closed) => break,
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
            };
            stats.received += 1;

            let applied = self
                .apply_projection_once(projection_name, &event, |e| apply(e))
                .await?;
            if applied {
                stats.applied += 1;
            } else {
                stats.skipped += 1;
            }
        }

        Ok(stats)
    }
}
