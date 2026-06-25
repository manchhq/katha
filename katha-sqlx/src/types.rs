use crate::backend::Backend;
use crate::notifications::EventNotification;
use sqlx::AnyPool;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct SqlxEventStore {
    pub(crate) pool: AnyPool,
    pub(crate) name: String,
    pub(crate) backend: Backend,
    pub(crate) notifier: broadcast::Sender<EventNotification>,
    pub(crate) cancel_token: CancellationToken,
}

impl std::fmt::Debug for SqlxEventStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlxEventStore")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone)]
pub struct SqlxCommandStore {
    pub(crate) pool: AnyPool,
    pub(crate) name: String,
    pub(crate) backend: Backend,
}
