#![doc = include_str!("../README.md")]

mod commands;
mod events;

use anyhow::{Result, ensure};
use libsql::{Builder, Connection, Database};
use std::{path::Path, sync::Arc};
use tokio::sync::{Mutex, MutexGuard};

/// A database shared by event and command stores.
///
/// Clones share the same database and serialize access. Separate handles to a
/// file or remote database are protected by SQLite's write transactions.
#[derive(Clone)]
pub struct LibsqlDatabase(Arc<DatabaseInner>);

struct DatabaseInner {
    _database: Database,
    connection: Mutex<Connection>,
}

impl LibsqlDatabase {
    /// Wraps an official libSQL database, including caller-configured databases.
    pub fn from_database(database: Database) -> Result<Self> {
        let connection = database.connect()?;
        Ok(Self(Arc::new(DatabaseInner {
            _database: database,
            connection: Mutex::new(connection),
        })))
    }

    /// Opens an in-memory database. Keep this handle to share its contents.
    pub async fn new_memory() -> Result<Self> {
        Self::new_local(":memory:").await
    }

    /// Opens or creates a SQLite database. Its parent directory must exist.
    pub async fn new_local(path: impl AsRef<Path>) -> Result<Self> {
        Self::from_database(Builder::new_local(path).build().await?)
    }

    /// Connects to a hosted Turso database with its URL and authentication token.
    pub async fn new_remote(url: &str, auth_token: &str) -> Result<Self> {
        Self::from_database(
            Builder::new_remote(url.to_owned(), auth_token.to_owned())
                .build()
                .await?,
        )
    }

    async fn session(&self) -> MutexGuard<'_, Connection> {
        // Retain one connection: each :memory: connection is a separate database.
        self.0.connection.lock().await
    }
}

impl std::fmt::Debug for LibsqlDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LibsqlDatabase").finish_non_exhaustive()
    }
}

/// Event store over local SQLite or hosted Turso/libSQL.
#[derive(Debug, Clone)]
pub struct LibsqlEventStore {
    database: LibsqlDatabase,
    name: String,
}

/// Command store over local SQLite or hosted Turso/libSQL.
#[derive(Debug, Clone)]
pub struct LibsqlCommandStore {
    database: LibsqlDatabase,
    name: String,
}

fn validate_name(name: &str) -> Result<()> {
    ensure!(
        !name.is_empty() && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_'),
        "Store name must contain only ASCII letters, digits, and underscores"
    );
    Ok(())
}

macro_rules! constructors {
    ($store:ident) => {
        impl $store {
            /// Creates a store using a shared database handle.
            /// Tables are initialized separately through the core store trait.
            pub fn new_from_database(name: &str, database: LibsqlDatabase) -> Result<Self> {
                validate_name(name)?;
                Ok(Self {
                    database,
                    name: name.to_owned(),
                })
            }

            /// Creates a store backed by an in-memory SQLite database.
            pub async fn new_memory(name: &str) -> Result<Self> {
                validate_name(name)?;
                Self::new_from_database(name, LibsqlDatabase::new_memory().await?)
            }

            /// Opens or creates a local database. Its parent must exist.
            pub async fn new_local(name: &str, path: impl AsRef<Path>) -> Result<Self> {
                validate_name(name)?;
                Self::new_from_database(name, LibsqlDatabase::new_local(path).await?)
            }

            /// Creates a store connected to a hosted Turso database.
            pub async fn new_remote(name: &str, url: &str, auth_token: &str) -> Result<Self> {
                validate_name(name)?;
                Self::new_from_database(name, LibsqlDatabase::new_remote(url, auth_token).await?)
            }
        }
    };
}

constructors!(LibsqlEventStore);
constructors!(LibsqlCommandStore);
