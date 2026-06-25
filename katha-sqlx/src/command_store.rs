use crate::{
    backend::Backend,
    command_db::{CommandReadDb, CommandWriteDb},
    error::DbConversionError,
    pagination::{CommandCursor, CommandCursorPage},
    types::SqlxCommandStore,
    validate::validate_store_name,
};
use anyhow::Result;
use async_trait::async_trait;
use katha::traits::command_store::CommandStore;
use katha::types::command_write::{CommandRead, CommandWrite};
use serde::{Deserialize, Serialize};
use sqlx::{AnyPool, Executor, any::AnyPoolOptions};
use uuid::Uuid;

impl SqlxCommandStore {
    /// Creates a command store backed by an in-memory SQLite database.
    pub async fn new_memory(name: &str) -> Result<Self> {
        sqlx::any::install_default_drivers();
        validate_store_name(name)?;
        let pool = AnyPoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await?;
        Ok(Self {
            name: name.to_string(),
            pool,
            backend: Backend::Sqlite,
        })
    }

    /// Creates a command store backed by a local SQLite file.
    pub async fn new_local(name: &str, path: &str) -> Result<Self> {
        sqlx::any::install_default_drivers();
        validate_store_name(name)?;
        let url = format!("sqlite://{path}?mode=rwc");
        let pool = AnyPoolOptions::new().connect(&url).await?;
        Ok(Self {
            name: name.to_string(),
            pool,
            backend: Backend::Sqlite,
        })
    }

    /// Creates a command store from a database URL.
    ///
    /// URL format:
    /// - SQLite in-memory: `"sqlite::memory:"`
    /// - SQLite file: `"sqlite:///absolute/path/to/file.db"`
    /// - Postgres: `"postgres://user:pass@host/dbname"`
    pub async fn new_from_url(name: &str, url: &str) -> Result<Self> {
        sqlx::any::install_default_drivers();
        validate_store_name(name)?;
        let pool = AnyPoolOptions::new().connect(url).await?;
        Ok(Self {
            name: name.to_string(),
            pool,
            backend: Backend::from_url(url),
        })
    }

    /// Creates a command store from a pre-built `AnyPool`.
    ///
    /// The caller is responsible for calling `sqlx::any::install_default_drivers()` before
    /// creating the pool.
    pub async fn new_from_pool(name: &str, pool: AnyPool) -> Result<Self> {
        validate_store_name(name)?;
        let backend = Backend::from_url(pool.connect_options().database_url.as_str());
        Ok(Self {
            name: name.to_string(),
            pool,
            backend,
        })
    }

    /// Ensures command-store tables exist by running the command migration files.
    async fn ensure_command_tables(&self) -> Result<()> {
        let migrations = [
            include_str!("../migrations/0001_commands.sql"),
            include_str!("../migrations/0002_commands_add_causation_id.sql"),
        ];

        let mut tx = self.pool.begin().await?;
        for (i, template) in migrations.iter().enumerate() {
            let sql = template.replace("{{name}}", &self.name);
            for statement in sql.split(';') {
                let stmt = statement.trim();
                if stmt.is_empty() {
                    continue;
                }
                if let Err(e) = tx.execute(stmt).await {
                    // 0002 adds causation_id; ignore if column already exists (idempotent reopen).
                    // SQLite: "duplicate column name", Postgres: "already exists"
                    let msg = e.to_string();
                    if i == 1
                        && (msg.contains("duplicate column name") || msg.contains("already exists"))
                    {
                        continue;
                    }
                    tx.rollback().await?;
                    return Err(e.into());
                }
            }
        }
        tx.commit().await?;
        Ok(())
    }
}

#[async_trait]
impl<Payload> CommandStore<Payload> for SqlxCommandStore
where
    Payload: Send + Sync + 'static + Clone + Serialize + for<'de> Deserialize<'de>,
{
    /// Ensures the namespaced command table exists.
    async fn ensure_commands_table(&self) -> Result<()> {
        self.ensure_command_tables().await
    }

    /// Appends a command log entry.
    async fn append_command(&self, payload: &CommandWrite<Payload>) -> Result<()> {
        let command = CommandWriteDb::from(payload);

        sqlx::query(&self.backend.bind(&format!(
            r#"INSERT INTO "{}_commands" (id, correlation_id, causation_id, data, name, created_utc)
            VALUES (?, ?, ?, ?, ?, ?)"#,
            self.name
        )))
        .bind(&command.id)
        .bind(&command.correlation_id)
        .bind(&command.causation_id)
        .bind(&command.data)
        .bind(&command.name)
        .bind(&command.created_utc)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Fetches one command log entry by command id.
    async fn get_command(&self, id: &Uuid) -> Result<Option<CommandRead<Payload>>> {
        let row = sqlx::query_as::<_, CommandReadDb>(&self.backend.bind(&format!(
            r#"SELECT id, correlation_id, causation_id, data, name, created_utc
            FROM "{}_commands" WHERE id = ?"#,
            self.name
        )))
        .bind(id.to_string())
        .fetch_optional(&self.pool)
        .await?;

        row.map(CommandRead::try_from)
            .transpose()
            .map_err(|e: DbConversionError| anyhow::anyhow!(e))
    }

    /// Fetches command log entries in descending creation order with pagination.
    async fn get_commands(
        &self,
        limit: Option<usize>,
        offset: usize,
    ) -> Result<Vec<CommandRead<Payload>>> {
        let rows = match limit {
            Some(limit) => {
                sqlx::query_as::<_, CommandReadDb>(&self.backend.bind(&format!(
                    r#"SELECT id, correlation_id, causation_id, data, name, created_utc
                    FROM "{}_commands"
                    ORDER BY created_utc DESC
                    LIMIT ? OFFSET ?"#,
                    self.name
                )))
                .bind(limit as i64)
                .bind(offset as i64)
                .fetch_all(&self.pool)
                .await?
            }
            None => {
                sqlx::query_as::<_, CommandReadDb>(&self.backend.bind(&format!(
                    r#"SELECT id, correlation_id, causation_id, data, name, created_utc
                    FROM "{}_commands"
                    ORDER BY created_utc DESC
                    LIMIT 9223372036854775807 OFFSET ?"#,
                    self.name
                )))
                .bind(offset as i64)
                .fetch_all(&self.pool)
                .await?
            }
        };

        rows.into_iter()
            .map(CommandRead::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e: DbConversionError| anyhow::anyhow!(e))
    }
}

impl SqlxCommandStore {
    /// Reads commands using cursor-based pagination.
    ///
    /// Cursor semantics:
    /// - `None` starts at the most recent command.
    /// - `Some(cursor)` returns commands older than the cursor.
    /// - `next_cursor` is the last returned command's cursor when more items exist.
    pub async fn get_commands_page<Payload>(
        &self,
        cursor: Option<&CommandCursor>,
        limit: usize,
    ) -> Result<CommandCursorPage<Payload>>
    where
        Payload: Send + Sync + 'static + Clone + Serialize + for<'de> Deserialize<'de>,
    {
        let fetch_limit = (limit.max(1) + 1) as i64;

        let rows = match cursor {
            None => {
                sqlx::query_as::<_, CommandReadDb>(&self.backend.bind(&format!(
                    r#"SELECT id, correlation_id, causation_id, data, name, created_utc
                FROM "{}_commands"
                ORDER BY created_utc DESC, id DESC
                LIMIT ?"#,
                    self.name
                )))
                .bind(fetch_limit)
                .fetch_all(&self.pool)
                .await?
            }
            Some(c) => {
                sqlx::query_as::<_, CommandReadDb>(&self.backend.bind(&format!(
                    r#"SELECT id, correlation_id, causation_id, data, name, created_utc
                FROM "{}_commands"
                WHERE (created_utc, id) < (?, ?)
                ORDER BY created_utc DESC, id DESC
                LIMIT ?"#,
                    self.name
                )))
                .bind(c.created_utc.to_rfc3339())
                .bind(c.id.to_string())
                .bind(fetch_limit)
                .fetch_all(&self.pool)
                .await?
            }
        };

        let has_more = rows.len() > limit.max(1);
        let items: Vec<CommandRead<Payload>> = rows
            .into_iter()
            .take(limit.max(1))
            .map(CommandRead::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e: DbConversionError| anyhow::anyhow!(e))?;

        let next_cursor = if has_more {
            items.last().map(|cmd| CommandCursor {
                created_utc: cmd.created_utc,
                id: cmd.id,
            })
        } else {
            None
        };

        Ok(CommandCursorPage { items, next_cursor })
    }
}
