use crate::LibsqlCommandStore;
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use katha::{CommandRead, CommandStore, CommandWrite};
use libsql::{Row, params};
use serde::{Serialize, de::DeserializeOwned};
use uuid::Uuid;

fn read_command<P: DeserializeOwned>(row: Row) -> Result<CommandRead<P>> {
    Ok(CommandRead {
        id: Uuid::parse_str(&row.get::<String>(0)?)?,
        correlation_id: Uuid::parse_str(&row.get::<String>(1)?)?,
        causation_id: row
            .get::<Option<String>>(2)?
            .map(|s| Uuid::parse_str(&s))
            .transpose()?,
        name: row.get(3)?,
        data: serde_json::from_str(&row.get::<String>(4)?)?,
        created_utc: DateTime::parse_from_rfc3339(&row.get::<String>(5)?)?.with_timezone(&Utc),
    })
}

#[async_trait]
impl<P> CommandStore<P> for LibsqlCommandStore
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    async fn ensure_commands_table(&self) -> Result<()> {
        let connection = self.database.session().await;
        connection.execute(&format!(
            "CREATE TABLE IF NOT EXISTS \"{}_commands\" (id TEXT PRIMARY KEY, correlation_id TEXT NOT NULL, causation_id TEXT, name TEXT NOT NULL, data TEXT NOT NULL, created_utc TEXT NOT NULL)", self.name
        ), ()).await?;
        Ok(())
    }

    async fn append_command(&self, command: &CommandWrite<P>) -> Result<()> {
        let data = serde_json::to_string(&command.data)?;
        let connection = self.database.session().await;
        connection.execute(&format!(
            "INSERT INTO \"{}_commands\" (id, correlation_id, causation_id, name, data, created_utc) VALUES (?, ?, ?, ?, ?, ?)", self.name
        ), params![command.id.to_string(), command.correlation_id.to_string(),
            command.causation_id.map(|id| id.to_string()), command.name.clone(), data, Utc::now().to_rfc3339()]).await?;
        Ok(())
    }

    async fn get_command(&self, id: &Uuid) -> Result<Option<CommandRead<P>>> {
        let connection = self.database.session().await;
        let mut rows = connection.query(&format!(
            "SELECT id, correlation_id, causation_id, name, data, created_utc FROM \"{}_commands\" WHERE id = ?", self.name
        ), [id.to_string()]).await?;
        rows.next().await?.map(read_command).transpose()
    }

    async fn get_commands(
        &self,
        limit: Option<usize>,
        offset: usize,
    ) -> Result<Vec<CommandRead<P>>> {
        let limit = limit.map(i64::try_from).transpose()?.unwrap_or(-1);
        let offset = i64::try_from(offset)?;
        let connection = self.database.session().await;
        let mut rows = connection.query(&format!(
            "SELECT id, correlation_id, causation_id, name, data, created_utc FROM \"{}_commands\" ORDER BY created_utc DESC, id DESC LIMIT ? OFFSET ?", self.name
        ), params![limit, offset]).await?;
        let mut commands = Vec::new();
        while let Some(row) = rows.next().await? {
            commands.push(read_command(row)?);
        }
        Ok(commands)
    }
}
