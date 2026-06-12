use crate::types::command_write::{CommandRead, CommandWrite};
use anyhow::Result;
use async_trait::async_trait;
use uuid::Uuid;
/// A trait for storing and retrieving commands in an event-sourced system.
///
/// This trait defines the interface for command storage, allowing commands to be
/// persisted and retrieved. Commands represent the intent to change the state of
/// the system.
///
/// # Type Parameters
/// * `Payload` - The type of the command payload
///
/// # Examples
///
/// ```rust,ignore
/// use katha::{CommandStore, CommandWrite};
/// use anyhow::Result;
/// use async_trait::async_trait;
///
/// struct InMemoryCommandStore;
///
/// #[async_trait]
/// impl<Payload> CommandStore<Payload> for InMemoryCommandStore {
///     async fn append_command(&self, payload: &CommandWrite<Payload>) -> Result<()> {
///         // Implementation here
///         Ok(())
///     }
/// }
/// ```
#[async_trait]
pub trait CommandStore<Payload> {
    /// Ensures the commands table exists in the database.
    ///
    /// # Returns
    /// A `Result` indicating success or failure
    ///
    /// # Errors
    /// Returns an error if the commands table cannot be ensured
    async fn ensure_commands_table(&self) -> Result<()>;

    /// Appends a command to the command store.
    ///
    /// # Arguments
    /// * `payload` - The command to append
    ///
    /// # Returns
    /// A `Result` indicating success or failure
    ///
    /// # Errors
    /// Returns an error if the command cannot be appended
    async fn append_command(&self, payload: &CommandWrite<Payload>) -> Result<()>;

    /// Retrieves a command from the command store by its unique identifier.
    ///
    /// # Arguments
    /// * `id` - The unique identifier of the command to retrieve
    ///
    /// # Returns
    /// An `Option` containing the command if found, or `None` if not found, wrapped in a `Result`
    ///
    /// # Errors
    /// Returns an error if the command cannot be retrieved
    async fn get_command(&self, id: &Uuid) -> Result<Option<CommandRead<Payload>>>;

    /// Retrieves a list of commands from the command store with optional pagination.
    ///
    /// # Arguments
    /// * `limit` - An optional maximum number of commands to retrieve
    /// * `offset` - The number of commands to skip before starting to collect the result set
    ///
    /// # Returns
    /// A vector of commands wrapped in a `Result`
    ///
    /// # Errors
    /// Returns an error if the commands cannot be retrieved
    async fn get_commands(
        &self,
        limit: Option<usize>,
        offset: usize,
    ) -> Result<Vec<CommandRead<Payload>>>;
}
