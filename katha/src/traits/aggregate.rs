use crate::types::event_read_range::EventsReadRange;
use crate::types::event_write::EventWrite;
use crate::types::expected_version::ExpectedVersion;
use crate::{traits::event_store::EventStore, types::event_read::EventRead};
use anyhow::Result;
use serde::{Deserialize, Serialize};
/// A trait representing an event-sourced aggregate.
///
/// An aggregate is a cluster of domain objects that can be treated as a single unit.
/// This trait defines the core operations needed for event sourcing: initialization,
/// event application, and command execution.
///
/// # Type Parameters
/// * `State` - The type representing the aggregate's state
/// * `Command` - The type representing commands that can be executed
/// * `Event` - The type representing events that can be applied
///
/// # Examples
/// ```rust
/// use katha::Aggregate;
/// use anyhow::Result;
///
/// struct BankAccount;
///
/// #[derive(Clone)]
/// struct AccountState {
///     balance: i64,
/// }
///
/// #[derive(Clone)]
/// enum AccountCommand {
///     Deposit(i64),
///     Withdraw(i64),
/// }
///
/// #[derive(Clone)]
/// enum AccountEvent {
///     Deposited(i64),
///     Withdrawn(i64),
/// }
///
/// impl Aggregate<AccountState, AccountCommand, AccountEvent> for BankAccount {
///     fn init(&self) -> AccountState {
///         AccountState { balance: 0 }
///     }
///
///     fn apply(&self, state: AccountState, event: &AccountEvent) -> AccountState {
///         match event {
///             AccountEvent::Deposited(amount) => AccountState {
///                 balance: state.balance + amount,
///             },
///             AccountEvent::Withdrawn(amount) => AccountState {
///                 balance: state.balance - amount,
///             },
///         }
///     }
///
///     fn execute(&self, state: &AccountState, command: &AccountCommand) -> Result<Vec<AccountEvent>> {
///         match command {
///             AccountCommand::Deposit(amount) => Ok(vec![AccountEvent::Deposited(*amount)]),
///             AccountCommand::Withdraw(amount) => {
///                 if state.balance >= *amount {
///                     Ok(vec![AccountEvent::Withdrawn(*amount)])
///                 } else {
///                     Err(anyhow::anyhow!("Insufficient funds"))
///                 }
///             }
///         }
///     }
/// }
/// ```
pub trait Aggregate<State, Command, Event> {
    /// Initializes the aggregate's state.
    fn init(&self) -> State;
    /// Applies an event to the current state, producing a new state.
    ///
    /// # Arguments
    /// * `state` - The current state of the aggregate
    /// * `event` - The event to apply
    ///
    /// # Returns
    /// The new state after applying the event
    fn apply(&self, state: State, event: &Event) -> State;
    /// Executes a command against the current state, producing a list of events.
    ///
    /// # Arguments
    /// * `state` - The current state of the aggregate
    /// * `command` - The command to execute
    ///
    /// # Returns
    /// A `Result` containing a vector of events if successful
    ///
    /// # Errors
    /// Returns an error if the command cannot be executed
    fn execute(&self, state: &State, command: &Command) -> Result<Vec<Event>>;
}

/// Rehydrates aggregate state by folding a sequence of persisted events.
///
/// This helper is intentionally explicit: it only applies events in order using
/// the aggregate's `apply` function, preserving domain ownership of behavior.
pub fn rehydrate<State, Command, Event, Meta>(
    aggregate: &impl Aggregate<State, Command, Event>,
    events: &[EventRead<Event, Meta>],
) -> State {
    events.iter().fold(aggregate.init(), |state, event| {
        aggregate.apply(state, &event.data)
    })
}

/// Computes the next expected version from a rehydrated event sequence.
///
/// Returns:
/// - `ExpectedVersion::NoStream` when no events exist.
/// - `ExpectedVersion::Exact(last + 1)` when at least one event exists.
pub fn next_expected_version<Event, Meta>(
    events: &[EventRead<Event, Meta>],
) -> Result<ExpectedVersion> {
    match events.last() {
        None => Ok(ExpectedVersion::NoStream),
        Some(last) => {
            let next = last.version.checked_add(1).ok_or_else(|| {
                anyhow::anyhow!("Stream version overflow for stream {}", last.stream_id)
            })?;
            Ok(ExpectedVersion::Exact(next))
        }
    }
}

/// Loads all events for a stream and returns `(state, expected_version_for_next_write)`.
///
/// This is a small ergonomic helper for command handlers that want to:
/// 1) read current stream state, and
/// 2) compute the exact next version for optimistic concurrency.
pub async fn load_state_and_expected_version<State, Command, Event, Meta>(
    aggregate: &impl Aggregate<State, Command, Event>,
    store: &impl EventStore<Event, Meta>,
    stream_id: &str,
) -> Result<(State, ExpectedVersion)>
where
    Event: Clone + Serialize + for<'de> Deserialize<'de>,
    Meta: Clone + Serialize + for<'de> Deserialize<'de>,
{
    let events = store
        .get_events(stream_id, &EventsReadRange::AllEvents)
        .await?;
    let state = rehydrate(aggregate, &events);
    let expected = next_expected_version(&events)?;
    Ok((state, expected))
}
/// Creates a persistent, async command handler for an aggregate given an event store.
///
/// This function handles the command processing workflow:
/// 1. Executes the command against the current state to produce new events
/// 2. Appends the new events to the store
///
/// # Type Parameters
/// * `State` - The type representing the aggregate's state
/// * `Command` - The type representing commands that can be executed
/// * `Event` - The type representing events that can be applied
/// * `Meta` - The type representing event metadata
///
/// # Arguments
/// * `aggregate` - The aggregate instance
/// * `store` - The event store instance
/// * `command` - The command to execute
/// * `stream_id` - The ID of the event stream
/// * `current_state` - The current state of the aggregate
/// * `expected_version` - The expected version for optimistic concurrency control
///
/// # Returns
/// A `Result` containing the newly written events if successful
///
/// # Errors
/// Returns an error if any step in the process fails
pub async fn make_handler<State, Command, Event, Meta>(
    aggregate: &impl Aggregate<State, Command, Event>,
    store: &impl EventStore<Event, Meta>,
    command: &Command,
    stream_id: &str,
    current_state: &State,
    expected_version: &ExpectedVersion,
) -> Result<Vec<EventRead<Event, Meta>>>
where
    Event: Into<EventWrite<Event, Meta>> + Clone + Serialize + for<'de> Deserialize<'de>,
    Meta: Clone + Serialize + for<'de> Deserialize<'de>,
{
    let new_events = aggregate
        .execute(current_state, command)?
        .iter()
        .map(|x| x.clone().into())
        .collect();
    store
        .append_events(stream_id, expected_version, new_events)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::event_name::EventName;
    use crate::types::event_stream::EventStream;
    use crate::types::event_write::EventWrite;
    use crate::types::stream_read_filter::StreamsReadFilter;
    use async_trait::async_trait;
    use chrono::Utc;
    use uuid::Uuid;

    #[derive(Clone, Debug, PartialEq)]
    struct AccountState {
        balance: i64,
    }

    #[derive(Clone, Debug)]
    enum AccountCommand {
        Deposit(i64),
    }

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    enum AccountEvent {
        Deposited(i64),
    }

    impl crate::traits::event_name::EventName for AccountEvent {
        const NAME: &'static str = "Deposited";
    }

    impl From<AccountEvent> for EventWrite<AccountEvent, ()> {
        fn from(e: AccountEvent) -> Self {
            EventWrite {
                id: Uuid::new_v4(),
                correlation_id: None,
                causation_id: None,
                name: AccountEvent::NAME.to_string(),
                data: e,
                metadata: None,
            }
        }
    }

    #[derive(Default)]
    struct AccountAggregate;

    impl Aggregate<AccountState, AccountCommand, AccountEvent> for AccountAggregate {
        fn init(&self) -> AccountState {
            AccountState { balance: 0 }
        }

        fn apply(&self, state: AccountState, event: &AccountEvent) -> AccountState {
            match event {
                AccountEvent::Deposited(amount) => AccountState {
                    balance: state.balance + amount,
                },
            }
        }

        fn execute(
            &self,
            state: &AccountState,
            command: &AccountCommand,
        ) -> Result<Vec<AccountEvent>> {
            match command {
                AccountCommand::Deposit(amount) => {
                    Ok(vec![AccountEvent::Deposited(state.balance.min(0) + amount)])
                }
            }
        }
    }

    #[derive(Default)]
    struct InMemoryStore {
        /// stream_id -> events (ordered by version)
        streams:
            std::sync::Mutex<std::collections::HashMap<String, Vec<EventRead<AccountEvent, ()>>>>,
    }

    #[async_trait]
    impl EventStore<AccountEvent, ()> for InMemoryStore {
        async fn ensure_events_table(&self) -> Result<()> {
            Ok(())
        }

        async fn append_event(
            &self,
            stream_id: &str,
            version: &ExpectedVersion,
            payload: &EventWrite<AccountEvent, ()>,
        ) -> Result<EventRead<AccountEvent, ()>> {
            let mut guard = self.streams.lock().unwrap();
            let stream_events = guard.entry(stream_id.to_string()).or_default();
            let next_version = stream_events.len() as u32;

            match version {
                ExpectedVersion::NoStream if !stream_events.is_empty() => {
                    anyhow::bail!("Stream already exists");
                }
                ExpectedVersion::Exact(v) if *v != next_version => {
                    anyhow::bail!("ExpectedVersion mismatch: expected {v}, got {next_version}");
                }
                _ => {}
            }

            let event = EventRead {
                id: payload.id,
                correlation_id: payload.correlation_id,
                causation_id: payload.causation_id,
                stream_id: stream_id.to_string(),
                version: next_version,
                name: payload.name.clone(),
                data: payload.data.clone(),
                metadata: payload.metadata,
                created_utc: Utc::now(),
            };
            stream_events.push(event.clone());
            Ok(event)
        }

        async fn append_events(
            &self,
            stream_id: &str,
            version: &ExpectedVersion,
            payload: Vec<EventWrite<AccountEvent, ()>>,
        ) -> Result<Vec<EventRead<AccountEvent, ()>>> {
            let mut out = Vec::new();
            let mut v = version.clone();
            for p in payload {
                let written = self.append_event(stream_id, &v, &p).await?;
                v = ExpectedVersion::Exact(written.version + 1);
                out.push(written);
            }
            Ok(out)
        }

        async fn get_event(
            &self,
            stream_id: &str,
            version: u32,
        ) -> Result<EventRead<AccountEvent, ()>> {
            let guard = self.streams.lock().unwrap();
            let stream_events = guard
                .get(stream_id)
                .ok_or_else(|| anyhow::anyhow!("Stream not found"))?;
            stream_events
                .iter()
                .find(|e| e.version == version)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("Event not found"))
        }

        async fn get_events(
            &self,
            stream_id: &str,
            range: &crate::types::event_read_range::EventsReadRange,
        ) -> Result<Vec<EventRead<AccountEvent, ()>>> {
            let guard = self.streams.lock().unwrap();
            let stream_events = guard.get(stream_id).cloned().unwrap_or_default();
            let filtered = match range {
                crate::types::event_read_range::EventsReadRange::AllEvents => stream_events,
                crate::types::event_read_range::EventsReadRange::FromVersion(from) => stream_events
                    .into_iter()
                    .filter(|e| e.version >= *from)
                    .collect(),
                crate::types::event_read_range::EventsReadRange::ToVersion(to) => stream_events
                    .into_iter()
                    .filter(|e| e.version <= *to)
                    .collect(),
                crate::types::event_read_range::EventsReadRange::VersionRange {
                    from_version,
                    to_version,
                } => stream_events
                    .into_iter()
                    .filter(|e| e.version >= *from_version && e.version <= *to_version)
                    .collect(),
            };
            Ok(filtered)
        }

        async fn get_events_by_correlation_id(
            &self,
            correlation_id: &Uuid,
        ) -> Result<Vec<EventRead<AccountEvent, ()>>> {
            let guard = self.streams.lock().unwrap();
            Ok(guard
                .values()
                .flat_map(|v| v.iter().cloned())
                .filter(|e| e.correlation_id.as_ref() == Some(correlation_id))
                .collect())
        }

        async fn get_events_by_causation_id(
            &self,
            causation_id: &Uuid,
        ) -> Result<Vec<EventRead<AccountEvent, ()>>> {
            let guard = self.streams.lock().unwrap();
            Ok(guard
                .values()
                .flat_map(|v| v.iter().cloned())
                .filter(|e| e.causation_id.as_ref() == Some(causation_id))
                .collect())
        }

        async fn get_streams(&self, filter: &StreamsReadFilter) -> Result<Vec<EventStream>> {
            let guard = self.streams.lock().unwrap();
            let mut streams: Vec<EventStream> = guard
                .iter()
                .filter_map(|(id, evs)| {
                    evs.last().map(|last| EventStream {
                        id: id.clone(),
                        last_version: last.version,
                        last_updated_utc: last.created_utc,
                    })
                })
                .collect();
            streams.sort_by(|a, b| a.id.cmp(&b.id));
            match filter {
                StreamsReadFilter::AllStreams => Ok(streams),
                _ => Ok(streams), // Simplified: other filters not implemented for test helper
            }
        }

        async fn get_stream(&self, stream_id: &str) -> Result<EventStream> {
            let guard = self.streams.lock().unwrap();
            let evs = guard
                .get(stream_id)
                .ok_or_else(|| anyhow::anyhow!("Stream not found"))?;
            let last = evs
                .last()
                .ok_or_else(|| anyhow::anyhow!("Stream is empty"))?;
            Ok(EventStream {
                id: stream_id.to_string(),
                last_version: last.version,
                last_updated_utc: last.created_utc,
            })
        }
    }

    #[test]
    fn test_rehydrate_and_next_expected_version() {
        let aggregate = AccountAggregate;
        let events: Vec<EventRead<AccountEvent, ()>> = vec![
            EventRead {
                id: Uuid::new_v4(),
                correlation_id: None,
                causation_id: None,
                stream_id: "s1".to_string(),
                version: 0,
                name: "Deposited".to_string(),
                data: AccountEvent::Deposited(10),
                metadata: None::<()>,
                created_utc: Utc::now(),
            },
            EventRead {
                id: Uuid::new_v4(),
                correlation_id: None,
                causation_id: None,
                stream_id: "s1".to_string(),
                version: 1,
                name: "Deposited".to_string(),
                data: AccountEvent::Deposited(5),
                metadata: None::<()>,
                created_utc: Utc::now(),
            },
        ];

        let state = rehydrate(&aggregate, &events);
        assert_eq!(state.balance, 15);

        let expected = next_expected_version(&events).unwrap();
        match expected {
            ExpectedVersion::Exact(v) => assert_eq!(v, 2),
            _ => panic!("expected exact version"),
        }
    }

    #[tokio::test]
    async fn test_load_state_and_expected_version() {
        let aggregate = AccountAggregate;
        let store = InMemoryStore::default();
        let _ = aggregate
            .execute(&AccountState { balance: 0 }, &AccountCommand::Deposit(1))
            .unwrap();

        let e1 = EventWrite {
            id: Uuid::new_v4(),
            correlation_id: None,
            causation_id: None,
            name: "Deposited".to_string(),
            data: AccountEvent::Deposited(7),
            metadata: None::<()>,
        };
        let e2 = EventWrite {
            id: Uuid::new_v4(),
            correlation_id: None,
            causation_id: None,
            name: "Deposited".to_string(),
            data: AccountEvent::Deposited(3),
            metadata: None::<()>,
        };
        store
            .append_events("s1", &ExpectedVersion::NoStream, vec![e1, e2])
            .await
            .unwrap();

        let (state, expected) = load_state_and_expected_version(&aggregate, &store, "s1")
            .await
            .unwrap();
        assert_eq!(state.balance, 10);
        match expected {
            ExpectedVersion::Exact(v) => assert_eq!(v, 2),
            _ => panic!("expected exact version"),
        }
    }

    #[tokio::test]
    async fn test_make_handler_successful_append() {
        let aggregate = AccountAggregate;
        let store = InMemoryStore::default();
        let stream_id = "account-1";
        let state = AccountState { balance: 0 };
        let command = AccountCommand::Deposit(10);
        let expected_version = ExpectedVersion::NoStream;

        let result = make_handler(
            &aggregate,
            &store,
            &command,
            stream_id,
            &state,
            &expected_version,
        )
        .await
        .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].data, AccountEvent::Deposited(10));
        assert_eq!(result[0].version, 0);

        let (reloaded_state, _) = load_state_and_expected_version(&aggregate, &store, stream_id)
            .await
            .unwrap();
        assert_eq!(reloaded_state.balance, 10);
    }

    #[tokio::test]
    async fn test_make_handler_empty_events_from_execute() {
        #[derive(Clone, Debug, Serialize, Deserialize)]
        enum NoOpEvent {
            NoOp,
        }
        impl crate::traits::event_name::EventName for NoOpEvent {
            const NAME: &'static str = "NoOp";
        }
        impl From<NoOpEvent> for EventWrite<NoOpEvent, ()> {
            fn from(e: NoOpEvent) -> Self {
                EventWrite {
                    id: Uuid::new_v4(),
                    correlation_id: None,
                    causation_id: None,
                    name: NoOpEvent::NAME.to_string(),
                    data: e,
                    metadata: None,
                }
            }
        }

        #[derive(Default)]
        struct NoOpAggregate;
        impl Aggregate<(), (), NoOpEvent> for NoOpAggregate {
            fn init(&self) {}
            fn apply(&self, _: (), _: &NoOpEvent) {}
            fn execute(&self, _: &(), _: &()) -> Result<Vec<NoOpEvent>> {
                Ok(vec![]) // no events emitted
            }
        }

        #[derive(Default)]
        struct NoOpStore {
            events: std::sync::Mutex<Vec<EventRead<NoOpEvent, ()>>>,
        }
        #[async_trait]
        impl EventStore<NoOpEvent, ()> for NoOpStore {
            async fn ensure_events_table(&self) -> Result<()> {
                Ok(())
            }
            async fn append_event(
                &self,
                stream_id: &str,
                _v: &ExpectedVersion,
                p: &EventWrite<NoOpEvent, ()>,
            ) -> Result<EventRead<NoOpEvent, ()>> {
                let mut g = self.events.lock().unwrap();
                let ev = EventRead {
                    id: p.id,
                    correlation_id: p.correlation_id,
                    causation_id: p.causation_id,
                    stream_id: stream_id.to_string(),
                    version: g.len() as u32,
                    name: p.name.clone(),
                    data: p.data.clone(),
                    metadata: p.metadata,
                    created_utc: Utc::now(),
                };
                g.push(ev.clone());
                Ok(ev)
            }
            async fn append_events(
                &self,
                stream_id: &str,
                v: &ExpectedVersion,
                payload: Vec<EventWrite<NoOpEvent, ()>>,
            ) -> Result<Vec<EventRead<NoOpEvent, ()>>> {
                let mut out = Vec::new();
                for p in payload {
                    out.push(self.append_event(stream_id, v, &p).await?);
                }
                Ok(out)
            }
            async fn get_event(&self, _: &str, version: u32) -> Result<EventRead<NoOpEvent, ()>> {
                self.events
                    .lock()
                    .unwrap()
                    .get(version as usize)
                    .cloned()
                    .ok_or_else(|| anyhow::anyhow!("not found"))
            }
            async fn get_events(
                &self,
                _: &str,
                _: &crate::types::event_read_range::EventsReadRange,
            ) -> Result<Vec<EventRead<NoOpEvent, ()>>> {
                Ok(self.events.lock().unwrap().clone())
            }
            async fn get_events_by_correlation_id(
                &self,
                _: &Uuid,
            ) -> Result<Vec<EventRead<NoOpEvent, ()>>> {
                Ok(vec![])
            }
            async fn get_events_by_causation_id(
                &self,
                _: &Uuid,
            ) -> Result<Vec<EventRead<NoOpEvent, ()>>> {
                Ok(vec![])
            }
            async fn get_streams(&self, _: &StreamsReadFilter) -> Result<Vec<EventStream>> {
                Ok(vec![])
            }
            async fn get_stream(&self, _: &str) -> Result<EventStream> {
                Err(anyhow::anyhow!("not used"))
            }
        }

        let store = NoOpStore::default();
        let result = make_handler(
            &NoOpAggregate,
            &store,
            &(),
            "empty",
            &(),
            &ExpectedVersion::NoStream,
        )
        .await
        .unwrap();

        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_make_handler_execute_error_propagates() {
        #[derive(Clone, Debug)]
        enum FailCommand {
            Fail,
        }
        #[derive(Clone, Debug, Serialize, Deserialize)]
        enum FailEvent {
            Fail,
        }
        impl EventName for FailEvent {
            const NAME: &'static str = "Fail";
        }
        impl From<FailEvent> for EventWrite<FailEvent, ()> {
            fn from(e: FailEvent) -> Self {
                EventWrite {
                    id: Uuid::new_v4(),
                    correlation_id: None,
                    causation_id: None,
                    name: FailEvent::NAME.to_string(),
                    data: e,
                    metadata: None,
                }
            }
        }

        #[derive(Default)]
        struct FailAggregate;
        impl Aggregate<(), FailCommand, FailEvent> for FailAggregate {
            fn init(&self) {}
            fn apply(&self, _: (), _: &FailEvent) {}
            fn execute(&self, _: &(), _: &FailCommand) -> Result<Vec<FailEvent>> {
                Err(anyhow::anyhow!("intentional failure"))
            }
        }

        #[derive(Default)]
        struct FailStore {
            #[allow(dead_code)]
            events: std::sync::Mutex<Vec<EventRead<FailEvent, ()>>>,
        }
        #[async_trait]
        impl EventStore<FailEvent, ()> for FailStore {
            async fn ensure_events_table(&self) -> Result<()> {
                Ok(())
            }
            async fn append_event(
                &self,
                _: &str,
                _: &ExpectedVersion,
                _: &EventWrite<FailEvent, ()>,
            ) -> Result<EventRead<FailEvent, ()>> {
                unreachable!()
            }
            async fn append_events(
                &self,
                _: &str,
                _: &ExpectedVersion,
                _: Vec<EventWrite<FailEvent, ()>>,
            ) -> Result<Vec<EventRead<FailEvent, ()>>> {
                unreachable!()
            }
            async fn get_event(&self, _: &str, _: u32) -> Result<EventRead<FailEvent, ()>> {
                Err(anyhow::anyhow!("not used"))
            }
            async fn get_events(
                &self,
                _: &str,
                _: &crate::types::event_read_range::EventsReadRange,
            ) -> Result<Vec<EventRead<FailEvent, ()>>> {
                Ok(vec![])
            }
            async fn get_events_by_correlation_id(
                &self,
                _: &Uuid,
            ) -> Result<Vec<EventRead<FailEvent, ()>>> {
                Ok(vec![])
            }
            async fn get_events_by_causation_id(
                &self,
                _: &Uuid,
            ) -> Result<Vec<EventRead<FailEvent, ()>>> {
                Ok(vec![])
            }
            async fn get_streams(&self, _: &StreamsReadFilter) -> Result<Vec<EventStream>> {
                Ok(vec![])
            }
            async fn get_stream(&self, _: &str) -> Result<EventStream> {
                Err(anyhow::anyhow!("not used"))
            }
        }

        let store = FailStore::default();
        let err = make_handler(
            &FailAggregate,
            &store,
            &FailCommand::Fail,
            "fail",
            &(),
            &ExpectedVersion::NoStream,
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("intentional failure"));
    }

    #[test]
    fn prop_make_handler_rehydrate_matches_append_flow() {
        use proptest::prelude::*;

        proptest!(|(deposits in prop::collection::vec(-100i64..100, 1..15))| {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let aggregate = AccountAggregate;
                let store = InMemoryStore::default();
                let stream_id = "prop-account";
                let mut state = AccountState { balance: 0 };
                let mut expected_version = ExpectedVersion::NoStream;

                for amount in &deposits {
                    let command = AccountCommand::Deposit(*amount);
                    let written = make_handler(
                        &aggregate,
                        &store,
                        &command,
                        stream_id,
                        &state,
                        &expected_version,
                    )
                    .await
                    .unwrap();
                    state = aggregate.apply(state, &written[0].data);
                    expected_version = ExpectedVersion::Exact(written[0].version + 1);
                }

                let (rehydrated, _) =
                    load_state_and_expected_version(&aggregate, &store, stream_id)
                        .await
                        .unwrap();
                assert_eq!(state.balance, rehydrated.balance);
            });
        });
    }
}
