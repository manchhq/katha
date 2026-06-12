use katha::types::event_write::EventWrite;
use katha_macros::EventName;
use uuid::Uuid;

#[derive(Clone, Debug, EventName)]
#[event_name = "Patient.Created"]
struct PatientCreated {
    id: String,
}

#[test]
fn test_event_write_from_payload_uses_derived_event_name() {
    let event = EventWrite::from_payload(
        Uuid::new_v4(),
        Some(Uuid::new_v4()),
        None,
        PatientCreated {
            id: "p-1".to_string(),
        },
        Some("meta".to_string()),
    );

    assert_eq!(event.name, "Patient.Created");
    assert_eq!(event.data.id, "p-1");
    assert_eq!(event.metadata.as_deref(), Some("meta"));
}
