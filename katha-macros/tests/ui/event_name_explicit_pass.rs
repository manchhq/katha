use katha::traits::event_name::EventName;
use katha_macros::EventName as EventNameDerive;

#[derive(EventNameDerive)]
#[event_name = "Patient.Renamed"]
struct PatientRenamed;

fn main() {
    assert_eq!(PatientRenamed::NAME, "Patient.Renamed");
}
