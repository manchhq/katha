use katha::traits::event_name::EventName;
use katha_macros::EventName as EventNameDerive;

#[derive(EventNameDerive)]
struct PatientCreated;

fn main() {
    let _ = PatientCreated::NAME;
}
